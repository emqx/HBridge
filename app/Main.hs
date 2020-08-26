{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}

module Main where

import qualified Colog
import           Control.Concurrent              (forkFinally, threadDelay)
import           Control.Concurrent.Async        (async, race)
import           Control.Concurrent.STM          (TChan, atomically, dupTChan,
                                                  modifyTVar, readTChan,
                                                  readTVarIO, writeTChan)
import           Control.Exception               (SomeException)
import           Control.Monad                   (forever, when)
import           Control.Monad.Except            (runExceptT)
import           Control.Monad.IO.Class          (liftIO)
import           Control.Monad.Reader            (ask, runReaderT)
import           Data.Either                     (isLeft, isRight)
import qualified Data.List                       as L
import qualified Data.Map                        as Map
import           Data.Maybe                      (fromJust)
import qualified Data.Text                       as Text
import           Data.UUID                       (UUID)
import           Network.MQTT.Bridge.Environment (callbackFunc, getClientID,
                                                  getMQTTClient, getSocket,
                                                  newEnv1, newEnv2)
import           Network.MQTT.Bridge.Extra       (blToText, composeMP,
                                                  existMatch, fwdTCPMessage,
                                                  recvTCPMessages)
import           Network.MQTT.Bridge.RestAPI     (apiApp)
import           Network.MQTT.Bridge.Types       (App, Bridge (..), Broker (..),
                                                  BrokerName, Config (..),
                                                  ConnectionType (..), Env (..),
                                                  Message (..), MsgCounter (..),
                                                  parseConfig, runApp)
import           Network.MQTT.Client             (MQTTClient, pubAliased,
                                                  subscribe)
import           Network.MQTT.Types              (PublishRequest (..),
                                                  subOptions)
import qualified Network.Simple.TCP              as TCP
import qualified Network.Wai.Handler.Warp        as Warp
import           Prelude                         hiding (read)
import           System.Metrics.Counter          (inc)
import           Text.Printf                     (printf)


main :: IO ()
main = do
  conf' <- runExceptT parseConfig
  case conf' of
    Left  e    -> error e
    Right conf -> do
      env' <- runReaderT newEnv1 conf
      env  <- runApp env' newEnv2
      _    <- async (Warp.run 8999 (apiApp env))
      runApp env mainApp

mainApp :: App ()
mainApp = do
  env@(Env bridge _) <- ask
  initMCs                         <- liftIO $ readTVarIO (activeMQTT bridge)
  Colog.logInfo . Text.pack $
    "Connected to brokers:\n" <> L.intercalate "\n" (Map.keys initMCs)
  initTCPs <- liftIO $ readTVarIO (activeTCP bridge)
  Colog.logInfo . Text.pack $
    "Active TCP connections: \n" <> L.intercalate "\n" (Map.keys initTCPs)
  liftIO $ mapM_ (runApp env . processMQTT) (Map.toList initMCs)
  liftIO $ mapM_ (runApp env . processTCP)  (Map.toList initTCPs)
  _ <- liftIO $ forkFinally
       (runApp env . forever $ maintainConns True True)
       (\_ -> runApp env $ Colog.logWarning "Connection maintaining thread failed.")
  _ <- liftIO $ forever getChar
  return ()

-- | Maintain connections described in configuration file.
-- When the bridge lose a connectiuon, it will refer to the configuration file
-- and try re-connecting to it. The retry interval is 2000ms by default.
-- The first two arguments controls if to show warning information and if to run
-- processing thread.
maintainConns :: Bool -> Bool -> App ()
maintainConns warnFlag runProcess = do
  env@(Env Bridge {..} conf) <- ask
  activeMQs                  <- liftIO $ readTVarIO activeMQTT
  activeTCPs                 <- liftIO $ readTVarIO activeTCP
  let
    activeMQNs  = Map.keys activeMQs
    allMQs      = L.filter (\b -> connectType b == MQTTConnection) (brokers conf)
    missedMQs   = L.filter (\b -> brokerName b `L.notElem` activeMQNs) allMQs
    missedMQNs  = brokerName <$> missedMQs
    activeTCPNs = Map.keys activeTCPs
    allTCPs     = L.filter (\b -> connectType b == TCPConnection) (brokers conf)
    missedTCPs  = L.filter (\b -> brokerName b `L.notElem` activeTCPNs) allTCPs
    missedTCPNs = brokerName <$> missedTCPs
  -- MQTT
  when (not (L.null missedMQNs) && warnFlag) $
    Colog.logWarning . Text.pack $
      printf "Missed MQTT connections: %s . Retrying..." (show missedMQNs)
  tups1' <- liftIO $ mapM
    (runReaderT
      (runExceptT
        (getMQTTClient
          (\n mc pubReq -> runApp env (callbackFunc env n mc pubReq))
        )
      )
    ) missedMQs
  mapM_ (\(Left e) -> Colog.logWarning $ Text.pack e) (L.filter isLeft tups1')
  let tups1 = [ t | (Right t) <- L.filter isRight tups1' ]
  liftIO . atomically $ modifyTVar activeMQTT (Map.union (Map.fromList tups1))
  liftIO $ mapM_
    (\(n, mc) -> do
      let (fwds,_) = fromJust (Map.lookup n rules)
      subscribe mc (fwds `L.zip` L.repeat subOptions) []
    ) tups1
  liftIO $ when runProcess $ mapM_ (runApp env . processMQTT) tups1
  -- TCP
  when (not (L.null missedTCPNs) && warnFlag) $
    Colog.logWarning . Text.pack $
      printf "Lost TCP connections: %s . Retrying..." (show missedTCPNs)
  socksWithName' <- liftIO $ mapM (runReaderT (runExceptT getSocket)) missedTCPs
  liftIO $ mapM_ (\(Left e) -> runApp env $ Colog.logWarning $ Text.pack e)
                 (L.filter isLeft socksWithName')
  let socksWithName = [ t | (Right t) <- L.filter isRight socksWithName' ]
  tups2' <- liftIO $ mapM getClientID socksWithName
  let tups2 = [ t | (Right t) <- L.filter isRight tups2' ]
  liftIO . atomically $ modifyTVar activeTCP (Map.union (Map.fromList tups2))
  liftIO $ when runProcess $ mapM_ (runApp env . processTCP) tups2
  -- Sleep
  liftIO $ threadDelay 2000000

-- | Same as 'runMQTT', for symmetry only.
processMQTT :: (BrokerName, MQTTClient) -> App ()
processMQTT tup = do
    env@(Env Bridge {..} _) <- ask
    _ <- liftIO $ forkFinally (runApp env $ runMQTT tup) handleException
    return ()
  where
    handleException _ = return ()

-- | Describes how the bridge receives and forwards messages
-- from/to a certain broker.
-- It is bridge-scoped and does not care about specific behaviours
-- of brokers, which is provided by fwdMessage' and 'recvMessge'.
runMQTT :: (BrokerName, MQTTClient) -> App ()
runMQTT (n, mc) = do
    env@(Env Bridge {..} _) <- ask
    ch                      <- liftIO . atomically $ dupTChan broadcastChan
    _ <- liftIO $
      forkFinally (runApp env $ forwarding ch) (runApp env . handleException)
    return ()
  where
    forwarding :: TChan Message -> App ()
    forwarding ch = do
      (Env Bridge {..} _) <- ask
      let (_,subs) = fromJust (Map.lookup n rules)
      forever $ do
        msg <- liftIO $ atomically (readTChan ch)
        case msg of
          PubPkt p n' ->
            when (blToText (_pubTopic p) `existMatch` subs && n /= n') $ do
              Colog.logDebug . Text.pack $
                printf "Forwarded   [%s] from %s to %s." (show msg) n' n
              liftIO $ inc (mqttFwdCounter counters)
              liftIO $ pubAliased mc
                                  (blToText $ _pubTopic p)
                                  (_pubBody p)
                                  (_pubRetain p)
                                  (_pubQoS p)
                                  (_pubProps p)
          _           -> return ()
    handleException :: Either SomeException () -> App ()
    handleException e' = do
      (Env Bridge {..} _) <- ask
      case e' of
        Left e -> do
          liftIO . atomically $ modifyTVar activeMQTT (Map.delete n)
          Colog.logWarning . Text.pack $
            printf "MQTT broker %s disconnected : %s" n (show e)
        _      -> return ()

-- | Create a thread (in fact two, receiving and forwarding)
-- for certain TCP connection.
processTCP :: (BrokerName, (TCP.Socket, UUID)) -> App ()
processTCP tup@(n, (s, _)) = do
    env@(Env Bridge {..} _) <- ask
    _ <- liftIO $
      forkFinally (runApp env $ runTCP tup) (runApp env . handleException)
    return ()
  where
    handleException :: Either SomeException () -> App ()
    handleException e' = do
      (Env Bridge {..} _) <- ask
      case e' of
        Left e -> do
          liftIO . atomically $ modifyTVar activeTCP (Map.delete n)
          Colog.logWarning . Text.pack $
            printf "[TCP]  Broker %s disconnected (%s)" n (show e)
          TCP.closeSock s
          return ()
        _      -> return ()

-- | Describes how the bridge receives and forwards messages
-- from/to a certain TCP connection.
-- It is bridge-scoped and does not care about specific behaviours
-- of servers, which is provided by fwdTCPMessage' and 'recvTCPMessge'.
runTCP :: (BrokerName, (TCP.Socket, UUID)) -> App ()
runTCP (n, (s, cid)) = do
    env@(Env Bridge {..} Config {..}) <- ask
    ch <- liftIO . atomically $ dupTChan broadcastChan
    _  <- liftIO $ race (runApp env $ receiving ch) (runApp env $ forwarding ch)
    return ()
  where
    processRecvMsgs :: TChan Message -> Maybe Message -> App ()
    processRecvMsgs ch msg = do
      (Env Bridge {..} Config {..}) <- ask
      let (fwds,_) = fromJust (Map.lookup n rules)
          mp       = fromJust (Map.lookup n mountPoints)
      case msg of
        Just (PlainMsg p t) -> do
          Colog.logDebug . Text.pack $ printf "[TCP]  Received    [%s]." (show msg)
          liftIO $ inc (tcpMsgRecvCounter counters)
          let msgMP = PlainMsg p (mp `composeMP` t)
          liftIO $ when (t `existMatch` fwds) $ atomically $ do
            writeTChan broadcastChan msgMP
            _ <- readTChan ch
            return ()
          Colog.logDebug . Text.pack $
            printf "[TCP]  Mountpoint added: [%s]." (show msgMP)
        _                   -> return ()

    receiving :: TChan Message -> App ()
    receiving ch = forever $ do
      msgs <- liftIO $ recvTCPMessages s 1024
      mapM_ (processRecvMsgs ch) msgs

    forwarding :: TChan Message -> App ()
    forwarding ch = do
      env@(Env Bridge {..} Config {..}) <- ask
      liftIO . forever $ do
        let (_,subs) = fromJust (Map.lookup n rules)
        msg <- atomically (readTChan ch)
        case msg of
          PlainMsg p t                 -> when (t `existMatch` subs) $ do
            fwdTCPMessage (s, cid) t p
            runApp env $ Colog.logDebug . Text.pack $
              printf "[TCP]  Forwarded   [%s]." (show msg)
            inc (tcpMsgFwdCounter counters)
          PubPkt PublishRequest {..} _ ->
            when (crossForward && blToText _pubTopic `existMatch` subs) $ do
              fwdTCPMessage (s, cid) (blToText _pubTopic) _pubBody
              runApp env $ Colog.logDebug . Text.pack $
                printf "[TCP]  Forwarded   [%s]." (show msg)
              inc (tcpMsgFwdCounter counters)
