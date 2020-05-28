{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}

module Main where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Monad
import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Either                     (isLeft, isRight)
import qualified Data.List                       as L
import qualified Data.Map                        as Map
import           Data.Maybe                      (fromJust)
import           Network.MQTT.Bridge.Environment
import           Network.MQTT.Bridge.Extra
import           Network.MQTT.Bridge.RestAPI
import           Network.MQTT.Bridge.Types
import           Network.MQTT.Client
import           Network.MQTT.Types
import           Network.Simple.TCP
import           Network.Wai.Handler.Warp
import           Prelude                         hiding (read)
import           System.Metrics.Counter
import           System.Remote.Monitoring        hiding (Server)
import           Text.Printf


main :: IO ()
main = do
  conf' <- runExceptT parseConfig
  case conf' of
    Left e -> error e
    Right conf -> do
      forkServer "localhost" 22333
      env' <- runReaderT newEnv1 conf
      env@(Env bridge _ logger) <- execStateT newEnv2 env'
      forkFinally (logProcess logger) (\_ -> putStrLn "[Warning] Log service failed.")

      async (run 8999 (apiApp env))

      initMCs <- readTVarIO (activeMQTT bridge)
      logging logger INFO $ "Connected to brokers:\n" ++ L.intercalate "\n" (Map.keys initMCs)
      initTCPs <- readTVarIO (activeTCP bridge)
      logging logger INFO $ "Active TCP connections: \n" ++ L.intercalate "\n" (Map.keys initTCPs)

      mapM_ (`processMQTT` env) (Map.toList initMCs)
      mapM_ (`processTCP` env) (Map.toList initTCPs)

      forkFinally (forever $ maintainConns True True env)
        (\e -> logging logger WARNING "Connection maintaining thread failed.")

      forever getChar
      return ()

-- | Maintain connections described in configuration file.
-- When the bridge lose a connectiuon, it will refer to the configuration file
-- and try re-connecting to it. The retry interval is 2000ms by default.
-- The first two arguments controls if to show warning information and if to run
-- processing thread.
maintainConns :: Bool -> Bool -> Env -> IO ()
maintainConns warnFlag runProcess env@(Env Bridge{..} conf logger) = do
  activeMQs  <- readTVarIO activeMQTT
  activeTCPs <- readTVarIO activeTCP
  let activeMQNs = Map.keys activeMQs
      allMQs = L.filter (\b -> connectType b == MQTTConnection) (brokers conf)
      missedMQs = L.filter (\b -> brokerName b `L.notElem` activeMQNs) allMQs
      missedMQNs = brokerName <$> missedMQs

      activeTCPNs = Map.keys activeTCPs
      allTCPs = L.filter (\b -> connectType b == TCPConnection) (brokers conf)
      missedTCPs = L.filter (\b -> brokerName b `L.notElem` activeTCPNs) allTCPs
      missedTCPNs = brokerName <$> missedTCPs

  -- MQTT
  when (not (L.null missedMQNs) && warnFlag) $
    logging logger WARNING $ printf "Missed MQTT connections: %s . Retrying..." (show missedMQNs)

  tups1' <- mapM (runReaderT (runExceptT (getMQTTClient (callbackFunc env)))) missedMQs
  mapM_ (\(Left e) -> logging logger WARNING e) (L.filter isLeft tups1')
  let tups1 = [t | (Right t) <- L.filter isRight tups1']
  atomically $ modifyTVar activeMQTT (Map.union (Map.fromList tups1))
  mapM_ (\(n, mc) -> do
            let (fwds, subs) = fromJust (Map.lookup n rules)
            subscribe mc (fwds `L.zip` L.repeat subOptions) []) tups1
  when runProcess $ mapM_ (`processMQTT` env) tups1

  -- TCP
  when (not (L.null missedTCPNs) && warnFlag) $
    logging logger WARNING $
      printf "Lost TCP connections: %s . Retrying..." (show missedTCPNs)
  tups2' <- mapM (runReaderT (runExceptT getSocket)) missedTCPs
  liftIO $ mapM_ (\(Left e) -> logging logger WARNING e) (L.filter isLeft tups2')
  let tups2 = [t | (Right t) <- L.filter isRight tups2']
  atomically $ modifyTVar activeTCP (Map.union (Map.fromList tups2))
  when runProcess $ mapM_ (`processTCP` env) tups2

  threadDelay 2000000


-- | Same as 'runMQTT', for symmetry only.
processMQTT :: (BrokerName, MQTTClient)
        -> Env
        -> IO ()
processMQTT tup@(n, mc) env@(Env Bridge{..} conf logger) = do
    forkFinally (runMQTT tup env) handleException
    return ()
  where
    handleException e = return ()

-- | Describes how the bridge receives and forwards messages
-- from/to a certain broker.
-- It is bridge-scoped and does not care about specific behaviours
-- of brokers, which is provided by fwdMessage' and 'recvMessge'.
runMQTT :: (BrokerName, MQTTClient)
    -> Env
    -> IO ()
runMQTT (n, mc) env@(Env Bridge{..} conf logger) = do
    ch <- atomically $ dupTChan broadcastChan
    forkFinally (forwarding ch logger) handleException
    return ()
  where
    (fwds, subs) = fromJust (Map.lookup n rules)
    mp = fromJust (Map.lookup n mountPoints)

    forwarding ch logger = forever $ do
      msg <- atomically (readTChan ch)
      case msg of
        PubPkt p n' -> when (blToText (_pubTopic p) `existMatch` subs && n /= n') $ do
          logging logger INFO $ printf "Forwarded   [%s] from %s to %s." (show msg) n' n
          inc (mqttFwdCounter counters)
          pubAliased mc (blToText $ _pubTopic p) (_pubBody p) (_pubRetain p) (_pubQoS p) (_pubProps p)
        _            -> return ()

    handleException e = case e of
        Left e -> do
          atomically $ modifyTVar activeMQTT (Map.delete n)
          logging logger WARNING $ printf "MQTT broker %s disconnected : %s" n (show e)
        _     -> return ()


-- | Create a thread (in fact two, receiving and forwarding)
-- for certain TCP connection.
processTCP :: (BrokerName, Socket)
        -> Env
        -> IO ()
processTCP tup@(n, s) env@(Env Bridge{..} _ logger) = do
    forkFinally (runTCP tup env) handleException
    return ()
  where
    handleException e = case e of
        Left e -> do
          atomically $ modifyTVar activeTCP (Map.delete n)
          logging logger WARNING $ printf "[TCP]  Broker %s disconnected (%s)" n (show e)
          closeSock s
          return ()
        _     -> return ()

-- | Describes how the bridge receives and forwards messages
-- from/to a certain TCP connection.
-- It is bridge-scoped and does not care about specific behaviours
-- of servers, which is provided by fwdTCPMessage' and 'recvTCPMessge'.
runTCP :: (BrokerName, Socket)
    -> Env
    -> IO ()
runTCP (n, s) (Env Bridge{..} Config{..} logger) = do
    ch <- atomically $ dupTChan broadcastChan
    race (receiving ch) (forwarding ch)
    return ()
  where
    (fwds, subs) = fromJust (Map.lookup n rules)
    mp           = fromJust (Map.lookup n mountPoints)

    processRecvMsgs ch msg = case msg of
      Just (PlainMsg p t) -> do
        logging logger INFO $ printf "[TCP]  Received    [%s]." (show msg)
        inc (tcpMsgRecvCounter counters)
        let msgMP = PlainMsg p (mp `composeMP` t)
        when (t `existMatch` fwds) $ atomically $ do
          writeTChan broadcastChan msgMP
          readTChan ch
          return ()
        logging logger INFO $ printf "[TCP]  Mountpoint added: [%s]." (show msgMP)
      _                   -> return ()

    receiving ch = forever $ do
      msgs <- recvTCPMessages s 128
      mapM_ (processRecvMsgs ch) msgs

    forwarding ch = forever $ do
      msg <- atomically (readTChan ch)
      case msg of
        PlainMsg _ t -> when (t `existMatch` subs) $ do
          fwdTCPMessage s msg
          logging logger INFO $ printf "[TCP]  Forwarded   [%s]." (show msg)
          inc (tcpMsgFwdCounter counters)
        PubPkt PublishRequest{..} n -> when (crossForward && blToText _pubTopic `existMatch` subs) $ do
          fwdTCPMessage s msg
          logging logger INFO $ printf "[TCP]  Forwarded   [%s]." (show msg)
          inc (tcpMsgFwdCounter counters)
        _            -> return ()
