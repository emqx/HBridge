{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import           Types
import           Extra
import           Environment

import qualified Data.List                 as L
import qualified Data.Map                  as Map
import           Data.Maybe                (fromJust)
import qualified Data.ByteString           as BS
import qualified Data.ByteString.Lazy      as BL
import qualified Data.ByteString.Char8     as BSC (hPutStrLn)
import           Text.Printf
import           System.IO
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Except
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Concurrent.Async
import           Data.Aeson
import           System.Remote.Monitoring

import           Network.MQTT.Types
import           Network.MQTT.Client


main :: IO ()
main = do
  conf' <- runExceptT parseConfig
  case conf' of
    Left e -> error e
    Right Nothing -> error "Failed to parse config file."
    Right (Just conf) -> do
      forkServer "localhost" 22333
      env' <- runReaderT newEnv1 conf
      env@(Env bridge _ logger _) <- execStateT newEnv2 env'
      forkFinally (logProcess logger) (\_ -> putStrLn "[Warning] Log service failed.")

      initMCs <- readTVarIO (activeMQTT bridge)
      logging logger INFO $ "Connected to brokers:\n" ++ L.intercalate "\n" (Map.keys initMCs)
      initTCPs <- readTVarIO (activeTCP bridge)
      logging logger INFO $ "Active TCP connections: \n" ++ L.intercalate "\n" (Map.keys initTCPs)

      mapM_ (\tup -> processMQTT tup env) (Map.toList initMCs)
      mapM_ (\tup -> processTCP tup env) (Map.toList initTCPs)

      forever getChar
      return ()


-- | Create a thread (in fact two, receiving and forwarding)
-- for certain broker.
processMQTT :: (BrokerName, MQTTClient)
        -> Env
        -> IO ()
processMQTT tup@(n, mc) env@(Env Bridge{..} _ logger _) = do
    forkFinally (runMQTT tup env) (\e -> handleException e)
    return ()
  where
    handleException e = case e of
        Left e -> do
          atomically $ modifyTVar activeMQTT (Map.delete n)
          logging logger WARNING $ printf "MQTT broker %s disconnected (%s)\n" n (show e)
        _     -> return ()

-- | Describes how the bridge receives and forwards messages
-- from/to a certain broker.
-- It is bridge-scoped and does not care about specific behaviours
-- of brokers, which is provided by fwdMessage' and 'recvMessge'.
runMQTT :: (BrokerName, MQTTClient)
    -> Env
    -> IO ()
runMQTT (n, mc) (Env Bridge{..} _ logger _) = do
    ch <- atomically $ dupTChan broadcastChan
    forkFinally (forwarding ch logger) (\e -> return ())
    return ()
  where
    (fwds, subs) = fromJust (Map.lookup n rules)
    mp = fromJust (Map.lookup n mountPoints)

    forwarding ch logger = forever $ do
      msg <- atomically (readTChan ch)
      case msg of
        PubPkt p n' -> when ((blToText (_pubTopic p)) `existMatch` subs && n /= n') $ do
          logging logger INFO $ printf "Forwarded   [%s] from %s to %s." (show msg) n' n
          pubAliased mc (blToText $ _pubTopic p) (_pubBody p) (_pubRetain p) (_pubQoS p) (_pubProps p)
        _            -> return ()



-- | Create a thread (in fact two, receiving and forwarding)
-- for certain TCP connection.
processTCP :: (BrokerName, Handle)
        -> Env
        -> IO ()
processTCP tup@(n, h) env@(Env Bridge{..} _ logger _) = do
    forkFinally (runTCP tup env) (\e -> handleException e)
    return ()
  where
    handleException e = case e of
        Left e -> do
          atomically $ modifyTVar activeTCP (Map.delete n)
          logging logger WARNING $ printf "[TCP]  Broker %s disconnected (%s)\n" n (show e)
          hClose h
        _     -> return ()

-- | Describes how the bridge receives and forwards messages
-- from/to a certain TCP connection.
-- It is bridge-scoped and does not care about specific behaviours
-- of servers, which is provided by fwdTCPMessage' and 'recvTCPMessge'.
runTCP :: (BrokerName, Handle)
    -> Env
    -> IO ()
runTCP (n, h) (Env Bridge{..} _ logger _) = do
    ch <- atomically $ dupTChan broadcastChan
    race (receiving ch logger) (forwarding ch logger)
    return ()
  where
    (fwds, subs) = fromJust (Map.lookup n rules)
    mp = fromJust (Map.lookup n mountPoints)
    receiving ch logger = forever $ do
      msg <- recvTCPMessage h
      logging logger INFO $ printf "[TCP]  Received    [%s]." (show msg)
      case msg of

        Just (PlainMsg _ t) -> do
          when (t `existMatch` fwds) $ do
            funcs' <- readTVarIO functions
            let funcs = snd <$> funcs'
            ((msg', s), l) <- runFuncSeries (fromJust msg) funcs
            case msg' of
              Left e -> logging logger WARNING "[TCP]  Message transformation failed."
              Right msg'' -> atomically $ do
                writeTChan broadcastChan msg''
                readTChan ch
                return ()

        Just ListFuncs -> do
          funcs <- readTVarIO functions
          let (items :: [String]) = L.map (\(i,s) -> show i ++ " " ++ s ++ "\n") ([0..] `L.zip` (fst <$> funcs))
          logging logger INFO $ "[TCP]  Functions:\n" ++ L.concat items

        Just (InsertSaveMsg n i f) -> do
          funcs <- readTVarIO functions
          atomically $ writeTVar functions (insertToN i (n, saveMsg f) funcs)
          logging logger INFO $ printf "[TCP]  Function %s : save message to file %s." n f

        Just (InsertModifyTopic n i t t') -> do
          funcs <- readTVarIO functions
          atomically $ writeTVar functions (insertToN i (n, \x -> modifyTopic x t t') funcs)
          logging logger INFO $ printf "[TCP]  Function %s : modify topic %s to %s." n t t'

        Just (InsertModifyField n i fs v) -> do
          funcs <- readTVarIO functions
          atomically $ writeTVar functions (insertToN i (n, \x -> modifyField x fs v) funcs)
          logging logger INFO $ printf "[TCP]  Function %s : modify field %s to %s." n (show fs) (show v)

        _                   -> return ()

    forwarding ch logger = forever $ do
      msg <- atomically (readTChan ch)
      --logging logger INFO $ printf "[TCP]  Processing  [%s]." (show msg)
      case msg of
        PlainMsg _ t -> when (t `existMatch` subs) $ do
          let msg' = msg{payload = mp `composeMP` t}
          fwdTCPMessage h msg'
          logging logger INFO $ printf "[TCP]  Forwarded   [%s]." (show msg')
        _            -> return ()


-- | Forward message to certain broker. Broker-dependent and will be
-- replaced soon.
fwdTCPMessage :: Handle -> Message -> IO ()
fwdTCPMessage h msg = do
  BSC.hPutStrLn h $ BL.toStrict (encode msg)

-- | Receive message from certain broker. Broker-dependent and will be
-- replaced soon.
recvTCPMessage :: Handle -> IO (Maybe Message)
recvTCPMessage h = do
  s <- BS.hGetLine h
  return $ decode (BL.fromStrict s)
