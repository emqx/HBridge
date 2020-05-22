{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module Network.MQTT.Bridge.Environment
  ( getSocket
  , getMQTTClient
  , callbackFunc
  , newEnv1
  , newEnv2
  ) where

import           Control.Concurrent.STM
import           Control.Exception
import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Either               (isLeft, isRight)
import qualified Data.List                 as L
import           Data.Map                  as Map
import           Data.Maybe                (fromJust, isJust, isNothing)
import           Data.Time
import           Network.MQTT.Bridge.Extra
import           Network.MQTT.Bridge.Types
import           Network.MQTT.Client
import           Network.MQTT.Types
import           Network.Simple.TCP
import           Network.URI
import           System.Metrics.Counter
import           Text.Printf


-- | Create a TCP connection and return the handle with broker name.
-- It can catch exceptions when establishing a connection.
getSocket :: ExceptT String (ReaderT Broker IO) (BrokerName, Socket)
getSocket = do
  b@(Broker n t uris _ _ _) <- ask

  let uri   = fromJust (parseURI uris)
      host' = uriRegName       <$> uriAuthority uri
      port' = L.tail . uriPort <$> uriAuthority uri

  when (isNothing host' || isNothing port') $
    throwError $ printf "Invalid URI : %s ." (show uri)

  (s' :: Either SomeException Socket) <- liftIO . try $ do
    (sock,addr) <- connectSock (fromJust host') (fromJust port')
    return sock
  case s' of
    Left e  -> throwError $
               printf "Failed to connect to %s (%s) : %s."
                      n (show uri) (show e)
    Right s -> return (n, s)

-- | LowLevelCallback type with extra broker name information. For internal useage.
type LowLevelCallbackType = BrokerName -> MQTTClient -> PublishRequest -> IO ()

-- | Create a MQTT connection and return the client with broker name.
-- It can catch exceptions when making a connection.
getMQTTClient :: LowLevelCallbackType
              -> ExceptT String (ReaderT Broker IO) (BrokerName, MQTTClient)
getMQTTClient callback = do
  b@(Broker n t uris _ _ _) <- ask
  let uri = fromJust (parseURI uris)

  (mc' :: Either SomeException MQTTClient) <- liftIO . try $ do
    connectURI mqttConfig{_msgCB = LowLevelCallback (callback n)} uri
  case mc' of
    Left e   -> throwError $
                printf "Failed to connect to %s (%s) : %s." n (show uri) (show e)
    Right mc -> return (n, mc)

-- | Create basic bridge environment without connections created.
newEnv1 :: ReaderT Config IO Env
newEnv1 = do
  time                      <- liftIO getCurrentTime
  [mqrc,mqfc,tctlc,trc,tfc] <- liftIO $ replicateM 5 new
  conf@(Config bs _ _ _ _ sqls) <- ask
  logger                    <- liftIO $ mkLogger conf
  ch                        <- liftIO newBroadcastTChanIO
  activeMCs                 <- liftIO $ newTVarIO Map.empty
  activeTCPs                <- liftIO $ newTVarIO Map.empty
  fs'                       <- liftIO $ mapM parseSQLFile sqls
  funcs                     <- liftIO $ newTVarIO [(sql,fromJust f') | (sql,f') <- sqls `zip` fs', isJust f']

  let rules = Map.fromList [(brokerName b, (brokerFwds b, brokerSubs b)) | b <- bs]
      mps   = Map.fromList [(brokerName b, brokerMount b) | b <- bs]
      cnts  = MsgCounter mqrc mqfc tctlc trc tfc

  return $ Env
    { envBridge = Bridge time activeMCs activeTCPs rules mps ch funcs cnts
    , envConfig = conf
    , envLogger = logger
    }



callbackFunc :: Env -> LowLevelCallbackType
callbackFunc Env{..} n mc pubReq = do
  logging envLogger INFO $ printf "Received    [%s]." (show $ PubPkt pubReq n)
  inc (mqttRecvCounter (counters envBridge))

  -- message transformation
  funcs' <- readTVarIO (functions envBridge)
  let ch = broadcastChan envBridge
      mps = mountPoints envBridge
      funcs = snd <$> funcs'
      mp = fromJust (Map.lookup n mps)
  ((msg', s), l) <- runFuncSeries (PubPkt pubReq n) funcs
  case msg' of
    Left e      -> logging envLogger WARNING $ printf "Message transformation failed: [%s]."
                                                      (show $ PubPkt pubReq n)
    Right msg'' -> do
      logging envLogger INFO $ printf "Message transformation succeeded: [%s] to [%s]."
                                      (show $ PubPkt pubReq n)
                                      (show msg'')
      -- mountpoint
      case msg'' of
        PubPkt pubReq'' n'' -> do
          let msgMP = PubPkt pubReq''{_pubTopic = textToBL $
                             mp `composeMP` (blToText (_pubTopic pubReq''))} n''
          atomically $ writeTChan ch msgMP
          logging envLogger INFO $ printf "Mountpoint added: [%s]." (show msgMP)
        _ -> atomically $ writeTChan ch msg''


-- | Update the bridge environment from the old one with
-- connections created.
newEnv2 :: StateT Env IO ()
newEnv2 = do
  env@(Env bridge conf logger) <- get

  -- connect to MQTT
  tups1' <- liftIO $ mapM (runReaderT (runExceptT (getMQTTClient (callbackFunc env))))
                          (L.filter (\b -> connectType b == MQTTConnection) $ brokers conf)
  liftIO $ mapM_ (\(Left e) -> logging logger WARNING e) (L.filter isLeft tups1')
  let tups1 = [t | (Right t) <- (L.filter isRight tups1')]

  -- update active brokers
  liftIO . atomically $ writeTVar (activeMQTT bridge) (Map.fromList tups1)

  -- subscribe
  liftIO $ mapM_ (\(n, mc) -> do
            let (fwds, subs) = fromJust (Map.lookup n (rules bridge))
            subscribe mc (fwds `L.zip` L.repeat subOptions) []) tups1

  -- connect to TCP
  tups2' <- liftIO $ mapM (runReaderT (runExceptT getSocket))
                         (L.filter (\b -> connectType b == TCPConnection) $ brokers conf)
  liftIO $ mapM_ (\(Left e) -> logging logger WARNING e) (L.filter isLeft tups2')
  let tups2 = [t | (Right t) <- (L.filter isRight tups2')]
  liftIO . atomically $ writeTVar (activeTCP bridge) (Map.fromList tups2)

  -- commit changes
  put $ Env bridge conf logger
