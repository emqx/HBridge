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
  , getClientID
  ) where

import qualified Colog
import           Control.Concurrent.STM    (atomically, newBroadcastTChanIO,
                                            newTVarIO, readTVarIO, writeTChan,
                                            writeTVar)
import           Control.Exception         (SomeException, try)
import           Control.Monad             (replicateM, when)
import           Control.Monad.Except      (ExceptT, runExceptT, throwError)
import           Control.Monad.IO.Class    (liftIO)
import           Control.Monad.Reader      (ReaderT, ask, runReaderT)
import           Data.Either               (isLeft, isRight)
import qualified Data.List                 as L
import           Data.Map                  as Map
import           Data.Maybe                (fromJust, isJust, isNothing)
import qualified Data.Text                 as Text
import           Data.Time                 (getCurrentTime)
import           Data.UUID                 (UUID)
import qualified Data.UUID                 as UUID
import qualified Data.Vector               as V
import qualified Network.HESP              as HESP
import           Network.MQTT.Bridge.Extra
import           Network.MQTT.Bridge.Types (App, Bridge (..), Broker (..),
                                            BrokerName, Config (..),
                                            ConnectionType (..), Env (..),
                                            Message (..), MsgCounter (..),
                                            runApp, runFuncSeries)
import           Network.MQTT.Client       (MQTTClient, MQTTConfig (..),
                                            MessageCallback (..), connectURI,
                                            mqttConfig, subscribe)
import           Network.MQTT.Types        (PublishRequest (..), subOptions)
import           Network.Simple.TCP        (Socket, connectSock)
import           Network.URI               (parseURI, uriAuthority, uriPort,
                                            uriRegName)
import           System.Metrics.Counter    (inc, new)
import           Text.Printf               (printf)


-- | Create a TCP connection and return the handle with broker name.
-- It can catch exceptions when establishing a connection.

-- FIXME: More elegant style
getSocket :: ExceptT String (ReaderT Broker IO) (BrokerName, Socket)
getSocket = do
  Broker{..} <- ask
  let uri   = fromJust . parseURI $ brokerURI
  let host' = uriRegName         <$> uriAuthority uri
      port' = L.tail . uriPort   <$> uriAuthority uri
  when (isNothing host' || isNothing port') $
    throwError $ printf "Invalid URI : %s ." (show uri)
  (s' :: Either SomeException Socket) <- liftIO . try $ do
    (sock, _) <- connectSock (fromJust host') (fromJust port')
    return sock
  case s' of
    Left e  -> throwError $
               printf "Failed to connect to %s (%s) : %s."
                      brokerName (show uri) (show e)
    Right s -> return (brokerName, s)

-- | LowLevelCallback type with extra broker name information. For internal useage.
type LowLevelCallbackType = BrokerName -> MQTTClient -> PublishRequest -> IO ()

-- | Create a MQTT connection and return the client with broker name.
-- It can catch exceptions when making a connection.

-- FIXME: More elegant style
getMQTTClient :: LowLevelCallbackType
              -> ExceptT String (ReaderT Broker IO) (BrokerName, MQTTClient)
getMQTTClient callback = do
  Broker{..} <- ask
  let uri = fromJust . parseURI $ brokerURI
  (mc' :: Either SomeException MQTTClient) <- liftIO . try $ do
    connectURI mqttConfig{_msgCB = LowLevelCallback (callback brokerName)} uri
  case mc' of
    Left e   -> throwError $
                printf "Failed to connect to %s (%s) : %s."
                brokerName (show uri) (show e)
    Right mc -> return (brokerName, mc)

-- | Create basic bridge environment without connections created.
newEnv1 :: ReaderT (Config m) IO (Env m)
newEnv1 = do
  time                      <- liftIO getCurrentTime
  [mqrc,mqfc,tctlc,trc,tfc] <- liftIO $ replicateM 5 new
  conf@(Config bs _ sqls _) <- ask
  ch                        <- liftIO newBroadcastTChanIO
  activeMCs                 <- liftIO $ newTVarIO Map.empty
  activeTCPs                <- liftIO $ newTVarIO Map.empty
  fs'                       <- liftIO $ mapM parseSQLFile sqls
  funcs                     <- liftIO $ newTVarIO
                               [(sql,fromJust f') | (sql,f') <- sqls `zip` fs', isJust f']
  let rules = Map.fromList [(brokerName b, (brokerFwds b, brokerSubs b)) | b <- bs]
      mps   = Map.fromList [(brokerName b, brokerMount b) | b <- bs]
      cnts  = MsgCounter mqrc mqfc tctlc trc tfc
  return $ Env
    { envBridge = Bridge time activeMCs activeTCPs rules mps ch funcs cnts
    , envConfig = conf
    }

callbackFunc :: Env m
             -> BrokerName
             -> MQTTClient
             -> PublishRequest
             -> App ()
callbackFunc Env{..} n _ pubReq = do
  Colog.logDebug . Text.pack $
    printf "Received    [%s]." (show $ PubPkt pubReq n)
  liftIO $ inc (mqttRecvCounter (counters envBridge))
  -- message transformation
  funcs' <- liftIO $ readTVarIO (functions envBridge)
  let ch    = broadcastChan envBridge
      mps   = mountPoints envBridge
      funcs = snd <$> funcs'
      mp    = fromJust (Map.lookup n mps)
  ((msg',_) ,_) <- liftIO $ runFuncSeries (PubPkt pubReq n) funcs
  case msg' of
    Left _      -> Colog.logWarning . Text.pack $
      printf "Message transformation failed: [%s]." (show $ PubPkt pubReq n)
    Right msg'' -> do
      Colog.logDebug . Text.pack $
        printf "Message transformation succeeded: [%s] to [%s]."
               (show $ PubPkt pubReq n)
               (show msg'')
      -- mountpoint
      case msg'' of
        PubPkt pubReq'' n'' -> do
          let msgMP = PubPkt pubReq''{_pubTopic = textToBL $
                             mp `composeMP` blToText (_pubTopic pubReq'')} n''
          liftIO . atomically $ writeTChan ch msgMP
          Colog.logDebug . Text.pack $ printf "Mountpoint added: [%s]." (show msgMP)
        _ -> liftIO . atomically $ writeTChan ch msg''

-- | Update the bridge environment from the old one with
-- connections created.

newEnv2 :: App (Env App)
newEnv2  = do
  env@(Env bridge conf) <- ask
  -- connect to MQTT
  tups1' <- liftIO $
    mapM (runReaderT
           (runExceptT
             (getMQTTClient
               (\n mc pubReq -> runApp env (callbackFunc env n mc pubReq))
             )
           )
         ) (L.filter (\b -> connectType b == MQTTConnection) $ brokers conf)
  mapM_ (\(Left e) -> Colog.logWarning $ Text.pack e) (L.filter isLeft tups1')
  let tups1 = [t | (Right t) <- L.filter isRight tups1']
  -- update active brokers
  liftIO . atomically $ writeTVar (activeMQTT bridge) (Map.fromList tups1)
  -- subscribe
  liftIO $ mapM_ (\(n, mc) -> do
            let (fwds,_) = fromJust (Map.lookup n (rules bridge))
            subscribe mc (fwds `L.zip` L.repeat subOptions) []) tups1
  -- connect to TCP
  socksWithName' <- liftIO $
    mapM (runReaderT
           (runExceptT getSocket)
         ) (L.filter (\b -> connectType b == TCPConnection) $ brokers conf)
  mapM_ (\(Left e) -> Colog.logWarning $ Text.pack e) (L.filter isLeft socksWithName')
  let socksWithName = [t | (Right t) <- L.filter isRight socksWithName']
  tups2' <- liftIO $ mapM getClientID socksWithName
  let tups2 = [t | (Right t) <- L.filter isRight tups2']
  liftIO . atomically $ writeTVar (activeTCP bridge) (Map.fromList tups2)
  return $  Env bridge conf

getClientID :: (BrokerName, Socket)
            -> IO (Either SomeException (BrokerName, (Socket,UUID)))
getClientID (n, s) = do
  e_uuid <- try $ do
    HESP.sendMsg s $ HESP.mkArrayFromList
      [ HESP.mkBulkString "hi"
      , HESP.mkMapFromList [ (HESP.mkBulkString "pub-level",  HESP.Integer 1)
                           , (HESP.mkBulkString "pub-method", HESP.Integer 0)
                           ]
      ]
    acks <- HESP.recvMsgs s 1024
    let (Right ack) = V.head acks
        HESP.MatchArray ms = ack
        HESP.MatchBulkString uuid = (V.!) ms 2
    return $ fromJust $ UUID.fromASCIIBytes uuid
  case e_uuid of
    Left e     -> return $ Left e
    Right uuid -> return $ Right (n, (s, uuid))
