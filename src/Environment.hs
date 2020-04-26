{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE MultiParamTypeClasses #-}

{-# LANGUAGE FlexibleInstances #-}

{-
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE StandaloneDeriving #-}
-}

module Environment
  ( getHandle
  , getMQTTClient
  , newEnv1
  , newEnv2
  ) where

import qualified Data.List               as L
import qualified Data.ByteString.Lazy    as BL
import           Data.Map                as Map
import           Data.Maybe              (isNothing, fromJust)
import           Data.Either             (isLeft, isRight)
import           Data.Time
import qualified Data.Serialize          as S
import           System.IO
import           Text.Printf
import           Network.Socket
import           Network.URI
import           Control.Exception
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Except
import           Control.Concurrent.STM
import           Network.MQTT.Client
import           Network.MQTT.Types

import qualified Database.RocksDB       as R
import qualified Database.RocksDB.Query as RQ

import           Types
import           Extra


-- | Create a TCP connection and return the handle with broker name.
-- It can catch exceptions when making a connection.
getHandle :: ExceptT String (ReaderT Broker IO) (BrokerName, Handle)
getHandle = do
  b@(Broker n t uri _ _ _) <- ask

  let host' = uriRegName <$> uriAuthority uri
      port' = L.tail . uriPort <$> uriAuthority uri

  when (isNothing host' || isNothing port') $
    throwError $ printf "Invalid URI : %s ." (show uri)

  (h' :: Either SomeException Handle) <- liftIO . try $ do
    addr <- L.head <$> getAddrInfo (Just $ defaultHints {addrSocketType = Stream})
                       host' port'
    sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    connect sock $ addrAddress addr
    socketToHandle sock ReadWriteMode
  case h' of
    Left e -> throwError $
              printf "Failed to connect to %s (%s:%s) : %s."
                     n (fromJust host') (fromJust port') (show e)
    Right h -> return (n, h)

type LowLevelCallbackType = BrokerName -> MQTTClient -> PublishRequest -> IO ()

-- | Create a MQTT connection and return the client with broker name.
-- It can catch exceptions when making a connection.
getMQTTClient :: LowLevelCallbackType
              -> ExceptT String (ReaderT Broker IO) (BrokerName, MQTTClient)
getMQTTClient callbackFunc = do
  b@(Broker n t uri _ _ _) <- ask

  (mc' :: Either SomeException MQTTClient) <- liftIO . try $ do
    connectURI mqttConfig{_msgCB = LowLevelCallback (callbackFunc n)} uri
  case mc' of
    Left e -> throwError $
              printf "Failed to connect to %s." (show uri)
    Right mc -> return (n, mc)


instance RQ.KeyValue String (BL.ByteString, BL.ByteString)

-- | Create basic bridge environment without connections created.
newEnv1 :: ReaderT Config IO Env
newEnv1 = do
  conf@(Config bs _ _ _ db') <- ask
  logger                     <- liftIO $ mkLogger conf
  db                         <- R.open db' R.defaultOptions{R.createIfMissing = True}
  ch                         <- liftIO newBroadcastTChanIO
  activeMCs                  <- liftIO $ newTVarIO Map.empty
  activeTCPs                 <- liftIO $ newTVarIO Map.empty
  funcs                      <- liftIO $ newTVarIO
    [ ("SaveMsg_1", saveMsg "save1.txt")
    , ("ModifyTopic_1", \x -> modifyTopic x "office/#" "mountpoint_on_recv_office/office/light")
    , ("saveMsg_2", saveMsg "save2.txt")
    ]
  let rules = Map.fromList [(brokerName b, (brokerFwds b, brokerSubs b)) | b <- bs]
      mps   = Map.fromList [(brokerName b, brokerMount b) | b <- bs]
  return $ Env
    { envBridge = Bridge activeMCs activeTCPs rules mps ch funcs
    , envConfig = conf
    , envLogger = logger
    , envDB     = db
    }

-- | Update the bridge environment from the old one with
-- connections created.
newEnv2 :: StateT Env IO ()
newEnv2 = do
  Env bridge conf logger db <- get
  let ch = broadcastChan bridge
      mps = mountPoints bridge
      callbackFunc n mc pubReq = do
        logging logger INFO $ printf "Received    [%s]." (show $ PubPkt pubReq n)

        -- save to DB
        time <- getCurrentTime
        RQ.insert db (show time) (_pubTopic pubReq, _pubBody pubReq)

        -- message transformation
        funcs' <- readTVarIO (functions bridge)
        let funcs = snd <$> funcs'
            mp = fromJust (Map.lookup n mps)
        ((msg', s), l) <- runFuncSeries (PubPkt pubReq n) funcs
        case msg' of
          Left e      -> logging logger WARNING $ printf "Message transformation failed: [%s]."
                                                         (show $ PubPkt pubReq n)
          Right msg'' -> do
            logging logger INFO $ printf "Message transformation succeeded: [%s] to [%s]."
                                         (show $ PubPkt pubReq n)
                                         (show msg'')
            -- mountpoint
            case msg'' of
              PubPkt pubReq'' n'' -> do
                let msgMP = PubPkt pubReq''{_pubTopic = textToBL $ mp `composeMP` (blToText (_pubTopic pubReq''))} n''
                atomically $ writeTChan ch msgMP
                logging logger INFO $ printf "Mountpoint added: [%s]." (show msgMP)
              _ -> atomically $ writeTChan ch msg''

  -- connect to MQTT
  tups1' <- liftIO $ mapM (runReaderT (runExceptT (getMQTTClient callbackFunc)))
                        (L.filter (\b -> connectType b == MQTTConnection) $ brokers conf)
  liftIO $ mapM_ (\(Left e) -> logging logger WARNING e) (L.filter isLeft tups1')
  let tups1 = [t | (Right t) <- (L.filter isRight tups1')]
  liftIO . atomically $ writeTVar (activeMQTT bridge) (Map.fromList tups1)

  -- update active brokers
  liftIO . atomically $ writeTVar (activeMQTT bridge) (Map.fromList tups1)

  -- subscribe
  liftIO $ mapM_ (\(n, mc) -> do
            let (fwds, subs) = fromJust (Map.lookup n (rules bridge))
            subscribe mc (fwds `L.zip` L.repeat subOptions) []) tups1

  -- connect to TCP
  tups2' <- liftIO $ mapM (runReaderT (runExceptT getHandle))
                         (L.filter (\b -> connectType b == TCPConnection) $ brokers conf)
  liftIO $ mapM_ (\(Left e) -> logging logger WARNING e) (L.filter isLeft tups2')
  let tups2 = [t | (Right t) <- (L.filter isRight tups2')]
  liftIO . atomically $ writeTVar (activeTCP bridge) (Map.fromList tups2)

  -- commit changes
  put $ Env bridge conf logger db
