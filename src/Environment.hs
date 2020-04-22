{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module Environment where

import qualified Data.List                       as L
import           Data.Map                        as Map
import           Data.Maybe                      (isJust, fromJust)
import           Data.Either                     (isLeft, isRight)
import qualified Data.HashMap.Strict             as HM
import           Data.Text
import           Data.Text.Encoding
import qualified Data.ByteString                 as BS
import qualified Data.ByteString.Lazy            as BL
import           Data.Functor.Identity
import           System.IO
import           System.Environment
import           Text.Printf
import           Data.Time
import           Network.Socket
import           Control.Exception
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Except
import           Control.Monad.Writer
import           Control.Concurrent
import           Control.Concurrent.STM
import           Data.Aeson
import           GHC.Generics

import           Network.MQTT.Topic
import           Network.MQTT.Client
import           Network.MQTT.Types
import           Network.URI

import HBridge


-- | Create a TCP connection and return the handle.
getHandle :: ExceptT String (ReaderT Broker IO) (BrokerName, Handle)
getHandle = do
  b@(Broker n t h p _ _ _ _) <- ask
  (h' :: Either SomeException Handle) <- liftIO . try $ do
    addr <- L.head <$> getAddrInfo (Just $ defaultHints {addrSocketType = Stream})
          (Just h) (Just p)
    sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    connect sock $ addrAddress addr
    socketToHandle sock ReadWriteMode
  case h' of
    Left e -> throwError $ printf "Failed to connect to %s (%s:%s) : %s." n h p (show e)
    Right h -> return (n, h)

-- | Create basic bridge environment without connections created.
newEnv1 :: ReaderT Config IO Env
newEnv1 = do
  conf@(Config bs _ _ _) <- ask
  logger                 <- liftIO $ mkLogger conf
  ch                     <- liftIO newBroadcastTChanIO
  activeMCs              <- liftIO $ newTVarIO Map.empty
  activeTCPs             <- liftIO $ newTVarIO Map.empty
  funcs                  <- liftIO $ newTVarIO [ ("ModifyTopic_1", \x -> modifyTopic x "office/#" "mountpoint_on_recv_office/office/light") ]
  let rules = Map.fromList [(brokerName b, (brokerFwds b, brokerSubs b)) | b <- bs]
      mps   = Map.fromList [(brokerName b, brokerMount b) | b <- bs]
  return $ Env
    { envBridge = Bridge activeMCs activeTCPs rules mps ch funcs
    , envConfig = conf
    , envLogger = logger
    }

-- | Update the bridge environment from the old one with
-- connections created.
newEnv2 :: StateT Env IO ()
newEnv2 = do
  Env bridge conf logger <- get
  let ch = broadcastChan bridge
      callbackFunc n mc pubReq = do
        logging logger INFO $ printf "Received    [%s]." (show $ PubPkt pubReq n)
        -- message transformation
        funcs' <- readTVarIO (functions bridge)
        let funcs = snd <$> funcs'
        ((msg', s), l) <- runFuncSeries (PubPkt pubReq n) funcs
        case msg' of
          Left e      -> logging logger WARNING $ printf "Message transformation failed: [%s]."
                                                         (show $ PubPkt pubReq n)
          Right msg'' -> do
            logging logger INFO $ printf "Message transformation succeeded: [%s] to [%s]."
                                         (show $ PubPkt pubReq n)
                                         (show msg'')
            atomically $ writeTChan ch msg''

  -- connect to MQTT
  tups <- liftIO $ mapM (\b -> do
    mc <- connectURI mqttConfig{_msgCB = LowLevelCallback $ callbackFunc (brokerName b)} (uri b)
    return (brokerName b, mc)) (L.filter (\b -> connectType b == MQTTConnection) $ brokers conf)

  -- update active brokers
  liftIO . atomically $ writeTVar (activeMQTT bridge) (Map.fromList tups)

  -- subscribe
  liftIO $ mapM_ (\(n, mc) -> do
            let (fwds, subs) = fromJust (Map.lookup n (rules bridge))
            subscribe mc (fwds `L.zip` L.repeat subOptions) []) tups

  -- connect to TCP
  tups' <- liftIO $ mapM (runReaderT (runExceptT getHandle)) (L.filter (\b -> connectType b == TCPConnection) $ brokers conf)
  liftIO $ mapM_ (\(Left e) -> logging logger WARNING e) (L.filter isLeft tups') -- Failed to connect, log
  liftIO . atomically $ writeTVar (activeTCP bridge) (Map.fromList [ t | (Right t) <- (L.filter isRight tups')])

  -- commit changes
  put $ Env bridge conf logger
