{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module Main where

import HBridge

import qualified Data.List                       as L
import qualified Data.HashMap.Strict             as HM
import qualified Data.Map                        as Map
import           Data.Maybe                      (isJust, fromJust)
import           Data.Either                     (isLeft, isRight)
import           Data.String                     (fromString)
import           Data.Text
import           Data.Text.Encoding
import qualified Data.ByteString                 as BS
import qualified Data.ByteString.Lazy            as BL
import qualified Data.ByteString.Char8           as BSC (hPutStrLn)
import           Text.Printf
import           System.IO
import           Data.Time
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.Writer
import           Control.Monad.State
import           Control.Monad.Except
import           Control.Exception
import           GHC.IO.Exception
import           Network.Socket
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Concurrent.Async
import           Happstack.Server.Internal.Listen
import           GHC.Generics
import           Data.Aeson
import           System.Environment
import           System.Remote.Monitoring

import           Network.MQTT.Types
import           Network.MQTT.Client


{-
getHandle :: ExceptT String (ReaderT Broker IO) (BrokerName, Handle)
getHandle = do
  b@(Broker n h p _ _) <- ask
  (h' :: Either SomeException Handle) <- liftIO . try $ do
    addr <- L.head <$> getAddrInfo (Just $ defaultHints {addrSocketType = Stream})
          (Just h) (Just p)
    sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    connect sock $ addrAddress addr
    socketToHandle sock ReadWriteMode
  case h' of
    Left e -> throwError $ printf "Failed to connect to %s (%s:%s) : %s." n h p (show e)
    Right h -> return (n, h)

-- | Connect to brokers and get handles; make a logger and finally
-- generate Env
newEnv :: ReaderT Config IO Env
newEnv = do
  conf@(Config bs _ _ _) <- ask    -- envConfig
  logger <- liftIO $ mkLogger conf -- envLogger

  ch <- liftIO newBroadcastTChanIO -- broadcastChan


  -- FOR TEST
  -- funcs <- liftIO $ newTVarIO []     -- functions
  let v1 = Object $ HM.fromList [("v1", String "v1v1v1"), ("v2", Number 114514), ("v3", Bool True)]
  funcs <- liftIO $ newTVarIO [ ("ModifyTopic_1", \x -> modifyTopic x "home/+/temp" "home/temp")
                              , ("ModifyField_1", \x -> modifyField x ["payloadHost"] v1)
                              ]
  --


  tups' <- liftIO $ mapM (runReaderT (runExceptT getHandle)) bs
  liftIO $ mapM_ (\(Left e) -> logging logger WARNING e) (L.filter isLeft tups') -- Failed to connect, log
  activeBs <- liftIO . newTVarIO $ Map.fromList [ t | (Right t) <- (L.filter isRight tups')]
  let rules = Map.fromList [(brokerName b, (brokerFwds b, brokerSubs b)) | b <- bs]
  return $ Env
    { envBridge = Bridge activeBs rules ch funcs
    , envConfig = conf
    , envLogger = logger
    }
-}

newEnv1 :: ReaderT Config IO Env
newEnv1 = do
  conf@(Config bs _ _ _) <- ask
  logger <- liftIO $ mkLogger conf
  ch <- liftIO newBroadcastTChanIO
  activeBs <- liftIO $ newTVarIO Map.empty
  funcs <- liftIO $ newTVarIO []
  let rules = Map.fromList [(brokerName b, (brokerFwds b, brokerSubs b)) | b <- bs]
  return $ Env
    { envBridge = Bridge activeBs rules ch funcs
    , envConfig = conf
    , envLogger = logger
    }

newEnv2 :: StateT Env IO ()
newEnv2 = do
  Env bridge conf logger <- get
  let ch = broadcastChan bridge
  let callback n = LowLevelCallback (\mc pubReq -> do
                                      atomically $ do
                                        writeTChan ch (PubPkt pubReq n)

                                      print (_pubTopic pubReq)
                                      print (_pubBody pubReq))

  -- connect
  tups <- liftIO $ mapM (\b -> do
                  mc <- connectURI mqttConfig{_msgCB = callback (brokerName b)} (uri b)
                  return ((brokerName b), mc)) (brokers conf)

  -- update active brokers
  liftIO . atomically $ writeTVar (activeBrokers bridge) (Map.fromList tups)
  put $ Env bridge conf logger

  -- subscribe
  liftIO $ mapM_ (\(n, mc) -> do
            let (fwds, subs) = fromJust (Map.lookup n (rules bridge))
            subscribe mc (subs `L.zip` L.repeat subOptions) []) tups


main :: IO ()
main = do
  conf' <- runExceptT parseConfig
  case conf' of
    Left e -> error e
    Right Nothing -> error "Failed to parse config file."
    Right (Just conf) -> do
      forkServer "localhost" 22333                        -- ekg
      {-
      env@(Env bridge _ logger) <- runReaderT newEnv conf -- create env
      -}
      env' <- runReaderT newEnv1 conf
      env@(Env bridge _ logger) <- execStateT newEnv2 env'
      forkFinally (logProcess logger) (\_ -> putStrLn "[Warning] Log service failed.")

      initBs <- atomically $ readTVar (activeBrokers bridge)
      logging logger INFO $ "Connected to brokers:\n" ++ L.intercalate "\n" (Map.keys initBs)
      mapM_ (\tup -> process tup env) (Map.toList initBs)

      _      <- getChar
      return ()


-- | Create a thread (in fact two, receiving and forwarding)
-- for certain broker.
process :: (BrokerName, MQTTClient)
        -> Env
        -> IO ()
process tup@(n, mc) env@(Env Bridge{..} _ logger) = do
    forkFinally (run tup env) (\e -> handleException e)
    return ()
  where
    handleException e = case e of
        Left e -> do
          atomically $ modifyTVar activeBrokers (Map.delete n)
          logging logger WARNING $ printf "Broker %s disconnected (%s)\n" n (show e)
          --hClose h
        _     -> return ()

-- | Describes how the bridge receives and forwards messages
-- from/to a certain broker.
-- It is bridge-scoped and does not care about specific behaviours
-- of brokers, which is provided by fwdMessage' and 'recvMessge'.
run :: (BrokerName, MQTTClient)
    -> Env
    -> IO ()
run (n, mc) (Env Bridge{..} _ logger) = do
    ch <- atomically $ dupTChan broadcastChan
    --race (receiving ch logger) (forwarding ch logger)
    forkFinally (forwarding ch logger) (\e -> return ())
    return ()
  where
    (fwds, subs) = fromJust (Map.lookup n rules)
    {-
    receiving ch logger = forever $ do
      msg <- recvMessage h
      logging logger INFO $ printf "Received    [%s]." (show msg)
      case msg of

        Just (PlainMsg _ t) -> do
          when (t `existMatch` fwds) $ do
            funcs' <- atomically $ readTVar functions
            let funcs = snd <$> funcs'
            ((msg', s), l) <- runFuncSeries (fromJust msg) funcs
            case msg' of
              Left e -> logging logger WARNING "Message transformation failed."
              Right msg'' -> atomically $ do
                writeTChan broadcastChan msg''
                readTChan ch
                return ()

        Just ListFuncs -> do
          funcs <- atomically $ readTVar functions
          let (items :: [String]) = L.map (\(i,s) -> show i ++ " " ++ s ++ "\n") ([0..] `L.zip` (fst <$> funcs))
          logging logger INFO $ "Functions:\n" ++ L.concat items


        Just (InsertSaveMsg n i f) -> do
          funcs <- atomically $ readTVar functions
          atomically $ writeTVar functions (insertToN i (n, saveMsg f) funcs)
          logging logger INFO $ printf "Function %s : save message to file %s." n f

        Just (InsertModifyTopic n i t t') -> do
          funcs <- atomically $ readTVar functions
          atomically $ writeTVar functions (insertToN i (n, \x -> modifyTopic x t t') funcs)
          logging logger INFO $ printf "Function %s : modify topic %s to %s." n t t'

        Just (InsertModifyField n i fs v) -> do
          funcs <- atomically $ readTVar functions
          atomically $ writeTVar functions (insertToN i (n, \x -> modifyField x fs v) funcs)
          logging logger INFO $ printf "Function %s : modify field %s to %s." n (show fs) (show v)

        _                   -> return ()
    -}
    forwarding ch logger = forever $ do
      msg <- atomically (readTChan ch)
      logging logger INFO $ printf "Processing  [%s]." (show msg)
      case msg of
        {-
        PlainMsg _ t -> when (t `existMatch` subs) $ do
          fwdMessage h msg
          logging logger INFO $ printf "Forwarded   [%s]." (show msg)
        -}
        PubPkt p n' -> when ((decodeUtf8 . BL.toStrict $ _pubTopic p) `existMatch` subs && n /= n') $ do
          pubAliased mc (decodeUtf8 . BL.toStrict $ _pubTopic p) (_pubBody p) (_pubRetain p) (_pubQoS p) (_pubProps p)
        _            -> return ()



{-
-- | Forward message to certain broker. Broker-dependent and will be
-- replaced soon.
fwdMessage :: Handle -> Message -> IO ()
fwdMessage h msg = do
  BSC.hPutStrLn h $ BL.toStrict (encode msg)

-- | Receive message from certain broker. Broker-dependent and will be
-- replaced soon.
recvMessage :: Handle -> IO (Maybe Message)
recvMessage h = do
  s <- BS.hGetLine h
  return $ decode (BL.fromStrict s)
-}
