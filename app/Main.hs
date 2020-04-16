{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module Main where

import HBridge

import qualified Data.List                       as L
import qualified Data.Map                        as Map
import           Data.Maybe                      (isJust, fromJust)
import           Data.String                     (fromString)
import           Data.Text
import           Data.Text.Encoding
import qualified Data.ByteString                 as BS
import qualified Data.ByteString.Lazy            as BSL
import qualified Data.ByteString.Char8           as BSC (hPutStrLn)
import           Text.Printf
import           System.IO
import           Data.Time
import           Control.Monad
import           Control.Exception
import           Network.Socket
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Concurrent.Async
import           Happstack.Server.Internal.Listen
import           GHC.Generics
import           Data.Aeson
import           System.Environment
import           System.Remote.Monitoring

-- | Connect to a broker by host address and port
getHandle :: HostName
          -> ServiceName
          -> IO (Either SomeException Handle)
getHandle h p = try $ do
  addr <- L.head <$> getAddrInfo (Just $ defaultHints {addrSocketType = Stream})
          (Just h) (Just p)
  sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
  connect sock $ addrAddress addr
  socketToHandle sock ReadWriteMode

-- | Collect information from config file, connect to brokers and build a bridge
newBridge :: Config -> Logger -> IO Bridge
newBridge Config{..} logger = do
    ch               <- newBroadcastTChanIO
    tups'    <- mapM getItem brokers
    activeBs         <- newTVarIO $ Map.fromList [fromJust t | t <- tups', isJust t]
    return $ Bridge activeBs rules ch
  where
    rules = Map.fromList [(brokerName b, (brokerFwds b, brokerSubs b)) | b <- brokers]

    getItem :: Broker -> IO (Maybe (BrokerName, Handle))
    getItem b = do
      h' <- getHandle (brokerHost b) (brokerPort b)
      case h' of
        Left _  -> do
          logging logger WARNING $ printf "Failed to connect to %s (%s : %s)."
                                   (brokerName b)
                                   (brokerHost b)
                                   (brokerPort b)
          return Nothing
        Right h -> return $ Just (brokerName b, h)


main :: IO ()
main = do
  args <- getArgs
  when (L.length args > 0) $ do
    conf' <- parseConfig (args !! 0)
    case conf' of
      Left _            -> error "Failed to open file."
      Right Nothing     -> error "Failed to parse config file."
      Right (Just conf) -> do
        forkServer "localhost" 22333
        logger <- mkLogger conf
        forkFinally (logProcess logger) (\_ -> putStrLn "[Warning] Log service failed.")

        bridge <- newBridge conf logger
        initBs <- atomically $ readTVar (activeBrokers bridge)
        logging logger INFO $ "Connected to brokers:\n" ++ L.intercalate "\n" (Map.keys initBs)

        mapM_ (\tup -> process tup bridge logger) (Map.toList initBs)

        _      <- getChar
        return ()


-- | Create a thread (in fact two, receiving and forwarding)
-- for certain broker.
process :: (BrokerName, Handle)
        -> Bridge
        -> Logger
        -> IO ()
process tup@(n, h) bridge logger = do
  forkFinally (run tup bridge logger) (\e -> handleException e)
  return ()
  where
    handleException e =
      case e of
        Left e -> do
          atomically $ modifyTVar (activeBrokers bridge) (Map.delete n)
          logging logger WARNING $
            printf "Broker %s disconnected (%s)\n" n (show e)
          hClose h
        _     -> return ()

-- | Describes how the bridge receives and forwards messages
-- from/to a certain broker.
-- It is bridge-scoped and does not care about specific behaviours
-- of brokers, which is provided by fwdMessage' and 'recvMessge'.
run :: (BrokerName, Handle)
    -> Bridge
    -> Logger
    -> IO ()
run tup@(n, h) Bridge{..} logger = do
    ch <- atomically $ dupTChan broadcastChan
    race (receiving ch logger) (forwarding ch logger)
    return ()
  where
    (fwds, subs) = fromJust (Map.lookup n rules)

    receiving ch logger = forever $ do
      msg <- recvMessage h
      logging logger INFO $ printf "Received    [%s]." (show msg)
      case msg of
        Just (PlainMsg _ t) -> atomically $ do
          when (t `existMatch` fwds) $ do
            writeTChan broadcastChan (fromJust msg)
            readTChan ch
            return ()
        _                   -> return ()

    forwarding ch logger = forever $ do
      msg <- atomically (readTChan ch)
      logging logger INFO $ printf "Processing  [%s]." (show msg)
      case msg of
        PlainMsg _ t -> when (t `existMatch` subs) $ do
          fwdMessage h msg
          logging logger INFO $ printf "Forwarded   [%s]." (show msg)
        _            -> return ()



-- | Forward message to certain broker. Broker-dependent and will be
-- replaced soon.
fwdMessage :: Handle -> Message -> IO ()
fwdMessage h msg = do
  BSC.hPutStrLn h $ BSL.toStrict (encode msg)

-- | Receive message from certain broker. Broker-dependent and will be
-- replaced soon.
recvMessage :: Handle -> IO (Maybe Message)
recvMessage h = do
  s <- BS.hGetLine h
  return $ decode (BSL.fromStrict s)
