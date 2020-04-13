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
import           Text.Printf
import           System.IO
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
newBridge :: Config -> IO Bridge
newBridge (Config bs) = do
    ch       <- newBroadcastTChanIO
    tups'    <- mapM getItem bs
    activeBs <- newTVarIO $ Map.fromList [fromJust t | t <- tups', isJust t]
    return $ Bridge activeBs rules ch
  where
    rules = Map.fromList [(brokerName b, (brokerFwds b, brokerSubs b)) | b <- bs]

    getItem :: Broker -> IO (Maybe (BrokerName, Handle))
    getItem b = do
      h' <- getHandle (brokerHost b) (brokerPort b)
      case h' of
        Left _  -> do
          printf "Warning: Failed to connect to %s (%s : %s).\n"
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
      Left _            -> putStrLn "Err: Failed to open file."
      Right Nothing     -> putStrLn "Err: Failed to parse config file."
      Right (Just conf) -> do
        forkServer "localhost" 22333
        bridge <- newBridge conf
        initBs <- atomically $ readTVar (activeBrokers bridge)
        putStrLn "Info: Connected to brokers:"
        mapM_ putStrLn (Map.keys initBs)

        mapM_ (flip process bridge) (Map.toList initBs)

        c      <- getChar
        return ()

-- | Create a thread (in fact two, receiving and forwarding)
-- for certain broker.
process :: (BrokerName, Handle)
        -> Bridge
        -> IO ()
process tup@(n, h) bridge = do
  forkFinally (run tup bridge) (\_ -> handleException)
  return ()
  where
    handleException = do
      atomically $ modifyTVar (activeBrokers bridge) (Map.delete n)
      printf "Warning: Broker %s disconnected.\n" n
      hClose h

-- | Describes how the bridge receives and forwards messages
-- from/to a certain broker.
-- It is bridge-scoped and does not care about specific behaviours
-- of brokers, which is provided by fwdMessage' and 'recvMessge'.
run :: (BrokerName, Handle)
    -> Bridge
    -> IO ()
run tup@(n, h) Bridge{..} = do
    ch <- atomically $ dupTChan broadcastChan
    race (receiving ch) (forwarding ch)
    return ()
  where
    (fwds, subs) = fromJust (Map.lookup n rules)
    receiving ch = forever $ do
      msg@(Message _ topic) <- recvMessage h
      atomically $ do
        when (topic `existMatch` fwds) $ do
          writeTChan broadcastChan msg
          readTChan ch
          return ()
    forwarding ch = forever $ do
      msg@(Message _ topic) <- atomically (readTChan ch)
      when (topic `existMatch` subs) (fwdMessage h msg)




-- | Forward message to certain broker. Broker-dependent and will be
-- replaced soon.
fwdMessage :: Handle -> Message -> IO ()
fwdMessage h (Message p t) = hPutStrLn h (unpack p)

-- | Receive message from certain broker. Broker-dependent and will be
-- replaced soon.
recvMessage :: Handle -> IO Message
recvMessage h = do
  p <- hGetLine h
  t <- hGetLine h
  return $ Message (pack p) t
