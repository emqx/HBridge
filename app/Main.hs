{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module Main where

import HBridge

import Data.List as L
import Data.Map  as Map
import Data.Maybe (fromJust)
import Data.String (fromString)
import Data.ByteString.Lazy as BS hiding (pack, unpack, putStr, putStrLn, hPutStr, hPutStrLn, elem)
import Data.Text
import Text.Printf
import System.IO
import Control.Monad
import Control.Exception
import Network.Socket
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Async
import Happstack.Server.Internal.Listen
import GHC.Generics
import Data.Aeson
import System.Environment
--import System.Environment.Monitoring


-- EXCEPTIONS!
getHandle :: HostName -> ServiceName -> IO Handle
getHandle h p = do
  addr <- L.head <$> getAddrInfo (Just $ defaultHints {addrSocketType = Stream}) (Just h) (Just p)
  sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
  connect sock $ addrAddress addr
  socketToHandle sock ReadWriteMode

-- EXCEPTIONS!
newBridge :: Config -> IO Bridge
newBridge (Config bs) = do
  ch <- newBroadcastTChanIO
  tups <- mapM getItem bs
  activeBs <- newTVarIO $ Map.fromList tups
  return $ Bridge activeBs rules ch
  where
    rules = Map.fromList $ (\b -> (brokerName b, (brokerFwds b, brokerSubs b))) <$> bs
    getItem b = do
      h <- getHandle (brokerHost b) (brokerPort b)
      return (brokerName b, h)

main :: IO ()
main = do
  --args <- getArgs
  --conf <- fromJust <$> parseConfig $ args !! 1 -- EXCEPTIONS!
  conf <- fromJust <$> parseConfig "etc/config.json"
  bridge <- newBridge conf
  putStrLn "Connected to brokers."
  initBs <- atomically $ readTVar (activeBrokers bridge)
  mapConcurrently_ (flip process bridge) (toList initBs)


process :: (BrokerName, Handle) -> Bridge -> IO ()
process tup@(n, h) bridge = do
  forkFinally (run tup bridge) (\_ -> handleException)
  return ()
  where
    handleException = do
      hClose h
      atomically $ modifyTVar (activeBrokers bridge) (Map.delete n)


run :: (BrokerName, Handle) -> Bridge -> IO ()
run tup@(n, h) Bridge{..} = do
  ch <- atomically $ dupTChan broadcastChan
  race (receiving ch) (forwarding ch)
  return ()
  where
    (fwdsTopics, subsTopics) = fromJust $ Map.lookup n rules
    receiving ch = forever $ do
      msg@(Message content topic) <- recvMessage h
      atomically $ do
        when (topic `elem` fwdsTopics) $ do
          writeTChan broadcastChan msg
          readTChan ch
          return ()
    forwarding ch = forever $ do
      msg@(Message content topic) <- atomically $ readTChan ch
      when (topic `elem` subsTopics) $
        fwdMessage h msg



fwdMessage :: Handle -> Message -> IO ()
fwdMessage h msg@(Message c t) =
  --hPrintf h "\n[Received] Contents = %s Topic = %s" (unpack c) t
  hPutStrLn h (unpack c)

recvMessage :: Handle -> IO Message
recvMessage h = do
  --hPutStr h "[Contents]> "
  contents <- hGetLine h
  --hPutStr h "[Topic]> "
  topic    <- hGetLine h
  return $ Message (pack contents) topic
