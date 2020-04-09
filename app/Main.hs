{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}

module Main where

import HBridge

import Data.List as L
import Data.Map  as Map
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

type BrokerName  = String
type Topic       = String

-- FOR NO USE NOW
data MessageType = FromClient
                 | FromBroker
                 deriving (Eq, Show)

data Message     = Message MessageType Text Topic

data Broker = Broker
  { brokerName   :: String
  , brokerHandle :: Handle
  , brokerSubs   :: [Topic]
  }
-- DEBUGGING
instance Show Broker where
  show Broker{..} = "Name: " ++ brokerName ++ " Topics: " ++ show brokerSubs


newBroker :: BrokerName -> Handle -> [Topic] -> STM Broker
newBroker n h subs = do
  ch <- newTChan
  return Broker { brokerName   = n
                , brokerHandle = h
                , brokerSubs   = subs
                }

data Bridge = Bridge
  { brokers :: TVar (Map BrokerName Broker)
  , broadcastChan :: TChan Message
  }

newBridge :: IO Bridge
newBridge = do
  v <- newTVarIO Map.empty
  ch <- newBroadcastTChanIO
  return $ Bridge v ch


fwdMessage :: Handle -> Message -> IO ()
fwdMessage h msg@(Message typ c t) =
  hPrintf h "\n[Received] Type = %s Contents = %s Topic = %s" (show typ) (unpack c) t

recvMessage :: Handle -> IO (String, String)
recvMessage h = do
  hPutStr h "[Contents]> "
  contents <- hGetLine h
  hPutStr h "[Topic]> "
  topic    <- hGetLine h
  return (contents, topic)

matchTopic :: Topic -> Topic -> Bool
matchTopic topic template = topic == template


bridgePort :: Int
bridgePort = 19198

main :: IO ()
main = withSocketsDo $ do
  bridge <- newBridge
  sock   <- listenOn bridgePort
  printf "Listening on port %d\n" bridgePort
  forever $ do
    (sock', SockAddrInet port addr) <- accept sock -- PARTIAL!
    printf "Accepted connection from %s : %s\n" (show $ hostAddressToTuple addr) (show port)
    handle <- socketToHandle sock' ReadWriteMode
    forkFinally (run handle bridge) (\_ -> hClose handle)


checkAddBroker :: Bridge -> BrokerName -> Handle -> [Topic] -> IO (Maybe Broker)
checkAddBroker Bridge{..} n h t = atomically $ do
  bs <- readTVar brokers
  if n `Map.member` bs
  then return Nothing
  else do
    b <- newBroker n h t
    modifyTVar brokers (Map.insert n b)
    return (Just b)

removeBroker :: Bridge -> BrokerName -> IO ()
removeBroker Bridge{..} n = atomically $ do
  modifyTVar brokers (Map.delete n)

run :: Handle -> Bridge -> IO ()
run h bridge@Bridge{..} = do
  getName
  where getName = do
          hPutStr h "[Broker Name]> "
          n <- hGetLine h
          hPutStr h "[Topics]> "
          t <- hGetLine h
          --let (t' :: [Topic]) = read t -- EXCEPTIONS!
          let (t' :: [Topic]) = ["1", "2", "3"]
          if L.null n then getName
          else mask $ \restore -> do
              chk <- checkAddBroker bridge n h t'
              case chk of
                Nothing -> restore $ do
                  hPrintf h "Name %s is in use, choose another\n" n
                  getName
                Just broker -> do
                  --
                  bs <- atomically $ readTVar brokers
                  hPrintf h "Current Brokers: %s\n" (show bs)
                  --
                  restore (runBroker broker bridge) `finally` removeBroker bridge n


runBroker :: Broker -> Bridge -> IO ()
runBroker broker@Broker{..} bridge@Bridge{..} = do
  ch <- atomically $ dupTChan broadcastChan
  race (forwarding ch) (receive ch)
  return ()
  where
    receive ch = forever $ do
      (contents, topic) <- recvMessage brokerHandle
      atomically $ do
        writeTChan broadcastChan (Message FromClient (pack contents) topic)
        readTChan ch

    forwarding ch = forever $ do
      msg@(Message _ _ t) <- atomically (readTChan ch)
      when (True `elem` (matchTopic t <$> brokerSubs)) $
        fwdMessage brokerHandle msg


{-
data BrokerConfig = BrokerConfig
  { host :: HostName
  , port :: ServiceName
  , name :: BrokerName
  , topics :: [Topic]
  }
  deriving (Show, Generic)

data Config = Config [BrokerConfig] deriving (Show, Generic)
-}
