{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}

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


type BrokerName = String
type Topic      = String
data Message    = Message Text Topic

data Broker = Broker
  { brokerName   :: String
  , brokerHandle :: Handle
  , brokerSubs   :: [Topic]
  , brokerChan   :: TChan Message
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
                , brokerChan   = ch
                }

sendMessage :: Message -> Broker -> STM ()
sendMessage msg Broker{..} = writeTChan brokerChan msg


data Bridge = Bridge
  { brokers :: TVar (Map BrokerName Broker)
  }

newBridge :: IO Bridge
newBridge = do
  v <- newTVarIO Map.empty
  return $ Bridge v

-- Topic rules is unavailiable now
broadcast :: Message -> BrokerName -> Bridge -> STM ()
broadcast msg@(Message _ t) n Bridge{..} = do
  bs <- readTVar brokers
  --mapM_ (sendMessage msg) $ L.filter (\b -> t `elem` brokerSubs b) (Map.elems bs)
  mapM_ (sendMessage msg) $ Map.elems (Map.delete n bs)

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
          hPutStrLn h "[Topics]> "
          t <- hGetLine h
          -- let (t' :: [Topic]) = read t -- EXCEPTIONS!
          let (t' :: [Topic]) = []
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
                  hPrintf h "Current Brokers: %s\n" (show $ bs)
                  --
                  restore (runBroker broker bridge) `finally` removeBroker bridge n


runBroker :: Broker -> Bridge -> IO ()
runBroker broker@Broker{..} bridge@Bridge{..} = do
  concurrently sending receive
  return ()
  where
    receive = forever $ do
      hPutStr brokerHandle "[Contents]> "
      contents <- hGetLine brokerHandle
      hPutStr brokerHandle "[Topic]> "
      topic   <- hGetLine brokerHandle
      atomically $ sendMessage (Message (pack contents) topic) broker

    sending = forever $ do
      msg@(Message c t) <- atomically $ readTChan brokerChan
      hPrintf brokerHandle "[Received] Contents = %s Topic = %s\n" (unpack c) t
      atomically $ broadcast msg brokerName bridge
