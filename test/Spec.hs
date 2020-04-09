{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}

import HBridge

import Data.List as L
import Data.Map  as Map
--import Data.Text
--import Text.Printf
import System.IO
import Control.Monad
import Control.Exception
import Network.Socket
import Network.Socket.ByteString
import Control.Concurrent
--import Control.Concurrent.STM
import Control.Concurrent.Async
--import Happstack.Server.Internal.Listen

import Network.Run.TCP
--import System.Environment
import System.Random
import Control.Concurrent
import Data.String


main = do
  --arg <- getLine
  --let (n :: Int) = read arg
  let n = 1
  let confs = L.zip (("Name" ++ ) <$> (show <$> [1..n])) (show <$> L.replicate n [1])
  --mapM_ runClient confs
  mapConcurrently_ runClient confs


runClient :: (String, String) -> IO ()
runClient (name, topics) = do
  runTCPClient "localhost" "19198" $ \s -> do
    sendAll s (fromString name <> "\n")
    sendAll s (fromString topics <> "\n")
    forever $ do
      (ri :: Int) <- randomRIO (0, 100000)
      let msg = "[Message from " <> fromString name <> ": " <> fromString (show ri) <> "]\n"
      sendAll s msg
      sendAll s "TestTopic\n"

      print $ "[Sent] " <> msg
      threadDelay 1000000

{-
runClient = do
  args <- getArgs
  runTCPClient "localhost" "19198" $ \s -> do
    --nameMsg <- recv s 1024
    sendAll s (fromString (args !! 0) <> "\n")
    --topicsMsg <- recv 1024
    sendAll s (fromString (args !! 1) <> "\n")
    --contentsMsg <- recv s 1024
    forever $ do
      (ri :: Int) <- randomRIO (0, 100000)
      let msg = "[Message from " <> fromString (args !! 0) <> ": " <> fromString (show ri) <> "]\n"
      sendAll s msg
      --topicMsg <- recv s 1024
      sendAll s "TestTopic\n"

      print $ "[Sent] " <> msg
      threadDelay 1000000
-}
