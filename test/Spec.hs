{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

import Data.List as L
import Data.Map  as Map
import Data.Maybe (fromJust)
import Data.String (fromString)
import Data.ByteString.Char8 as BS
import Data.ByteString.Lazy as BSL
import Data.Text
import Text.Printf
import System.IO
import Control.Monad
import Control.Exception
import Network.Socket
import Network.Socket.ByteString
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Async
import Happstack.Server.Internal.Listen
import GHC.Generics
import Data.Aeson
import System.Environment

import System.Random
import Control.Concurrent
import Network.Run.TCP

import HBridge

---
myBroker1 = Broker "broker1" "localhost" "19990" ["1", "2", "3"] ["1", "2", "3"]
myBroker2 = Broker "broker2" "localhost" "19991" ["2", "3"] ["1", "2"]
myBroker3 = Broker "broker3" "localhost" "19992" ["3"] ["2", "3"]
myConfig  = Config [myBroker1, myBroker2, myBroker3]
---


writeConfig :: IO ()
writeConfig = BSL.writeFile "etc/config.json" $ encode myConfig

-- Assume brokerFwds is not NULL
getBrokerArgs :: IO [(ServiceName, Topic)]
getBrokerArgs = do
  (Config bs) <- fromJust <$> parseConfig "etc/config.json"
  return $ (\b -> (brokerPort b, L.head $ brokerFwds b)) <$> bs

main :: IO ()
main = do
  writeConfig
  brokerArgs <- getBrokerArgs
  mapConcurrently_ (uncurry $ runBroker "localhost") brokerArgs

runBroker :: HostName -> ServiceName -> Topic -> IO ()
runBroker host port topic = do
  runTCPServer (Just host) port $ \s -> do
    h <- socketToHandle s ReadWriteMode
    race (receiving h) (sending h)
    return ()
  where
    sending h = forever $ do
      (ri :: Int) <- randomRIO (0, 100000)
      let msg = "[Message from " <> fromString host <> ":" <> fromString port <> " : " <> fromString (show ri) <> "]\n"
      BS.hPutStr h msg
      BS.hPutStrLn h $ fromString topic
      BS.putStr $ "[Sent] " <> msg
      threadDelay 1000000

    receiving h = forever $ do
      msg <- BS.hGetLine h
      when (not $ BS.null msg) $
        BS.putStrLn $ "[Received] " <> msg
