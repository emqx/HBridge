{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

import qualified Data.List as L
import           Data.Maybe (fromJust)
import           Data.String (fromString)
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BSL
import           Data.Text
import           System.IO
import           Control.Monad
import           Network.Socket
import           Control.Concurrent.Async
import           GHC.Generics
import           Data.Aeson
import           System.Random
import           Control.Concurrent
import           Network.Run.TCP
import           HBridge

-- | Write test cases here.
t1 = "home/room/temp"
t2 = "home/+/temp"
t3 = "home/#"
t4 = "office/light"

myBroker1 = Broker "broker1" "localhost" "19990" [t1, t2, t3] [t1, t2, t3]
myBroker2 = Broker "broker2" "localhost" "19991" [t2, t3]     [t4]
myBroker3 = Broker "broker3" "localhost" "19992" [t3]         [t2, t3]
myConfig  = Config [myBroker1, myBroker2, myBroker3]

-- | Write config to file.
writeConfig :: IO ()
writeConfig = BSL.writeFile "etc/config.json" (encode myConfig)

-- | Generate broker arguments. For convenience, we assume
-- 'brokerFwds' is not empty.
-- "L.take 2 bs" makes it that you can open brokers less than what
-- described in config file.
getBrokerArgs :: IO [(ServiceName, Topic)]
getBrokerArgs = do
    (Config bs) <- fromJust <$> decodeFileStrict "etc/config.json"
    return [(brokerPort b, L.head (brokerFwds b)) | b <- L.take 2 bs]


main :: IO ()
main = do
  writeConfig
  brokerArgs <- getBrokerArgs
  mapConcurrently_ (uncurry $ runBroker "localhost") brokerArgs

-- | Run a simple broker for test. It is in fact a simple TCP server.
runBroker :: HostName
          -> ServiceName
          -> Topic
          -> IO ()
runBroker host port topic = do
  runTCPServer (Just host) port $ \s -> do
    h <- socketToHandle s ReadWriteMode
    race (receiving h) (sending h)
    return ()
  where
    sending h = forever $ do
      (ri :: Int) <- randomRIO (0, 100000)
      let msg = "[Message from "
              <> fromString host
              <> ":"   <> fromString port
              <> " : " <> fromString (show ri)
              <> "]\n"
      BS.hPutStr h msg
      BS.hPutStrLn h (fromString topic)
      BS.putStr $ "[Sent]     " <> msg
      threadDelay 1000000

    receiving h = forever $ do
      msg <- BS.hGetLine h
      when (not (BS.null msg)) $
        BS.putStrLn $ "[Received] " <> msg
