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
import           Data.Text.Encoding
import           Text.Printf
import           Data.Time
import           System.IO
import           Control.Monad
import           Control.Monad.State
import           Control.Monad.Except
import           Control.Monad.Writer
import           Network.Socket
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           GHC.Generics
import           Data.Aeson
import           Control.Concurrent
import           Network.Run.TCP
import           HBridge

-- | Write test cases here.
t1 = "home/room/temp"
t2 = "home/+/temp"
t3 = "home/#"
t4 = "office/light"
t5 = "home/ac/temp"

myBroker1 = Broker "broker1" "localhost" "19190" [t1, t2, t3] [t1, t2, t3, t5]
myBroker2 = Broker "broker2" "localhost" "19191" [t4, t3]     [t4]
myBroker3 = Broker "broker3" "localhost" "19192" [t5]         [t2, t3]
myBroker4 = Broker "broker4" "localhost" "19193" [t1, t3] [t3]

myConfig  = Config
  { brokers =  [myBroker1, myBroker2, myBroker3, myBroker4]
  , logToStdErr = True
  , logFile = "test.log"
  , logLevel = INFO
  }

-- | Write config to file.
writeConfig :: IO ()
writeConfig = BSL.writeFile "etc/config.json" (encode myConfig)

-- | Generate broker arguments. For convenience, we assume
-- 'brokerFwds' is not empty.
-- "L.take 2 bs" makes it that you can open brokers less than what
-- described in config file.
getBrokerArgs :: IO [(ServiceName, Topic)]
getBrokerArgs = do
    conf <- fromJust <$> decodeFileStrict "etc/config.json"
    return [(brokerPort b, L.head (brokerFwds b)) | b <- L.take 3 (brokers conf)]


-- | Sample payload type, for test only.
data SamplePayload = SamplePayload
  { payloadHost :: HostName
  , payloadPort :: ServiceName
  , payloadTime :: UTCTime
  }
  deriving (Show, Generic, FromJSON, ToJSON)

genMsg :: (HostName, ServiceName) -> Topic -> IO Message
genMsg (h, p) t = do
    time <- getCurrentTime
    let payload = encode $ SamplePayload h p time
    return (PlainMsg (decodeUtf8 (BSL.toStrict payload)) t)


main :: IO ()
main = do
  logger <- mkLogger myConfig
  forkFinally (logProcess logger) (\_ -> putStrLn "[Warning] Log service failed.")
  writeConfig
  brokerArgs <- getBrokerArgs
  mapConcurrently_ (\t -> runBroker "localhost" (fst t) (snd t) logger) brokerArgs

-- | Run a simple broker for test. It is in fact a simple TCP server.
runBroker :: HostName
          -> ServiceName
          -> Topic
          -> Logger
          -> IO ()
runBroker host port topic logger = do
  runTCPServer (Just host) port $ \s -> do
    h <- socketToHandle s ReadWriteMode
    race (receiving h logger) (sending h logger)
    return ()
  where
    sending h logger = forever $ do
      msg <- genMsg (host, port) topic
      let msg' = encode msg
      BS.hPutStrLn h $ BSL.toStrict msg'
      logging logger INFO $ printf "[%s:%s] Sent     [%s]" host port (unpack . decodeUtf8 . BSL.toStrict $ msg')
      threadDelay 1000000

    receiving h logger = forever $ do
      msg' <- BS.hGetLine h
      when (not (BS.null msg')) $
        logging logger INFO $ printf "[%s:%s] Received [%s]" host port (unpack . decodeUtf8 $ msg')

-- For temporarily test only
f1 = saveMsg "save1.txt"
f2 = \x -> modifyTopic x "home/+/temp" "home/temp"
f3 = saveMsg "save2.txt"
fs = [f1, f2, f3]

m1 = PlainMsg "msg1" "home/room/temp"

test = runWriterT $ runStateT (runExceptT $ foldM (\acc f -> f acc) m1 fs) 0
