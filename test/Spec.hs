{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

import qualified Data.List as L
import qualified Data.HashMap.Strict      as HM
import           Data.Maybe (fromJust)
import           Data.Either (fromRight)
import qualified Data.ByteString.Char8    as BS
import qualified Data.ByteString.Lazy     as BSL
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
import           GHC.Generics
import           Data.Aeson
import qualified Data.Yaml                 as Y
import           Control.Concurrent
import           Network.Run.TCP
import           Network.MQTT.Bridge.Types
import           Network.MQTT.Bridge.Extra
import           Network.URI
import           Network.MQTT.Types
import           Network.MQTT.Client

-- | Write test cases here.
t1 = "home/room/temp"
t2 = "home/+/temp"
t3 = "home/#"
t4 = "office/light"
t5 = "home/ac/temp"
t6 = "mountpoint_home/home/#"
t7 = "mountpoint_office/mountpoint_on_recv_office/office/+"

myBroker1 = Broker "broker1" MQTTConnection
            "mqtt://localhost:1883/mqtt"
            [t1] [t7] "mountpoint_home/"
myBroker2 = Broker "broker2" MQTTConnection
            "mqtt://localhost:1885/mqtt"
            [t4] [t6] "mountpoint_office/"
myBroker3 = Broker "broker3" TCPConnection
            "tcp://localhost:19192"
            [t5] [t2, t3] ""
myBroker4 = Broker "broker4" TCPConnection
            "tcp://localhost:19193"
            [t1, t3] [t3] ""

myConfig  = Config
  { brokers =  [myBroker1, myBroker2, myBroker3]
  , logToStdErr = True
  , logFile = "test.log"
  , logLevel = INFO
  , msgFuncs = [  ("ModifyTopic_1", ModifyTopic "office/#" "mountpoint_on_recv_office/office/light")
               ]
  }

-- | Write config to file.
writeConfig :: IO ()
writeConfig = BS.writeFile "etc/config.yaml" (Y.encode myConfig)

-- | Create a MQTT client and connect to a broker. Then
-- send test message with certain topic continuously.
runMQTTClient :: String -> Topic -> IO ()
runMQTTClient uri' t = do
  let (Just uri) = parseURI uri'
  mc <- connectURI mqttConfig uri
  subscribe mc [("#", subOptions)] []
  forever $ do
    pubAliased mc t "TEST MESSAGE" False QoS0 []
    threadDelay 1000000

-- | Generate broker arguments. For convenience, we assume
-- 'brokerFwds' is not empty.
-- "L.take 2 bs" makes it that you can open brokers less than what
-- described in config file.
getBrokerArgs :: IO [(ConnectionType, String, Topic)]
getBrokerArgs = do
    conf <- fromRight myConfig <$> Y.decodeFileEither "etc/config.yaml"
    return [(connectType b, brokerURI b, L.head (brokerFwds b)) | b <- (brokers conf)]

-- | Sample payload type, for test only.
data SamplePayload = SamplePayload
  { payloadHost :: HostName
  , payloadPort :: ServiceName
  , payloadTime :: UTCTime
  }
  deriving (Show, Generic, FromJSON, ToJSON)

-- | Generate plain message with time as payload for test purpose.
genPlainMsg :: (HostName, ServiceName) -> Topic -> IO Message
genPlainMsg (h, p) t = do
    time <- getCurrentTime
    let payload = encode $ SamplePayload h p time
    return (PlainMsg (decodeUtf8 (BSL.toStrict payload)) t)


main :: IO ()
main = do
  logger <- mkLogger myConfig
  forkFinally (logProcess logger) (\_ -> putStrLn "[Warning] Log service failed.")
  --writeConfig
  brokerArgs <- getBrokerArgs
  let tcpArgs = L.filter (\(t,_,_) -> t == TCPConnection) brokerArgs
      tcpArgs' = if not (L.null tcpArgs) then L.tail tcpArgs else [] -- the first one is for monitoring
      mqttArgs = L.filter (\(t,_,_) -> t == MQTTConnection) brokerArgs
      getPort u = fromJust $ L.tail . uriPort <$> uriAuthority (fromJust . parseURI $ u)
      getHost u = fromJust $ uriRegName <$> uriAuthority (fromJust . parseURI $ u)
  a1 <- async $ mapConcurrently_ (\(_,u,t) -> runBroker (getHost u) (getPort u) "test/tcp/msg" logger) tcpArgs'
  a2 <- async $ mapConcurrently_ (uncurry runMQTTClient) ((\(_,b,c) -> (b,c)) <$> mqttArgs)

  wait a1
  wait a2

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
      -- PlainMsg
      msg <- genPlainMsg (host, port) topic
      let msg' = encode msg
      BS.hPutStrLn h $ BSL.toStrict msg'

      -- ListFuncs
      --let msg2 = ListFuncs
      --    msg2' = encode msg2
      --BS.hPutStrLn h $ BSL.toStrict msg2'

      logging logger INFO $ printf "[%s:%s] [TCP] Sent     [%s]" host port (unpack . decodeUtf8 . BSL.toStrict $ msg')
      threadDelay 3000000

    receiving h logger = forever $ do
      msg' <- BS.hGetLine h
      when (not (BS.null msg')) $
        logging logger INFO $ printf "[%s:%s] [TCP] Received [%s]" host port (unpack . decodeUtf8 $ msg')

-- | Test message processing functions, temporarily for test only.
v1 = Object $ HM.fromList [("v1", String "v1v1v1"), ("v2", Number 114514), ("v3", Bool True)]
v2 = Object $ HM.fromList [("v11", Number 1919810), ("v12", String "v12v12v12")]

f1 = saveMsg "save1.txt"
f2 = modifyTopic "home/+/temp" "home/temp"
f3 = saveMsg "save2.txt"
f4 = modifyField ["payloadHost"] v1
f5 = saveMsg "save3.txt"
f6 = modifyField ["payloadHost", "v1"] v2
fs = [f1, f2, f3, f4, f5, f6]

m1 = PlainMsg "msg1" "home/room/temp"
gm = genPlainMsg ("localhost", "19199") "home/room/temp"

test msg = runWriterT $ runStateT (runExceptT $ foldM (\acc f -> f acc) msg fs) 0
