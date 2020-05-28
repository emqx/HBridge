{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Monad
import           Data.Aeson
import qualified Data.ByteString.Char8     as BS
import qualified Data.ByteString.Lazy      as BSL
import           Data.Either               (fromRight, isRight)
import qualified Data.List                 as L
import           Data.Maybe                (fromJust)
import           Data.Time
import qualified Data.Yaml                 as Y
import           GHC.Generics
import           Network.MQTT.Bridge.Extra
import           Network.MQTT.Bridge.Types
import           Network.MQTT.Client
import           Network.Simple.TCP
import           Network.URI
import           Text.Printf

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
  { brokers =  [myBroker1, myBroker2, myBroker3, myBroker4]
  , logToStdErr = True
  , logFile = "test.log"
  , logLevel = INFO
  , crossForward = True
  , sqlFiles = ["etc/sql.txt"]
  }

-- | Write config to file.
writeConfig :: IO ()
writeConfig = BS.writeFile "etc/config.yaml" (Y.encode myConfig)



data Foo = Foo
    { temp     :: Double
    , humidity :: Int
    , location :: String
    }
    deriving (Show, Generic, FromJSON, ToJSON)

foo = Foo 27.5 70 "Hangzhou"
msgBody = encode foo

-- | Create a MQTT client and connect to a broker. Then
-- send test message with certain topic continuously.
runMQTTClient :: String -> Topic -> IO ()
runMQTTClient uri' t = do
  let (Just uri) = parseURI uri'
  mc <- connectURI mqttConfig uri
  subscribe mc [("#", subOptions)] []
  forever $ do
    pubAliased mc t msgBody False QoS0 []
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
    return (PlainMsg payload t)


main :: IO ()
main = do
  logger <- mkLogger myConfig
  forkFinally (logProcess logger) (\e -> printf "[Warning] Log service failed : %s." (show e))
  --writeConfig
  brokerArgs <- getBrokerArgs
  let tcpArgs = L.filter (\(t,_,_) -> t == TCPConnection) brokerArgs
      --tcpArgs' = if not (L.null tcpArgs) then L.tail tcpArgs else [] -- the first one is for monitoring
      --tcpArgs' = L.init tcpArgs
      tcpArgs' = tcpArgs
      mqttArgs = L.filter (\(t,_,_) -> t == MQTTConnection) brokerArgs
      getPort u = fromJust $ L.tail . uriPort <$> uriAuthority (fromJust . parseURI $ u)
      getHost u = fromJust $ uriRegName <$> uriAuthority (fromJust . parseURI $ u)
  a1 <- async $ mapConcurrently_ (\(_,u,t) -> runBroker (getHost u) (getPort u) "tcp/test/msg" logger) tcpArgs'
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
  serve (Host host) port $ \(s, addr) -> do
    race (receiving s logger) (sending s logger)
    --forkFinally (sending s logger) (\e -> handleException e)
    return ()
  where
    handleException e = do
      case e of
        Left e  -> putStrLn $ "\n\n\n" ++ show e ++ "\n\n\n"
        Right _ -> return ()

    sending s logger = forever $ do
      -- PlainMsg
      msg <- genPlainMsg (host, port) topic
      sendBridgeMsg s msg
      logging logger INFO $ printf "[%s:%s] [TCP] Sent     [%s]" host port (show msg)
      threadDelay 3000000

    receiving s logger = forever $ do
      msgs' <- recvBridgeMsgs s 128
      mapM_ processRecvMsg' msgs'

    processRecvMsg' msg' = case msg' of
      Nothing -> return ()
      Just msg ->
        logging logger INFO $ printf "[%s:%s] [TCP] Received [%s]" host port (show msg)
