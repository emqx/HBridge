{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module Network.MQTT.Bridge.Types
  ( BrokerName
  , FwdsTopics
  , SubsTopics
  , Message(..)
  , ConnectionType(..)
  , Broker(..)
  , Config(..)
  , Bridge(..)
  , Env(..)
  , MessageFuncs(..)
  , Priority(..)
  , Log(..)
  , Logger(..)
  , MsgCounter(..)
  , MsgNum(..)
  , Metrics(..)
  , FuncSeries

  , parseConfig
  , mkLogger
  , mkLog
  , commitLog
  , logging
  , logProcess
  , runFuncSeries
  ) where

import           Data.Int               (Int64)
import qualified Data.List              as L
import           Data.Map
import           Data.Maybe             (isNothing, fromJust)
import           Data.Text
import           Data.Text.Encoding
import qualified Data.ByteString.Lazy   as BL
import           System.IO
import           System.Metrics.Counter
import           Text.Printf
import           Data.Time
import           Control.Exception
import           Control.Monad
import           Control.Monad.State
import           Control.Monad.Except
import           Control.Monad.Writer
import           Control.Concurrent.STM
import           Data.Aeson
import qualified Data.Yaml              as Y
import           Options.Applicative
import           GHC.Generics
import           Network.MQTT.Topic
import           Network.MQTT.Client
import           Network.MQTT.Types
import           Network.URI

type BrokerName  = String
type FwdsTopics  = [Topic]
type SubsTopics  = [Topic]

-- | Message type. The payload part is in 'Text'
-- type currently but can be modified soon.
data Message = PlainMsg { payload :: Text
                        , topic   :: Topic
                        }
             | PubPkt PublishRequest BrokerName
             | ListFuncs
             | ListFuncsAck [String]
             | InsertSaveMsg { sName  :: String
                             , sIndex :: Int
                             , sFilePath :: FilePath
                             }
             | InsertModifyTopic { mtName  :: String
                                 , mtIndex :: Int
                                 , mtPat   :: Topic
                                 , mtTop   :: Topic
                                 }
             | InsertModifyField { mfName   :: String
                                 , mfIndex  :: Int
                                 , mfFields :: [Text]
                                 , mfValue  :: Value
                                 }
             | DeleteFunc { delIndex :: Int }
             deriving (Show, Generic)

deriving instance FromJSON URIAuth
deriving instance ToJSON URIAuth

deriving instance FromJSON URI
deriving instance ToJSON URI

deriving instance Generic QoS
deriving instance FromJSON QoS
deriving instance ToJSON QoS

instance FromJSON BL.ByteString where
  parseJSON = fmap (BL.fromStrict . encodeUtf8) . parseJSON
instance ToJSON BL.ByteString where
  toJSON bl = String (decodeUtf8 . BL.toStrict $ bl)

deriving instance Generic Property
deriving instance FromJSON Property
deriving instance ToJSON Property

deriving instance Generic PublishRequest
deriving instance FromJSON PublishRequest
deriving instance ToJSON PublishRequest

deriving instance FromJSON Message
deriving instance ToJSON Message


-- | Type of connections. Sometimes TCP and MQTT
-- connections are both called "brokers".
data ConnectionType = TCPConnection
                    | MQTTConnection
                    deriving (Show, Eq, Generic, FromJSON, ToJSON)

-- | A broker. It contains only static information so
-- it can be formed directly from config file without
-- other I/O actions.
data Broker = Broker
  { brokerName  :: BrokerName     -- ^ Name
  , connectType :: ConnectionType -- ^ Connection type (TCP or MQTT)
  , brokerURI   :: String         -- ^ Unparsed URI
  , brokerFwds  :: FwdsTopics     -- ^ Topics it will forward
  , brokerSubs  :: SubsTopics     -- ^ Topics it subscribes
  , brokerMount :: Topic          -- ^ Mountpoint, for changing topic on forwarding
  }
  deriving (Show, Generic, FromJSON, ToJSON)

-- | Config of the bridge, it can be described by
-- brokers it (will) connect to.
data Config = Config
  { brokers     :: [Broker]
  , logToStdErr :: Bool
  , logFile     :: FilePath
  , logLevel    :: Priority
  , msgFuncs    :: [(String, MessageFuncs)]
  } deriving (Show, Generic, FromJSON, ToJSON)

configP :: Parser String
configP = strOption
  (  long "config"
  <> short 'c'
  <> metavar "FILEPATH"
  <> help "File path of configuration file" )

-- | Parse a config file. It may fail and throw an exception.
parseConfig :: ExceptT String IO Config
parseConfig = do
    f <- liftIO $ execParser opts

    conf' <- liftIO $ Y.decodeFileEither f
    conf <- case conf' of
      Left e -> throwError (show e)
      Right conf -> return conf

    let uri' = brokerURI <$> brokers conf
        uri  = parseURI  <$> uri'
    when (Nothing `L.elem` uri)
      (throwError $ printf "Invalid URI: %s."
                           (uri' !! (fromJust $ L.findIndex isNothing uri)) )
    return conf
  where
    opts = info (configP <**> helper)
      (  fullDesc
      <> progDesc "Run an instance of bridge with certain configuration file"
      <> header   "HBridge - a multi-way MQTT/TCP message bridge" )


-- | Counter of messages
data MsgCounter = MsgCounter
  { mqttRecvCounter   :: Counter
  , mqttFwdCounter    :: Counter
  , tcpCtlRecvCounter :: Counter
  , tcpMsgRecvCounter :: Counter
  , tcpMsgFwdCounter  :: Counter
  }

-- | Bridge. It contains both dynamic (active connections to brokers
-- and a broadcast channel) and static (topics) information.
data Bridge = Bridge
  { startTime     :: UTCTime -- ^ System start time
  , activeMQTT    :: TVar (Map BrokerName MQTTClient) -- ^ Active connections to MQTT brokers
  , activeTCP     :: TVar (Map BrokerName Handle) -- ^ Active TCP connections
  , rules         :: Map BrokerName (FwdsTopics, SubsTopics) -- ^ Topic rules
  , mountPoints   :: Map BrokerName Topic -- ^ Mount points
  , broadcastChan :: TChan Message -- ^ Broadcast channel
  , functions     :: TVar [(String, Message -> FuncSeries Message)] -- ^ Processing functions
  , counters      :: MsgCounter  -- ^ Counter of messages
  }

-- | Environment. It describes the state of system.
data Env = Env
  { envBridge :: Bridge
  , envConfig :: Config
  , envLogger :: Logger
  }


-- | Functions for processing messages. New members may be added at any time.
data MessageFuncs = SaveMsg FilePath
                  | ModifyField [Text] Value
                  | ModifyTopic Topic Topic
                  deriving (Show, Generic, FromJSON, ToJSON)

----------------------------------------------------------------------------------------------
-- | Priority of log
data Priority = DEBUG | INFO | WARNING | ERROR
              deriving (Show, Eq, Ord, Generic, FromJSON, ToJSON)

-- | Log. It contains priority, time stamp and log content.
data Log = Log
  { logContent  :: String
  , logPriority :: Priority
  , logTime     :: UTCTime
  }
  deriving (Eq)
instance Show Log where
  show Log{..} = printf "[%7s][%30s] %s"
                        (show logPriority)
                        (show logTime)
                        logContent

-- | A logger is in fact a STM channel.
data Logger = Logger
  { loggerChan     :: TChan Log
  , loggerLevel    :: Priority
  , loggerFile     :: FilePath
  , loggerToStdErr :: Bool
  }

-- | Make a logger from config file.
mkLogger :: Config -> IO Logger
mkLogger Config{..} = do
  ch <- newTChanIO
  return $ Logger ch logLevel logFile logToStdErr

-- | Generate a log item.
mkLog :: Priority -> String -> IO Log
mkLog p s = do
  time <- getCurrentTime
  return (Log s p time)

-- | Pass a log item to logger.
commitLog :: Log -> Logger -> IO ()
commitLog l Logger{..} = when (logPriority l >= loggerLevel)
                              (atomically $ writeTChan loggerChan l)

-- | Generate and then commit a log item.
logging :: Logger -> Priority -> String -> IO ()
logging ch p s = mkLog p s >>= flip commitLog ch

-- | Thread that processes all the log items
logProcess :: Logger -> IO ()
logProcess Logger{..} = do
  time <- getCurrentTime
  (h'  :: Either SomeException Handle) <- try $ openFile loggerFile WriteMode
  (dh' :: Either SomeException Handle) <- case h' of
    Left _ -> try $ openFile (show time ++ ".log") WriteMode
    Right _ -> return h'

  forever $ do
    log <- atomically $ readTChan loggerChan
    let s = show log
    when loggerToStdErr (hPutStrLn stderr s)
    case h' <> dh' of
      Right h -> hPutStrLn h s
      _       -> return ()

----------------------------------------------------------------------------------------------
-- | Num of messages. Read from counters.
data MsgNum = MsgNum
  { mqttRecvNum   :: Int64
  , mqttFwdNum    :: Int64
  , tcpCtlRecvNum :: Int64
  , tcpMsgRecvNum :: Int64
  , tcpMsgFwdNum  :: Int64
  } deriving (Show, Generic, FromJSON, ToJSON)

-- | Metrics information.
data Metrics = Metrics
  { version :: String
  , sysTime :: UTCTime
  , runningTime :: String -- NominalDiffTime
  , sysMsgNum   :: MsgNum
  , runningBrokers :: [Broker]
  } deriving (Show, Generic, FromJSON, ToJSON)

----------------------------------------------------------------------------------------------
-- | Monad that describes message processing stage.
type FuncSeries = ExceptT SomeException (StateT Int (WriterT String IO))

-- | Run the monad to pass a message through a series of functions.
runFuncSeries :: Message
  -> [Message -> FuncSeries Message]
  -> IO ((Either SomeException Message, Int), String)
runFuncSeries msg fs = runWriterT $ runStateT
                                    (runExceptT $ foldM (\acc f -> f acc) msg fs) 0
