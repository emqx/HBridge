{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module HBridge where

import           Data.Map
import           Data.Text
import qualified Data.ByteString                 as BS
import qualified Data.ByteString.Lazy            as BSL
import           System.IO
import           Text.Printf
import           Data.Time
import           Network.Socket
import           Control.Exception
import           Control.Monad
import           Control.Concurrent
import           Control.Concurrent.STM
import           Data.Aeson
import           GHC.Generics
import           Network.MQTT.Topic (match)

type BrokerName  = String
type Topic       = String
type FwdsTopics  = [Topic]
type SubsTopics  = [Topic]

-- | Priority of log
data Priority = DEBUG | INFO | WARNING | ERROR
              deriving (Show, Eq, Ord, Generic, FromJSON, ToJSON)

data Log = Log
  { logContent  :: String
  , logPriority :: Priority
  , logTime     :: UTCTime
  }
  deriving (Show, Eq)

-- | A logger is in fact a STM channel
data Logger = Logger
  { loggerChan     :: TChan Log
  , loggerLevel    :: Priority
  , loggerFile     :: FilePath
  , loggerToStdErr :: Bool
  }

-- Make a logger from config file
mkLogger :: Config -> IO Logger
mkLogger Config{..} = do
  ch <- newTChanIO
  return $ Logger ch logLevel logFile logToStdErr

-- | Generate a log item
mkLog :: Priority -> String -> IO Log
mkLog p s = do
  time <- getCurrentTime
  return (Log s p time)

-- | Pass a log item to logger
commitLog :: Log -> Logger -> IO ()
commitLog l Logger{..} = when (logPriority l >= loggerLevel)
                              (atomically $ writeTChan loggerChan l)

-- | Generate and then commit a log item
logging :: Logger -> Priority -> String -> IO ()
logging ch p s = mkLog p s >>= flip commitLog ch


-- | Thread that processes all the log items
logProcess :: Logger -> IO ()
logProcess Logger{..} = do
  time <- getCurrentTime
  (h'  :: Either SomeException Handle) <- try $ openFile loggerFile WriteMode
  (dh' :: Either SomeException Handle) <- try $ openFile (show time ++ ".log") WriteMode
  forever $ do
    (Log c p t) <- atomically $ readTChan loggerChan
    let s = printf "[%30s][%7s] %s" (show t) (show p) c
    when loggerToStdErr (hPutStrLn stderr s)
    case h' of
      Right h -> hPutStrLn h s
      _       -> case dh' of
                   Right dh -> hPutStrLn dh s
                   _        -> return ()

-- | Message type. The payload part is in 'Text'
-- type currently but can be modified soon.
data Message = PlainMsg { payload :: Text
                        , topic   :: Topic
                        }
             | ForTest
             deriving (Show, Generic, FromJSON, ToJSON)

-- | A broker. It contains only static information so
-- it can be formed directly from config file without
-- other I/O actions.
data Broker = Broker
  { brokerName :: BrokerName  -- ^ Name
  , brokerHost :: HostName    -- ^ Host address
  , brokerPort :: ServiceName -- ^ Port
  , brokerFwds :: FwdsTopics  -- ^ Topics it will forward
  , brokerSubs :: SubsTopics  -- ^ Topics it subscribes
  }
  deriving (Show, Generic, FromJSON, ToJSON)

-- | Config of the bridge, it can be described by
-- brokers it (will) connect to.
data Config = Config
  { brokers     :: [Broker]
  , logToStdErr :: Bool
  , logFile     :: FilePath
  , logLevel    :: Priority
  } deriving (Show, Generic, FromJSON, ToJSON)

-- | Bridge. It contains both dynamic (active connections to brokers
-- and a broadcast channel) and static (topics) information.
data Bridge = Bridge
  { activeBrokers :: TVar (Map BrokerName Handle)            -- Active connections to brokers
  , rules         :: Map BrokerName (FwdsTopics, SubsTopics) -- Topic rules
  , broadcastChan :: TChan Message                           -- Broadcast channel
  }

-- | Parse a config file. It may fail and throw an exception.
parseConfig :: FilePath -> IO (Either SomeException (Maybe Config))
parseConfig = try . decodeFileStrict

-- | Check if there exists a topic in the list that the given one can match.
existMatch :: Topic -> [Topic] -> Bool
existMatch t ts = elem True [(pack pat) `match` (pack t) | pat <- ts]
