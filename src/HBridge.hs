{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module HBridge where

import qualified Data.List                       as L
import           Data.Map
import qualified Data.HashMap.Strict             as HM
import           Data.Text
import           Data.Text.Encoding
import qualified Data.ByteString                 as BS
import qualified Data.ByteString.Lazy            as BL
import           Data.Functor.Identity
import           System.IO
import           System.Environment
import           Text.Printf
import           Data.Time
import           Network.Socket
import           Control.Exception
import           Control.Monad
import           Control.Monad.State
import           Control.Monad.Except
import           Control.Monad.Writer
import           Control.Concurrent
import           Control.Concurrent.STM
import           Data.Aeson
import           GHC.Generics

import           Network.MQTT.Topic
import           Network.MQTT.Client
import           Network.MQTT.Types
import           Network.URI


type BrokerName  = String
--type Topic       = Text
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
  deriving (Eq)
instance Show Log where
  show Log{..} = printf "[%7s][%30s] %s" (show logPriority) (show logTime) logContent

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
    log <- atomically $ readTChan loggerChan
    --let s = printf "[%7s][%30s] %s" (show p) (show t) c
    let s = show log
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
             | PubPkt PublishRequest BrokerName
             | ListFuncs
             | InsertSaveMsg String Int FilePath
             | InsertModifyTopic String Int Topic Topic
             | InsertModifyField String Int [Text] Value
             | DeleteFunc Int
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


data ConnectionType = TCPConnection
                    | MQTTConnection
                    deriving (Show, Eq, Generic, FromJSON, ToJSON)

-- | A broker. It contains only static information so
-- it can be formed directly from config file without
-- other I/O actions.
data Broker = Broker
  { brokerName  :: BrokerName     -- ^ Name
  , connectType :: ConnectionType -- ^ Connection type (TCP or MQTT)
  , brokerHost  :: HostName       -- ^ Host address (for TCP)
  , brokerPort  :: ServiceName    -- ^ Port (for TCP)
  , uri         :: URI            -- ^ URI (for MQTT)
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
  } deriving (Show, Generic, FromJSON, ToJSON)

-- | Bridge. It contains both dynamic (active connections to brokers
-- and a broadcast channel) and static (topics) information.
data Bridge = Bridge
  { activeMQTT    :: TVar (Map BrokerName MQTTClient)                -- ^ Active connections to MQTT brokers
  , activeTCP     :: TVar (Map BrokerName Handle)                    -- ^ Active TCP connections
  , rules         :: Map BrokerName (FwdsTopics, SubsTopics)         -- ^ Topic rules
  , mountPoints   :: Map BrokerName Topic                            -- ^ Mount points
  , broadcastChan :: TChan Message                                   -- ^ Broadcast channel
  , functions     :: TVar [(String, Message -> FuncSeries Message)]  -- ^ Processing functions
  }

-- | Environment. It describes the state of system
data Env = Env
  { envBridge :: Bridge
  , envConfig :: Config
  , envLogger :: Logger
  }


-- | Parse a config file. It may fail and throw an exception.
parseConfig :: ExceptT String IO (Maybe Config)
parseConfig = do
  args <- liftIO getArgs
  if L.null args
    then throwError "No argument given."
    else liftIO $ decodeFileStrict (L.head args)

-- | Check if there exists a topic in the list that the given one can match.
existMatch :: Topic -> [Topic] -> Bool
existMatch t ts = elem True [pat `match` t | pat <- ts]

-- | Monad that describes message processing stage
type FuncSeries = ExceptT SomeException (StateT Int (WriterT String IO))


runFuncSeries :: Message -> [Message -> FuncSeries Message] -> IO ((Either SomeException Message, Int), String)
runFuncSeries msg fs = runWriterT $ runStateT (runExceptT $ foldM (\acc f -> f acc) msg fs) 0


-- | Sample message processign functions, for test only
saveMsg :: FilePath -> Message -> FuncSeries Message
saveMsg f msg = do
  liftIO $ catchError (appendFile f (show msg)) (\e -> print e)
  modify (+ 1)
  log <- liftIO $ mkLog INFO $ printf "Saved to %s: [%s].\n" f (show msg)
  tell $ show log
  return msg


modifyField :: Message -> [Text] -> Value -> FuncSeries Message
modifyField msg@(PlainMsg p t) fields v = do
  modify (+ 1)
  let (obj' :: Maybe Object) = decode $ (BL.fromStrict . encodeUtf8) p
  case obj' of
    Just obj -> do
      let newobj = helper obj fields v
          newp   = encode (newobj)
      return $ PlainMsg (decodeUtf8 . BL.toStrict $ newp) t
    Nothing  -> return msg
  where
    helper o [] v = o
    helper o (f:fs) v =
      case HM.lookup f o of
        Just o' -> case o' of
            Object o'' -> if L.null fs
                          then HM.adjust (const v) f o
                          else HM.adjust (const $ Object $ helper o'' fs v) f o
            _          -> if L.null fs
                          then HM.adjust (const v) f o
                          else o
        Nothing -> o


modifyTopic :: Message -> Topic -> Topic -> FuncSeries Message
modifyTopic msg pat t' = do
  modify (+ 1)
  case msg of
    PlainMsg p t -> if pat `match` t
                    then do
      log <- liftIO $ mkLog INFO $ printf "Modified Topic %s to %s.\n" t t'
      tell $ show log
      return $ msg {topic = t'}
                    else return msg

    PubPkt pubReq n -> if pat `match` (decodeUtf8 . BL.toStrict $ t)
                       then do
      log <- liftIO $ mkLog INFO $ printf "Modified Topic %s to %s.\n" (decodeUtf8 . BL.toStrict $ t) t'
      tell $ show log
      return $ PubPkt pubReq{_pubTopic = BL.fromStrict . encodeUtf8 $ t'} n
                       else return msg
        where
          t = _pubTopic pubReq

    _            -> return msg


-- | Compose a mountpoint with a topic
composeMP :: Topic -> Topic -> Topic
composeMP = append

blToText :: BL.ByteString -> Text
blToText = decodeUtf8 . BL.toStrict

textToBL :: Text -> BL.ByteString
textToBL = BL.fromStrict . encodeUtf8

insertToN :: Int -> a -> [a] -> [a]
insertToN n x xs
  | n < 0 || n > L.length xs = xs ++ [x]
  | otherwise = L.take n xs ++ [x] ++ L.drop n xs
