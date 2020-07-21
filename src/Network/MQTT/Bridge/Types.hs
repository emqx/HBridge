{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}

module Network.MQTT.Bridge.Types
  ( BrokerName
  , FwdsTopics
  , SubsTopics
  , Message(..)
  , ConnectionType(..)
  , Broker(..)
  , Config(..)
  , LoggerSetting(..)
  , Bridge(..)
  , Env(..)
  , App
  , MsgCounter(..)
  , MsgNum(..)
  , Metrics(..)
  , FuncSeries

  , runApp
  , parseConfig
  , runFuncSeries
  ) where

import qualified Colog
import           Control.Applicative         ((<|>))
import           Control.Concurrent.STM      (TChan, TVar, atomically,
                                              newTChanIO, readTChan, writeTChan)
import           Control.Exception           (SomeException, try)
import           Control.Monad               (foldM, forever, when)
import           Control.Monad.Base          (MonadBase)
import           Control.Monad.Except        (ExceptT, runExceptT, throwError)
import           Control.Monad.IO.Class      (MonadIO, liftIO)
import           Control.Monad.Reader        (MonadReader, ReaderT, runReaderT)
import           Control.Monad.State         (StateT, runStateT)
import           Control.Monad.Trans.Control (MonadBaseControl (..))
import           Control.Monad.Writer        (WriterT, runWriterT)
import           Data.Aeson                  (FromJSON (..), ToJSON, Value (..),
                                              (.:))
import qualified Data.Aeson                  as Aeson
import qualified Data.Aeson.Types            as Aeson
import qualified Data.ByteString.Lazy        as BL
import           Data.Int                    (Int64)
import qualified Data.List                   as L
import qualified Data.Map                    as Map
import           Data.Maybe                  (fromJust, isNothing)
import           Data.Text                   (Text)
import           Data.Text.Encoding          (decodeUtf8, encodeUtf8)
import           Data.Time                   (UTCTime, getCurrentTime)
import           Data.UUID                   (UUID)
import qualified Data.Yaml                   as Yaml
import           GHC.Generics                (Generic)
import           Network.MQTT.Client         (MQTTClient, Topic)
import           Network.MQTT.Types          (Property (..),
                                              PublishRequest (..), QoS (..))
import           Network.Simple.TCP          (Socket)
import           Network.URI                 (URI, URIAuth, parseURI)
import           Options.Applicative         (Parser, execParser, fullDesc,
                                              header, help, helper, info, long,
                                              metavar, progDesc, short,
                                              strOption, (<**>))
import           System.IO                   (Handle, IOMode (WriteMode),
                                              hPutStrLn, openFile, stderr)
import           System.Metrics.Counter      (Counter)
import           Text.Printf                 (printf)

type BrokerName  = String
type FwdsTopics  = [Topic]
type SubsTopics  = [Topic]

-- | Message type. The payload part is in 'ByteString'
-- type currently but can be modified soon.
data Message = PlainMsg
    { payload :: BL.ByteString
    , topic   :: Topic
    }
    | PubPkt PublishRequest BrokerName
    deriving (Show, Generic)

instance Ord Value where
  (Object _)  <= (Object _)  = True
  (Array _)   <= (Array _)   = True
  (String s1) <= (String s2) = s1 <= s2
  (Number n1) <= (Number n2) = n1 <= n2
  (Bool b1)   <= (Bool b2)   = b1 <= b2

instance Semigroup Value where
  v1 <> v2 = v1

deriving instance Read QoS

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
    { brokerName  :: BrokerName -- ^ Name
    -- ^ Connection type (TCP or MQTT)
    , connectType :: ConnectionType
    -- ^ Unparsed URI
    , brokerURI   :: String
    -- ^ Topics it will forward
    , brokerFwds  :: FwdsTopics
    -- ^ Topics it subscribes
    , brokerSubs  :: SubsTopics
    -- ^ Mountpoint, for changing topic on forwarding
    , brokerMount :: Topic
    }
    deriving (Show, Generic, FromJSON, ToJSON)

-- | Config of the bridge, it can be described by
-- brokers it (will) connect to.
data Config m = Config
    { brokers       :: [Broker]
    , crossForward  :: Bool
    , sqlFiles      :: [FilePath]
    , loggerSetting :: LoggerSetting m
    }
    deriving (Generic, FromJSON)

configP :: Parser String
configP = strOption
  (  long "config"
  <> short 'c'
  <> metavar "FILEPATH"
  <> help "File path of configuration file" )

-- | Parse a config file. It may fail and throw an exception.
parseConfig :: MonadIO m => ExceptT String IO (Config m)
parseConfig = do
    f      <- liftIO $ execParser opts
    e_conf <- liftIO $ Yaml.decodeFileEither f
    conf   <- case e_conf of
      Left e     -> throwError (show e)
      Right conf -> return conf
    let uriStrs = brokerURI <$> brokers conf
        m_uris  = parseURI  <$> uriStrs
    when (Nothing `L.elem` m_uris)
      (throwError $ printf "Invalid URI: %s."
        (uriStrs !! fromJust (L.findIndex isNothing m_uris)))
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
    -- ^ Active connections to MQTT brokers
    , activeMQTT    :: TVar (Map.Map BrokerName MQTTClient)
    -- ^ Active TCP connections
    , activeTCP     :: TVar (Map.Map BrokerName (Socket, UUID))
    -- ^ Topic rules
    , rules         :: Map.Map BrokerName (FwdsTopics, SubsTopics)
    -- ^ Mount points
    , mountPoints   :: Map.Map BrokerName Topic
    -- ^ Broadcast channel
    , broadcastChan :: TChan Message
    -- ^ Processing functions
    , functions     :: TVar [(FilePath, Message -> FuncSeries Message)]
    -- ^ Counter of messages
    , counters      :: MsgCounter
    }

-- | Environment. It describes the state of system.
data Env m = Env
    { envBridge :: Bridge
    , envConfig :: Config m
    }

newtype App a = App { unApp :: ReaderT (Env App) IO a }
  deriving newtype ( Functor, Applicative, Monad
                   , MonadIO, MonadReader (Env App), MonadBase IO
                   )

instance MonadBaseControl IO App where
  type StM App a = a
  liftBaseWith f = App $ liftBaseWith $ \x -> f (x . unApp)
  restoreM = App . restoreM

runApp :: Env App -> App a -> IO a
runApp env app = runReaderT (unApp app) env

----------------------------------------------------------------------------------------------
newtype LoggerSetting m =
  LoggerSetting { logAction :: Colog.LogAction m Colog.Message }

instance MonadIO m => FromJSON (LoggerSetting m) where
  parseJSON v = Aeson.withObject "logger"        customMode v
            <|> Aeson.withText   "simple-logger" simpleMode v

instance Colog.HasLog (Env m) Colog.Message m where
  getLogAction :: Env m -> Colog.LogAction m Colog.Message
  getLogAction = logAction . loggerSetting . envConfig
  {-# INLINE getLogAction #-}

  setLogAction :: Colog.LogAction m Colog.Message -> Env m -> Env m
  setLogAction new env =
    let settings = envConfig env
     in env {envConfig = settings {loggerSetting = LoggerSetting new}}
  {-# INLINE setLogAction #-}

simpleMode :: MonadIO m => Text -> Aeson.Parser (LoggerSetting m)
simpleMode fmt = LoggerSetting <$> formatter fmt

customMode :: MonadIO m => Aeson.Object -> Aeson.Parser (LoggerSetting m)
customMode obj = do
  fmt <- obj .: "formatter" :: Aeson.Parser Text
  lvl <- obj .: "level"     :: Aeson.Parser Text
  action <- formatter fmt
  LoggerSetting <$> level action lvl

formatter :: MonadIO m => Text -> Aeson.Parser (Colog.LogAction m Colog.Message)
formatter "rich"   = return Colog.richMessageAction
formatter "simple" = return Colog.simpleMessageAction
formatter _        = fail "Invalid logger formatter"

level :: MonadIO m
      => Colog.LogAction m Colog.Message
      -> Text
      -> Aeson.Parser (Colog.LogAction m Colog.Message)
level action = \case
  "debug"   -> return $ Colog.filterBySeverity Colog.D sev action
  "info"    -> return $ Colog.filterBySeverity Colog.I sev action
  "warning" -> return $ Colog.filterBySeverity Colog.W sev action
  "error"   -> return $ Colog.filterBySeverity Colog.E sev action
  _         -> fail "Invalid logger level"
  where sev Colog.Msg{..} = msgSeverity

----------------------------------------------------------------------------------------------
-- | Num of messages. Read from counters.
data MsgNum = MsgNum
    { mqttRecvNum   :: Int64
    , mqttFwdNum    :: Int64
    , tcpCtlRecvNum :: Int64
    , tcpMsgRecvNum :: Int64
    , tcpMsgFwdNum  :: Int64
    }
    deriving (Show, Generic, FromJSON, ToJSON)

-- | Metrics information.
data Metrics = Metrics
    { version        :: String
    , sysTime        :: UTCTime
    -- NominalDiffTime
    , runningTime    :: String -- NominalDiffTime
    , sysMsgNum      :: MsgNum
    , runningBrokers :: [Broker]
    }
    deriving (Show, Generic, FromJSON, ToJSON)

----------------------------------------------------------------------------------------------
-- | Monad that describes message processing stage.
type FuncSeries = ExceptT SomeException (StateT Int (WriterT String IO))

-- | Run the monad to pass a message through a series of functions.
runFuncSeries :: Message
  -> [Message -> FuncSeries Message]
  -> IO ((Either SomeException Message, Int), String)
runFuncSeries msg fs = runWriterT $ runStateT
                                    (runExceptT $ foldM (\acc f -> f acc) msg fs) 0
