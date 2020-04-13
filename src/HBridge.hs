{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module HBridge where

import           Data.Map
import           Data.Text
import           System.IO
import           Network.Socket
import           Control.Exception
import           Control.Concurrent
import           Control.Concurrent.STM
import           Data.Aeson
import           GHC.Generics
import           Network.MQTT.Topic (match)

type BrokerName  = String
type Topic       = String
type FwdsTopics  = [Topic]
type SubsTopics  = [Topic]

-- | Message type. The payload part is in 'Text'
-- type currently but can be modified soon.
data Message = Message
  { msgPayload :: Text
  , msgTopic   :: Topic
  }
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
  { brokers :: [Broker]
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
