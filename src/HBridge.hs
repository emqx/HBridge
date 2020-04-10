{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}

module HBridge where

import Data.Map  as Map
import Data.Text
import System.IO
import Network.Socket
import Control.Concurrent
import Control.Concurrent.STM
import Data.Aeson
import GHC.Generics

type BrokerName  = String
type Topic       = String
type FwdsTopics  = [Topic]
type SubsTopics  = [Topic]

data Message = Message
  { msgContent :: Text
  , msgTopic   :: Topic
  }
  deriving (Show, Generic, FromJSON, ToJSON)

data Broker = Broker
  { brokerName :: String
  , brokerHost :: HostName
  , brokerPort :: ServiceName
  , brokerFwds :: FwdsTopics
  , brokerSubs :: SubsTopics
  }
  deriving (Show, Generic, FromJSON, ToJSON)

data Config = Config
  { brokers :: [Broker]
  } deriving (Show, Generic, FromJSON, ToJSON)

data Bridge = Bridge
  { activeBrokers :: TVar (Map BrokerName Handle)
  , rules         :: Map BrokerName (FwdsTopics, SubsTopics)
  , broadcastChan :: TChan Message
  }

-- EXCEPTIONS!
parseConfig :: FilePath -> IO (Maybe Config)
parseConfig f = decodeFileStrict f
