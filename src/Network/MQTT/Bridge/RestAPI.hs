{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeOperators         #-}

module Network.MQTT.Bridge.RestAPI
  ( apiApp
  ) where

import           Control.Concurrent.STM    (readTVarIO)
import           Control.Monad.IO.Class    (liftIO)
import qualified Data.List                 as L
import qualified Data.Map                  as Map
import           Data.Proxy                (Proxy(..))
import           Data.Time                 ( getCurrentTime, diffUTCTime
                                           , formatTime, defaultTimeLocale)
import           Network.MQTT.Bridge.Types ( Env(..), Metrics(..), MsgCounter(..)
                                           , MsgNum(..), Broker(..), Bridge(..)
                                           , Config(..))
import           Prelude                   hiding (read)
import           Servant.API               (Get, JSON, (:>), (:<|>)(..))
import qualified Servant.Server            as Server
import           System.Metrics.Counter    (read)


type UserAPI = "funcs"   :> Get '[JSON] [String]
--          :<|> "funcs"   :> ReqBody '[JSON] Message :> Post '[JSON] String
          :<|> "metrics" :> Get '[JSON] Metrics

httpServer :: Env -> Server.Server UserAPI
httpServer Env{..} = getFuncs
--                :<|> postFuncs
                :<|> getMetrics
  where
    getFuncs :: Server.Handler [String]
    getFuncs = do
      funcs <- liftIO . readTVarIO $ functions envBridge
      return ["[ " ++ n ++ " ]"| (n,_) <- funcs]

    getMetrics :: Server.Handler Metrics
    getMetrics = do
        sysT  <- liftIO getCurrentTime
        mqBs  <- liftIO . readTVarIO $ activeMQTT envBridge
        tcpBs <- liftIO . readTVarIO $ activeTCP envBridge
        is    <- liftIO $ mapM read [mqrc,mqfc,tctlc,trc,tfc]

        let runT = formatTime defaultTimeLocale "%D days, %H hours, %M minutes, %S seconds"
                                                (diffUTCTime sysT startT)
            mqN  = Map.keys mqBs
            tcpN = Map.keys tcpBs
            mqs  = L.concat $ (\n -> L.filter (\b -> brokerName b == n) (brokers envConfig)) <$> mqN
            tcps = L.concat $ (\n -> L.filter (\b -> brokerName b == n) (brokers envConfig)) <$> tcpN
            [mqri,mqfi,tctli,tri,tfi]   = is
        return $ Metrics ver sysT runT (MsgNum mqri mqfi tctli tri tfi) (mqs ++ tcps)
      where
        ver    = "0.0.1"
        startT = startTime envBridge
        MsgCounter mqrc mqfc tctlc trc tfc = counters envBridge

userAPI :: Proxy UserAPI
userAPI = Proxy

apiApp :: Env -> Server.Application
apiApp env = Server.serve userAPI (httpServer env)
