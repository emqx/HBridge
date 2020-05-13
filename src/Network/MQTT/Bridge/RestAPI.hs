{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}

module Network.MQTT.Bridge.RestAPI
  ( apiApp
  ) where

import           Prelude                   hiding (read)
import qualified Data.List                 as L
import qualified Data.Map                  as Map
import           Data.Time
import           Control.Concurrent.STM
import           Control.Monad.IO.Class
import           System.Metrics.Counter
import           Network.Wai
import           Servant.API
import           Servant.Server
import           Data.Proxy
import           Network.MQTT.Bridge.Types
import           Network.MQTT.Bridge.Extra


type UserAPI = "funcs"   :> Get '[JSON] [String]
          :<|> "funcs"   :> ReqBody '[JSON] Message :> Post '[JSON] String
          :<|> "metrics" :> Get '[JSON] Metrics

httpServer :: Env -> Server UserAPI
httpServer Env{..} = getFuncs
                :<|> postFuncs
                :<|> getMetrics
  where
    getFuncs :: Handler [String]
    getFuncs = do
      funcs <- liftIO . readTVarIO $ functions envBridge
      return ["[ " ++ n ++ " ]: " ++ show f'| (n,f',f) <- funcs]

    postFuncs :: Message -> Handler String
    postFuncs msg = do
      case msg of
        InsertModifyField n i fs v -> do
          let f' = ModifyField fs v
          liftIO . atomically $ modifyTVar (functions envBridge) (insertToN i (n,f',modifyField fs v))
          return "OK"
        InsertModifyTopic n i pat t -> do
          let f' = ModifyTopic pat t
          liftIO . atomically $ modifyTVar (functions envBridge) (insertToN i (n,f',modifyTopic pat t))
          return "OK"
        DeleteFunc i -> do
          liftIO . atomically $ modifyTVar (functions envBridge) (deleteAtN i)
          return "OK"
        _ -> return "Unrecognized function"

    getMetrics :: Handler Metrics
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

apiApp :: Env -> Application
apiApp env = serve userAPI (httpServer env)
