{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}

module Main where

import qualified Data.List                 as L
import           Data.Text
import qualified Data.Map                  as Map
import           Data.Maybe                (fromJust)
import           Text.Printf
import           System.IO
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Except
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Concurrent.Async
import           Control.Exception
import           System.Remote.Monitoring
import           Network.MQTT.Types
import           Network.MQTT.Client
import           Types
import           Extra
import           Environment

import           Data.Aeson                (encode)
import           Yesod                     hiding (Env)
import           Yesod.Core
import           Network.HTTP.Types
import           Network.Wai


{-
data App = App

instance Yesod App

mkYesod "App" [parseRoutes|
/#Env FuncsR GET
|]

getFuncsR :: Env -> HandlerFor App TypedContent
getFuncsR Env{..} = respondSource "application/json" $ do
  funcs <- liftIO . readTVarIO $ functions envBridge
  --return (fst <$> funcs)
  sendChunkLBS $ encode (fst <$> funcs)
-}

{- Yesod Example

data Person = Person { name :: String, age :: Int }
data Post = Post { pname :: Text
                 , content :: Text
                 , likes :: Int
                 } deriving (Show, Generic, FromJSON, ToJSON)
data App = App

mkYesod "App" [parseRoutes|
/funcs HomeR GET
/func  PostR POST
|]

instance Yesod App

getHomeR :: Handler TypedContent
getHomeR = selectRep $ do
    provideRep $ return
        [shamlet|
            <p>Hello, my name is #{name} and I am #{age} years old.
        |]

    provideRep $ return $ object
        [ "name" .= name
        , "age" .= age
        ]
  where
    name = "Michael" :: Text
    age = 28 :: Int

postPostR :: Handler Value
postPostR = do
  post <- requireCheckJsonBody :: Handler Post
  sendStatusJSON created201 (object ["id" .= Number 123])

main :: IO ()
main = warp 3000 App
-}


{- Yesod Example with IO

data App = App
instance Yesod App

mkYesod "App" [parseRoutes|
/ HomeR GET
|]

getHomeR :: HandlerFor App TypedContent
getHomeR = respondSource "text/plain" $ do
    sendChunkBS "Starting streaming response.\n"
    sendChunkText "Performing some I/O.\n"
    sendFlush
    -- pretend we're performing some I/O
    liftIO $ threadDelay 1000000
    sendChunkBS "I/O performed, here are some results.\n"
    forM_ [1..50 :: Int] $ \i -> do
        sendChunk $ fromByteString "Got the value: " <>
                    fromShow i <>
                    fromByteString "\n"

-}






-----------

main :: IO ()
main = do
  conf' <- runExceptT parseConfig
  case conf' of
    Left e -> error e
    Right conf -> do
      forkServer "localhost" 22333
      env' <- runReaderT newEnv1 conf
      env@(Env bridge _ logger) <- execStateT newEnv2 env'
      forkFinally (logProcess logger) (\_ -> putStrLn "[Warning] Log service failed.")

      initMCs <- readTVarIO (activeMQTT bridge)
      logging logger INFO $ "Connected to brokers:\n" ++ L.intercalate "\n" (Map.keys initMCs)
      initTCPs <- readTVarIO (activeTCP bridge)
      logging logger INFO $ "Active TCP connections: \n" ++ L.intercalate "\n" (Map.keys initTCPs)

      mapM_ (\tup -> processMQTT tup env) (Map.toList initMCs)
      mapM_ (\tup -> processTCP  tup env) (Map.toList initTCPs)

      forever getChar
      return ()


-- | Create a thread (in fact two, receiving and forwarding)
-- for certain broker.
processMQTT :: (BrokerName, MQTTClient)
        -> Env
        -> IO ()
processMQTT tup@(n, mc) env@(Env Bridge{..} _ logger) = do
    forkFinally (runMQTT tup env) handleException
    return ()
  where
    handleException e = case e of
        Left e -> do
          atomically $ modifyTVar activeMQTT (Map.delete n)
          logging logger WARNING $ printf "MQTT broker %s disconnected (%s)\n" n (show e)
        _     -> return ()

-- | Describes how the bridge receives and forwards messages
-- from/to a certain broker.
-- It is bridge-scoped and does not care about specific behaviours
-- of brokers, which is provided by fwdMessage' and 'recvMessge'.
runMQTT :: (BrokerName, MQTTClient)
    -> Env
    -> IO ()
runMQTT (n, mc) (Env Bridge{..} _ logger) = do
    ch <- atomically $ dupTChan broadcastChan
    forkFinally (forwarding ch logger) (\e -> return ())
    return ()
  where
    (fwds, subs) = fromJust (Map.lookup n rules)
    mp = fromJust (Map.lookup n mountPoints)

    forwarding ch logger = forever $ do
      msg <- atomically (readTChan ch)
      case msg of
        PubPkt p n' -> when ((blToText (_pubTopic p)) `existMatch` subs && n /= n') $ do
          logging logger INFO $ printf "Forwarded   [%s] from %s to %s." (show msg) n' n
          pubAliased mc (blToText $ _pubTopic p) (_pubBody p) (_pubRetain p) (_pubQoS p) (_pubProps p)
        _            -> return ()


-- | Create a thread (in fact two, receiving and forwarding)
-- for certain TCP connection.
processTCP :: (BrokerName, Handle)
        -> Env
        -> IO ()
processTCP tup@(n, h) env@(Env Bridge{..} _ logger) = do
    forkFinally (runTCP tup env) (\e -> handleException e)
    return ()
  where
    handleException e = case e of
        Left e -> do
          atomically $ modifyTVar activeTCP (Map.delete n)
          logging logger WARNING $ printf "[TCP]  Broker %s disconnected (%s)\n" n (show e)
          hClose h
        _     -> return ()

-- | Describes how the bridge receives and forwards messages
-- from/to a certain TCP connection.
-- It is bridge-scoped and does not care about specific behaviours
-- of servers, which is provided by fwdTCPMessage' and 'recvTCPMessge'.
runTCP :: (BrokerName, Handle)
    -> Env
    -> IO ()
runTCP (n, h) (Env Bridge{..} _ logger) = do
    ch <- atomically $ dupTChan broadcastChan
    race (receiving ch logger) (forwarding ch logger)
    return ()
  where
    (fwds, subs) = fromJust (Map.lookup n rules)
    mp = fromJust (Map.lookup n mountPoints)

    receiving ch logger = forever $ do
      msg <- recvTCPMessage h
      logging logger INFO $ printf "[TCP]  Received    [%s]." (show msg)
      case msg of

        Just (PlainMsg _ t) -> do
          when (t `existMatch` fwds) $ do
            funcs' <- readTVarIO functions
            let funcs = snd <$> funcs'
            ((msg', s), l) <- runFuncSeries (fromJust msg) funcs
            case msg' of
              Left e -> logging logger WARNING "[TCP]  Message transformation failed."
              Right msg'' -> atomically $ do
                writeTChan broadcastChan msg''
                readTChan ch
                return ()

        Just ListFuncs -> do
          funcs <- readTVarIO functions
          let (items :: [String]) = L.map (\(i,s) -> show i ++ " " ++ s ++ "\n")
                                          ([0..] `L.zip` (fst <$> funcs))
          logging logger INFO $ "[TCP]  Functions:\n" ++ L.concat items
          fwdTCPMessage h (ListFuncsAck items)

        Just (InsertSaveMsg n i f) -> do
          atomically $ modifyTVar functions (insertToN i (n, saveMsg f))
          logging logger INFO $ printf "[TCP]  Function %s : save message to file %s." n f

        Just (InsertModifyTopic n i t t') -> do
          atomically $ modifyTVar functions (insertToN i (n, modifyTopic t t'))
          logging logger INFO $ printf "[TCP]  Function %s : modify topic %s to %s." n t t'

        Just (InsertModifyField n i fs v) -> do
          atomically $ modifyTVar functions (insertToN i (n, modifyField fs v))
          logging logger INFO $ printf "[TCP]  Function %s : modify field %s to %s." n (show fs) (show v)

        Just (DeleteFunc i) -> do
          atomically $ modifyTVar functions (deleteAtN i)
          logging logger INFO $ printf "[TCP]  Function : delete the %d th function." i

        _                   -> return ()

    forwarding ch logger = forever $ do
      msg <- atomically (readTChan ch)
      case msg of
        PlainMsg _ t -> when (t `existMatch` subs) $ do
          let msg' = msg{payload = mp `composeMP` t}
          fwdTCPMessage h msg'
          logging logger INFO $ printf "[TCP]  Forwarded   [%s]." (show msg')
        _            -> return ()
