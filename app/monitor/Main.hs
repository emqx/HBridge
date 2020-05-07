{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified Data.List as L
import Data.Text
import Text.Printf
import Control.Monad
import Control.Monad.Trans
import Control.Concurrent.Async (race)
import System.IO
import Data.Aeson
import Options.Applicative hiding (completer)
import System.Console.Repline
import Network.Socket
import Network.Run.TCP
import Network.MQTT.Bridge.Types
import Network.MQTT.Bridge.Extra

type Repl a = HaskelineT IO a

data ConnOptions = ConnOptions
  { host :: String
  , port :: String
  }

optionsP :: Parser ConnOptions
optionsP = ConnOptions
  <$> strOption
      (  long "host"
      <> short 'h'
      <> metavar "HOST"
      <> help "Address of monitor server" )
  <*> strOption
      (  long "port"
      <> short 'p'
      <> metavar "PORT"
      <> help "Port of monitor server" )


cmdList :: [String]
cmdList = [ "listFuncs"    , "insertModifyTopic", "insertModifyField"
          , "insertSaveMsg", "deleteFunc"
          ]

-- Eval
cmd :: Handle -> String -> Repl ()
cmd h input
  | "listFuncs" `L.isPrefixOf` input = do
      liftIO $ fwdTCPMessage h ListFuncs
  | "insertModifyTopic" `L.isPrefixOf` input = do
      n    <- liftIO getLine
      i'   <- liftIO getLine
      oldt <- liftIO getLine
      newt <- liftIO getLine
      let (i :: Int) = read i'
      liftIO $ fwdTCPMessage h (InsertModifyTopic n i (pack oldt) (pack newt))
  | "insertModifyField" `L.isPrefixOf` input = do
      n   <- liftIO getLine
      i'  <- liftIO getLine
      fs' <- liftIO getLine
      v'  <- liftIO getLine
      let (i :: Int) = read i'
          (fs :: [Text]) = read fs'
          (v :: Value) = read v'
      liftIO $ fwdTCPMessage h (InsertModifyField n i fs v)
  | "insertSaveMsg" `L.isPrefixOf` input = do
      n  <- liftIO getLine
      i' <- liftIO getLine
      f  <- liftIO getLine
      let (i :: Int) = read i'
      liftIO $ fwdTCPMessage h (InsertSaveMsg n i f)
  | "deleteFunc" `L.isPrefixOf` input = do
      i' <- liftIO getLine
      let (i :: Int) = read i'
      liftIO $ fwdTCPMessage h (DeleteFunc i)
      --liftIO $ putStrLn ("#" ++ show i ++ "#")
  | otherwise = liftIO $ printf "%s: unknown command.\n" input

-- Tab Completion: return a completion for partial words entered
completer :: Monad m => WordCompleter m
completer n = do
  return $ L.filter (L.isPrefixOf n) cmdList

replOpts :: [(String, [String] -> Repl ())]
replOpts = []

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome!"

main :: IO ()
main = do
  ConnOptions host port <- execParser opts
  runTCPServer (Just host) port $ \s -> do
    h <- socketToHandle s ReadWriteMode
    race (receiving h) (sending h)
    return ()
  where
    opts = info (optionsP <**> helper)
      (  fullDesc
      <> progDesc "Run a service for monitoring and controlling HBridge" )

    sending h = evalRepl (pure ">>> ") (cmd h) replOpts Nothing (Word completer) ini

    receiving h = forever $ do
      msg <- recvTCPMessage h
      case msg of
        Just (ListFuncsAck items) -> putStrLn (L.concat items)
        _ -> return ()
      --when (not (BS.null msg')) (print msg')
