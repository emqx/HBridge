{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Concurrent.Async  (race)
import           Control.Monad
import           Control.Monad.Trans
import           Data.Aeson
import qualified Data.List                 as L
import           Data.Text
import           Network.MQTT.Bridge.Extra
import           Network.MQTT.Bridge.Types
import           Network.Run.TCP
import           Network.Socket
import           Options.Applicative       hiding (completer)
import           System.Console.Repline
import           System.IO
import           Text.Printf

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
cmdList = [ "listFuncs", "deleteFunc"]

-- Eval
cmd :: Handle -> String -> Repl ()
cmd h input
  | "listFuncs" `L.isPrefixOf` input = do
      liftIO $ fwdTCPMessage h ListFuncs
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
        _                         -> return ()
      --when (not (BS.null msg')) (print msg')
