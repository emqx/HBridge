{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards     #-}

module Extra
  ( existMatch
  , parseMsgFuncs
  , saveMsg
  , modifyField
  , modifyTopic
  , composeMP
  , blToText
  , textToBL
  , insertToN
  ) where

import           Types

import           Data.Text
import           Data.Text.Encoding
import           Text.Printf
import qualified Data.List            as L
import qualified Data.HashMap.Strict  as HM
import qualified Data.ByteString.Lazy as BL
import           Control.Monad.State
import           Control.Monad.Writer
import           Control.Monad.Except
import           GHC.Generics
import           Data.Aeson
import           Network.MQTT.Client
import           Network.MQTT.Types
import           Network.MQTT.Topic



-- | Check if there exists a topic in the list that the given one can match.
existMatch :: Topic -> [Topic] -> Bool
existMatch t ts =  True `elem` [pat `match` t | pat <- ts]

parseMsgFuncs :: MessageFuncs -> Message -> FuncSeries Message
parseMsgFuncs (SaveMsg fp) = saveMsg fp
parseMsgFuncs (ModifyField fs v) = modifyField fs v
parseMsgFuncs (ModifyTopic pat t') = modifyTopic pat t'


-- | Save a message to file. For any type of message.
saveMsg :: FilePath -> Message -> FuncSeries Message
saveMsg f msg = do
  liftIO $ catchError (appendFile f (show msg ++ "\n")) (\e -> print e)
  modify (+ 1)
  log <- liftIO $ mkLog INFO $ printf "Saved to %s: [%s].\n" f (show msg)
  return msg

-- | Modify certain field of payload. For PlainMsg and PubPkt types.
modifyField :: [Text] -> Value -> Message -> FuncSeries Message
modifyField fields v msg = case msg of
  PlainMsg p t -> do
    modify (+ 1)
    let (obj' :: Maybe Object) = decode (textToBL p)
    case obj' of
      Just obj -> do
        let newobj = helper obj fields v
            newp   = encode newobj
        return $ PlainMsg (decodeUtf8 . BL.toStrict $ newp) t
      Nothing  -> return msg

  PubPkt pubReq@PublishRequest{..} n -> do
    modify (+ 1)
    let (obj' :: Maybe Object) = decode _pubBody
    case obj' of
      Just obj -> do
        let newobj = helper obj fields v
            newbody = encode newobj
        return $ PubPkt pubReq{_pubBody = newbody} n
      Nothing  -> return msg

  _ -> return msg

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

-- | Modify topic of message to another one. For PlainMsg and PubPkt types.
modifyTopic :: Topic -> Topic -> Message -> FuncSeries Message
modifyTopic pat t' msg = do
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
