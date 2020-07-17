{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.MQTT.Bridge.Extra
  ( parseSQLFile
  , existMatch
  , composeMP
  , blToText
  , textToBL
  , insertToN
  , deleteAtN
  , fwdTCPMessage
  , recvTCPMessages
  , recvBridgeMsgs
  , sendBridgeMsg
  ) where

import           Control.Exception
import           Data.Aeson
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString                  as BS
import qualified Data.ByteString.Lazy             as BL
import qualified Data.HashMap.Strict              as HM
import qualified Data.List                        as L
import qualified Data.Map                         as Map
import           Data.Text
import           Data.Text.Encoding
import           Data.UUID
import qualified Data.UUID                        as UUID
import qualified Data.UUID.V4                     as UUID
import qualified Data.Vector                      as V
import qualified Network.HESP                     as HESP
import qualified Network.HESP.Commands            as HESP
import           Network.MQTT.Bridge.SQL.AbsESQL
import           Network.MQTT.Bridge.SQL.ErrM
import           Network.MQTT.Bridge.SQL.ParESQL
import           Network.MQTT.Bridge.SQL.SkelESQL
import           Network.MQTT.Bridge.Types
import           Network.MQTT.Client
import           Network.MQTT.Topic
import           Network.MQTT.Types
import           Network.Simple.TCP               as TCP


lookup' :: ParsedProg -> Text -> Object -> Maybe Value
lookup' ParsedProg{..} n o = case a1 of
    Nothing -> a2
    _ -> case a2 of
           Nothing -> a1
           _       -> a2
  where
    a1 = HM.lookup n o
    n' = (Map.!) sel (unpack n)
    a2 = case n' of
           Nothing  -> Nothing
           Just n'' -> HM.lookup (pack n'') o

processCond :: ParsedProg -> Message -> Object -> Cond -> Bool
processCond pp@ParsedProg{..} (PubPkt PublishRequest{..} _) o (ECondEQ i v)
  | l == "topic" = case r of
                     String t -> blToText _pubTopic == t
                     _        -> False
  | l == "retain" = case r of
                      Bool b -> _pubRetain == b
                      _      -> False
  | l == "QoS" = case r of
                   String t -> _pubQoS == read (unpack t)
                   _        -> False
  | otherwise = case lookup' pp l o of
                  Nothing   -> False
                  (Just v') -> r == v'
  where
    l = pack . transIdent $ i
    r = transVar v
processCond pp@ParsedProg{..} (PubPkt PublishRequest{..} _) o (ECondNE i v)
  | l == "topic" = case r of
                     String t -> blToText _pubTopic /= t
                     _        -> False
  | l == "retain" = case r of
                      Bool b -> _pubRetain /= b
                      _      -> False
  | l == "QoS" = case r of
                   String t -> _pubQoS /= read (unpack t)
                   _        -> False
  | otherwise = case lookup' pp l o of
                  Nothing   -> False
                  (Just v') -> r /= v'
  where
    l = pack . transIdent $ i
    r = transVar v
processCond    ParsedProg{..} (PubPkt PublishRequest{..} _) o (ECondMt i r)
  | l == "topic" = pack r `match` blToText _pubTopic
  | otherwise = False
  where
    l = pack . transIdent $ i
processCond pp@ParsedProg{..} (PubPkt PublishRequest{..} _) o (ECondGT i v)
  | l == "topic" = False
  | l == "retain" = False
  | l == "QoS" = case r of
                   String t -> _pubQoS < read (unpack t)
                   _        -> False
  | otherwise = case lookup' pp l o of
                  Nothing   -> False
                  (Just v') -> r < v'
  where
    l = pack . transIdent $ i
    r = transVar v
processCond pp@ParsedProg{..} (PubPkt PublishRequest{..} _) o (ECondLE i v)
  | l == "topic" = False
  | l == "retain" = False
  | l == "QoS" = case r of
                   String t -> _pubQoS > read (unpack t)
                   _        -> False
  | otherwise = case lookup' pp l o of
                  Nothing   -> False
                  (Just v') -> v' < r
  where
    l = pack . transIdent $ i
    r = transVar v

processModify :: (String, Value) -> Message -> Message
processModify (s,v) msg@(PubPkt req@PublishRequest{..} n)
  | s == "topic" = case v of
                     String t -> PubPkt req{_pubTopic = textToBL t} n
                     _        -> msg
  | s == "retain" = case v of
                      Bool b -> PubPkt req{_pubRetain = b} n
                      _      -> msg
  | s == "QoS" = case v of
                   String t -> if t `elem` ["QoS0","QoS1","QoS2"]
                               then PubPkt req{_pubQoS = read (unpack t)} n
                               else msg
                   _ -> msg
  | otherwise = msg

progToFunc :: ParsedProg -> Message -> FuncSeries Message
progToFunc pp@ParsedProg{..} msg@(PubPkt req@PublishRequest{..} n) = do
  let (obj' :: Maybe Object) = decode _pubBody
  case obj' of
    Just obj -> do
      let newobj = if "*" `HM.member` obj then obj else
                     HM.filterWithKey (\k _ -> k `L.elem` (pack <$> Map.keys sel)) obj
          condSat = case whr of
            Nothing -> True
            Just conds -> L.all (== True) (processCond pp msg newobj <$> conds)
          msg'' = case mod of
            Nothing -> msg
            Just mods -> if condSat then
              let modObj = L.foldr (\(s,v) acc -> HM.update (Just . const v) (pack s) acc) newobj mods
                  msg' = PubPkt req{_pubBody = encode modObj} n
              in L.foldr processModify msg' mods
              else msg
      return msg''

    Nothing -> do
      let condSat = case whr of
            Nothing -> True
            Just conds -> L.all (== True) (processCond pp msg HM.empty <$> conds)
          msg'' = case mod of
            Nothing -> msg
            Just mods -> if condSat then L.foldr processModify msg mods else msg
      return msg''

parseSQLFile :: FilePath -> IO (Maybe (Message -> FuncSeries Message))
parseSQLFile f = do
  (sql' :: Either SomeException String) <- try $ readFile f
  case sql' of
    Left e -> return Nothing
    Right sql -> do
      let p' = pProgram . myLexer $ sql
      case p' of
        Bad _ -> return Nothing
        Ok p  -> return . Just . progToFunc . transProgram $ p

-- | Check if there exists a topic in the list that the given one can match.
existMatch :: Topic -> [Topic] -> Bool
existMatch t ts =  True `elem` [pat `match` t | pat <- ts]

-- | Compose a mountpoint with a topic
composeMP :: Topic -> Topic -> Topic
composeMP = append

blToText :: BL.ByteString -> Text
blToText = decodeUtf8 . BL.toStrict

textToBL :: Text -> BL.ByteString
textToBL = BL.fromStrict . encodeUtf8

-- | Insert a element to a certain position of a list.
-- If the index is out of bound, it will be appended to the end.
insertToN :: Int -> a -> [a] -> [a]
insertToN n x xs
  | n < 0 || n > L.length xs = xs ++ [x]
  | otherwise = L.take n xs ++ [x] ++ L.drop n xs

-- | Delete a element at certain position of a list.
-- If the index is out of bound, the list will not be modified.
deleteAtN :: Int -> [a] -> [a]
deleteAtN n xs
  | n < 0 || n > L.length xs = xs
  | otherwise =
    let (p1, p2) = L.splitAt n xs
    in case p2 of
         []     -> p1
         (_:ys) -> p1 ++ ys

-- | Forward message to certain broker. Broker-dependent and will be
-- replaced soon.
fwdTCPMessage :: (Socket,UUID) -> Topic -> BL.ByteString -> IO ()
fwdTCPMessage = sendBridgeMsg
  --BSC.hPutStrLn h $ BL.toStrict (encode msg)

-- | Receive message from certain broker. Broker-dependent and will be
-- replaced soon.
recvTCPMessages :: Socket -> Int -> IO [Maybe Message]
recvTCPMessages = recvBridgeMsgs

recvBridgeMsgs :: Socket -> Int -> IO [Maybe Message]
recvBridgeMsgs s n = do
    msgs' <- HESP.recvMsgs s n
    V.toList <$> mapM processMsg' msgs'
  where
    processMsg' msg' = case msg' of
      Left _ -> return Nothing
      Right msg ->
        case parseRequest msg of
          Left _ -> return Nothing
          Right (SPut topic payload) -> return $ Just $ PlainMsg payload topic

sendBridgeMsg :: (Socket,UUID) -> Topic -> BL.ByteString -> IO ()
sendBridgeMsg (s,cid) topic payload = do
  HESP.sendMsg s $ HESP.mkArrayFromList
    [ HESP.mkBulkString "sput"
    , HESP.mkBulkString $ UUID.toASCIIBytes cid
    , HESP.mkBulkString $ encodeUtf8 topic
    , HESP.mkBulkString $ BL.toStrict payload
    ]

--------------------------
data TCPReqType = SPut Topic BL.ByteString
    deriving (Show, Eq)

parseRequest :: HESP.Message
             -> Either ByteString TCPReqType
parseRequest msg = do
  (n, paras) <- HESP.commandParser msg
  case n of
    "sput" -> parseSPut paras
    _      -> Left $ "Unrecognized request " <> n <> "."

parseSPut :: V.Vector HESP.Message -> Either ByteString TCPReqType
parseSPut paras = do
  topic   <- HESP.extractBulkStringParam "Topic"     paras 0
  payload <- HESP.extractBulkStringParam "Payload"   paras 1
  return $ SPut (decodeUtf8 topic) (BL.fromStrict payload)
