{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BlockArguments      #-}

module Network.MQTT.Bridge.Extra
  ( parseSQLFile
  , existMatch
  , composeMP
  , blToText
  , textToBL
  , insertToN
  , deleteAtN
  , fwdTCPMessage
  , fwdTCPMessage'
  , recvTCPMessage
  , recvBridgeMsg
  , sendBridgeMsg
  ) where

import           Control.Exception
import           Data.Aeson
import qualified Data.ByteString                  as BS
import qualified Data.ByteString.Char8            as BSC (hPutStrLn)
import qualified Data.ByteString.Lazy             as BL
import qualified Data.HashMap.Strict              as HM
import qualified Data.List                        as L
import qualified Data.Map                         as Map
import           Data.Text
import           Data.Text.Encoding
import           Network.MQTT.Bridge.SQL.AbsESQL
import           Network.MQTT.Bridge.SQL.ErrM
import           Network.MQTT.Bridge.SQL.ParESQL
import           Network.MQTT.Bridge.SQL.SkelESQL
import           Network.MQTT.Bridge.Types
import           Network.MQTT.Client
import           Network.MQTT.Topic
import           Network.MQTT.Types
import           Network.Simple.TCP               as TCP
import qualified Network.HESP                     as HESP
import qualified Data.Vector                      as V


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


{-
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

-}

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

{-
-- | Remove elements of a list from another one.
subtractList :: (Eq a) => [a] -> [a] -> [a]
subtractList l s = L.filter (\x -> not (x `L.elem` s)) l
-}

-- | Forward message to certain broker. Broker-dependent and will be
-- replaced soon.
fwdTCPMessage :: Socket -> Message -> IO ()
fwdTCPMessage = sendBridgeMsg
  --BSC.hPutStrLn h $ BL.toStrict (encode msg)

-- | Receive message from certain broker. Broker-dependent and will be
-- replaced soon.
recvTCPMessage :: Socket -> Int -> IO (Maybe Message)
recvTCPMessage = recvBridgeMsg


pub :: BS.ByteString -> BS.ByteString -> HESP.Message
pub topic payload =
  let cs = [ HESP.mkBulkString "pub"
           , HESP.mkBulkString "<unique-id>"
	   , HESP.mkBulkString topic
	   , HESP.mkBulkString payload
           ]
   in HESP.mkArray $ V.fromList cs

fwdTCPMessage' :: Socket -> Message -> IO ()
fwdTCPMessage' s msg = do
  HESP.sendMsg s $ pub "test_topic" (BL.toStrict $ encode msg)
  {-
  ack <- HESP.recvMsg s 1024
  putStr $ "\n-> " ++ show ack ++ "\n"
  case ack of
    Left  e -> putStrLn $ "--> " ++ show e
    Right r -> putStrLn $ "--> " ++ show r ++ " " ++ show msg ++ "\n\n"
  -}

  {-
  (s' :: Either SomeException BS.ByteString) <- try $ BS.hGetLine h
  case s' of
    Left _ -> return Nothing
    Right s -> if BS.null s then return Nothing else return $ decode (BL.fromStrict s)
  -}

recvBridgeMsg :: Socket -> Int -> IO (Maybe Message)
recvBridgeMsg s n = do
  msg' <- HESP.recvMsg s n
  case msg' of
    Left _ -> return Nothing
    Right msg ->
      case msg of
        HESP.MatchSimpleString bs -> return $ decode (BL.fromStrict bs)
        _                         -> return Nothing

sendBridgeMsg :: Socket -> Message -> IO ()
sendBridgeMsg s msg = do
  let bs  = (BL.toStrict $ encode msg)
      msg' = HESP.mkSimpleString bs
  case msg' of
    Left _ -> return ()
    Right msg -> HESP.sendMsg s msg
