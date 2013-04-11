{-# LANGUAGE DeriveDataTypeable #-}
{-# Language ScopedTypeVariables #-}
module Data.HroqMnesia
  (
  -- * Schema etc
    create_schema
  , delete_schema
  , create_table
  , delete_table

  , TableStorage(..)
  -- , Storable(..)
  , change_table_copy_type
  , dirty_write
  , dirty_write_q
  , dirty_read
  , dirty_read_q
  , dirty_all_keys
  , wait_for_tables

  -- * table_info stuff
  , RecordType(..)
  , TableInfoReq(..)
  , TableInfoRsp(..)
  , table_info



  , startHroqMnesia

  -- * debug
  , queueExists
  , get_state

  , State (..)
  , do_dirty_write_q
  )
  where

import Control.Concurrent
import Control.Distributed.Process hiding (call,finally)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Exception as Exception
import Control.Monad(when,replicateM,foldM,liftM3,liftM2,liftM)
import Data.Binary
import Data.Binary.Get
import Data.DeriveTH
import Data.Hroq
import Data.HroqLogger
import Data.Int
import Data.IORef
import Data.List(elemIndices,isInfixOf)
import Data.Maybe(fromJust,fromMaybe,isNothing)
import Data.RefSerialize
import Data.Typeable (Typeable)
import Data.Word
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import System.Directory
import System.IO
import System.IO.Error
import System.IO.Unsafe
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Map as Map
import qualified Data.ByteString.Lazy as L

-- ---------------------------------------------------------------------

hroqMnesiaName = "HroqMnesia"

maxCacheSize :: Int
maxCacheSize = fromIntegral $ maxBucketSizeConst * 3

directoryPrefix :: String
directoryPrefix = ".hroqdata/"

--------------------------------------------------------------------------------
-- Types                                                                      --
--------------------------------------------------------------------------------

-- Call and Cast request types. Response types are unnecessary as the GenProcess
-- API uses the Async API, which in turn guarantees that an async handle can
-- /only/ give back a reply for that *specific* request through the use of an
-- anonymous middle-man (as the sender and reciever in our case).


--  , change_table_copy_type
data ChangeTableCopyType = ChangeTableCopyType !TableName !TableStorage
                           deriving (Typeable, Show)

--  , create_table
data CreateTable = CreateTable !TableStorage !TableName !RecordType
                   deriving (Typeable, Show)

--  , delete_table
data DeleteTable = DeleteTable !TableName
                   deriving (Typeable, Show)

--  , create_schema
data CreateSchema = CreateSchema
                   deriving (Typeable, Show)

--  , delete_schema
data DeleteSchema = DeleteSchema
                   deriving (Typeable, Show)

--  , dirty_all_keys
data DirtyAllKeys = DirtyAllKeys !TableName
                    deriving (Typeable, Show)

--  , dirty_read
data DirtyRead = DirtyRead !TableName !MetaKey
                   deriving (Typeable, Show)

--  , dirty_read
data DirtyReadQ = DirtyReadQ !TableName !QKey
                   deriving (Typeable, Show)

--  , dirty_write
data DirtyWrite = DirtyWrite !TableName !Meta
                   deriving (Typeable, Show)

--  , dirty_write_q
data DirtyWriteQ = DirtyWriteQ !TableName !QEntry
                   deriving (Typeable, Show)

--  , table_info
data TableInfo = TableInfo !TableName !TableInfoReq
                   deriving (Typeable, Show)

--  , wait_for_tables
data WaitForTables = WaitForTables ![TableName] !Delay
                   deriving (Typeable, Show)

-- , get_state
data GetState = GetState
                deriving (Typeable,Show)

-- ---------------------------------------------------------------------
-- Binary instances

instance Binary ChangeTableCopyType where
  put (ChangeTableCopyType tn ts) = put tn >> put ts
  get = do
    tn <- get
    ts <- get
    return (ChangeTableCopyType tn ts)

instance Binary CreateTable where
  put (CreateTable ts tn rt) = put ts >> put tn >> put rt
  get = liftM3 CreateTable get get get

instance Binary DeleteTable where
  put (DeleteTable tn) = put tn
  get = liftM DeleteTable get

instance Binary CreateSchema where
  put CreateSchema = putWord8 1
  get = do
          v <- getWord8
          case v of
            1 -> return CreateSchema

instance Binary DeleteSchema where
  put DeleteSchema = putWord8 1
  get = do
          v <- getWord8
          case v of
            1 -> return DeleteSchema

instance Binary DirtyAllKeys where
  put (DirtyAllKeys tn) = put tn
  get = liftM DirtyAllKeys get

instance Binary DirtyRead where
  put (DirtyRead tn key) = put tn >> put key
  get = liftM2 DirtyRead get get

instance Binary DirtyReadQ where
  put (DirtyReadQ tn key) = put tn >> put key
  get = liftM2 DirtyReadQ get get

instance Binary DirtyWrite where
  put (DirtyWrite tn key) = put tn >> put key
  get = liftM2 DirtyWrite get get

instance Binary DirtyWriteQ where
  put (DirtyWriteQ tn key) = put tn >> put key
  get = liftM2 DirtyWriteQ get get

instance Binary TableInfo where
  put (TableInfo tn req) = put tn >> put req
  get = liftM2 TableInfo get get

instance Binary WaitForTables where
  put (WaitForTables tables delay) = put tables >> put delay
  get = liftM2 WaitForTables get get

instance Binary GetState where
  put GetState = putWord8 1
  get = do
          v <- getWord8
          case v of
            1 -> return GetState

-- ---------------------------------------------------------------------

data HroqMnesiaResult = HMResOk
                      | HMResTimeout ![TableName]
                      | HMResError !String
                      deriving (Typeable,Show)

instance Binary HroqMnesiaResult where
  put HMResOk           = putWord8 1
  put (HMResTimeout ts) = putWord8 2 >> put ts
  put (HMResError s)    = putWord8 3 >> put s

  get = do
          v <- getWord8
          case v of
            1 -> return HMResOk
            2 -> liftM  HMResTimeout get
            3 -> liftM  HMResError get

-- ---------------------------------------------------------------------
-- State related functions

data TableMeta = TableMeta
  { tSize       :: !(Maybe Integer) -- ^Size of stored table, if known
  , tStorage    :: !TableStorage
  , tRecordType :: !RecordType
  } deriving (Show,Typeable)

data State = MnesiaState
  { sTableInfo :: !(Map.Map TableName TableMeta)
  , sRamQ      :: !(Map.Map TableName [QEntry])
  , sRamMeta   :: !(Map.Map TableName [Meta])
  } deriving (Show,Typeable)

instance Binary TableMeta where
  put (TableMeta size storage typ) = put size >> put storage >> put typ
  get = liftM3 TableMeta get get get

instance Binary State where
  put (MnesiaState ti rq rm) = put ti >> put rq >> put rm
  get = liftM3 MnesiaState get get get

-- ---------------------------------------------------------------------

getMetaForTable :: State -> TableName -> Maybe (TableMeta)
getMetaForTable s tableName = Map.lookup tableName (sTableInfo s)

getMetaForTableDefault :: State -> TableName -> TableMeta
getMetaForTableDefault s tableName =
  fromMaybe (TableMeta Nothing StorageNone RecordTypeQueueEntry)
           $ getMetaForTable s tableName


updateTableInfoMeta :: State -> TableName -> TableStorage -> [Meta] -> State
updateTableInfoMeta s tableName _storage vals = s'
  where
    m@(TableMeta _ storage _) = getMetaForTableDefault s tableName
    cnt = fromIntegral $ length vals
    ti' = Map.insert tableName (m { tSize = Just cnt }) (sTableInfo s)
    rm' = case storage of
            DiscOnlyCopies -> (sRamMeta s)
            _ ->  Map.insert tableName vals (sRamMeta s)
    s' = s { sTableInfo = ti', sRamMeta = rm' }


insertEntryMeta :: State -> TableName -> Meta -> State
insertEntryMeta s tableName val = s'
  where
    -- Add one to the Q size
    curr@(TableMeta _ storage _) = getMetaForTableDefault s tableName
    cnt = (fromMaybe 0 (tSize curr)) + 1
    ti' = Map.insert tableName (curr { tSize = Just cnt }) (sTableInfo s)

    -- Add the written record to the cache, if not DiscOnlyCopies
    rm' = case storage of
            DiscOnlyCopies -> (sRamMeta s)
            -- _ ->  Map.insert tableName (((sRamMeta s) Map.! tableName) ++ [val]) (sRamMeta s)
            _ ->  if Map.member tableName (sRamMeta s)
                   -- then Map.insert tableName (((sRamMeta s) Map.! tableName) ++ [val]) (sRamMeta s)
                   then Map.insert tableName (                                  [val]) (sRamMeta s)
                   else Map.insert tableName (                                  [val]) (sRamMeta s)

    s' = s {sTableInfo = ti', sRamMeta = rm'}


updateTableInfoQ :: State -> TableName -> TableStorage -> [QEntry] -> State
updateTableInfoQ s tableName _storage vals = s'
  where
    m@(TableMeta _ storage _) = getMetaForTableDefault s tableName
    cnt = fromIntegral $ length vals
    ti' = Map.insert tableName (m { tSize = Just cnt }) (sTableInfo s)
    rq' = case storage of
            DiscOnlyCopies -> (sRamQ s)
            _ ->  Map.insert tableName vals (sRamQ s)
    -- s' = s { sTableInfo = ti', sRamQ = rq' }
    s' = s { sTableInfo = ti', sRamQ = strictify rq' }

insertEntryQ :: State -> TableName -> QEntry -> State
insertEntryQ s tableName val = s'
  where
    -- Add one to the Q size
    curr@(TableMeta _ storage _) = getMetaForTableDefault s tableName
    cnt = (fromMaybe 0 (tSize curr)) + 1
    ti' = Map.insert tableName (curr { tSize = Just cnt }) (sTableInfo s)

    -- Add the written record to the cache, if not DiscOnlyCopies
    rq' = case storage of
            DiscOnlyCopies -> (sRamQ s)
            _ ->  if Map.member tableName (sRamQ s)
                   then Map.insert tableName (((sRamQ s) Map.! tableName) ++ [val]) (sRamQ s)
                   else Map.insert tableName (                               [val]) (sRamQ s)
    -- s' = s {sTableInfo = ti', sRamQ = rq'}
    s' = s {sTableInfo = ti', sRamQ = strictify rq'}


--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------

change_table_copy_type :: TableName -> TableStorage -> Process ()
change_table_copy_type tableName storage = mycall (ChangeTableCopyType tableName storage)

create_table :: TableStorage -> TableName -> RecordType -> Process ()
create_table storage tableName recordType = mycall (CreateTable storage tableName recordType)

delete_table :: TableName -> Process ()
delete_table tableName = mycall (DeleteTable tableName)

create_schema :: Process ()
create_schema = mycall (CreateSchema)

delete_schema :: Process ()
delete_schema = mycall (DeleteSchema)

dirty_all_keys :: TableName -> Process [QKey]
dirty_all_keys tableName = mycall (DirtyAllKeys tableName)

dirty_read :: TableName -> MetaKey -> Process (Maybe Meta)
dirty_read tableName key = mycall (DirtyRead tableName key)

dirty_read_q :: TableName -> QKey -> Process (Maybe QEntry)
dirty_read_q tableName key = mycall (DirtyReadQ tableName key)

dirty_write :: TableName -> Meta -> Process ()
dirty_write tableName val = mycall (DirtyWrite tableName val)

dirty_write_q :: TableName -> QEntry -> Process ()
dirty_write_q tablename val = mycall (DirtyWriteQ tablename val)

table_info :: TableName -> TableInfoReq -> Process TableInfoRsp
table_info tableName req = mycall (TableInfo tableName req)

wait_for_tables :: [TableName] -> Delay -> Process ()
wait_for_tables tables delay = mycall (WaitForTables tables delay)

get_state :: Process State
get_state = mycall (GetState)

-- | Start a Queue server
startHroqMnesia :: a -> Process ProcessId
startHroqMnesia initParams = do
  let server = serverDefinition
  sid <- spawnLocal $ start initParams initFunc server >> return ()
  register hroqMnesiaName sid
  return sid

-- init callback
initFunc :: InitHandler a State
initFunc _ = do
  let s = (MnesiaState Map.empty Map.empty Map.empty)
  ems <- liftIO $ Exception.try $ decodeFileSchema (tableNameToFileName schemaTable)
  logm $ "initFunc:ems=" ++ (show ems)
  let m = case ems of
            Left (e :: SomeException) -> Map.empty
            Right [ms] -> ms
            Right _    -> Map.empty -- junk read

  -- Set all size fields to Nothing, to prompt explicit check when
  -- doing wait_for_tables
  let m' = Map.map (\(TableMeta _ st rt) -> (TableMeta Nothing st rt)) m
  let s' = s {sTableInfo = m'}

  return $ InitOk s' Infinity

schemaTable :: TableName
schemaTable = TN "hschema"

-- ---------------------------------------------------------------------

getSid :: Process ProcessId
getSid = do
  -- deliberately blow up if not registered
  Just pid <- whereis hroqMnesiaName
  return pid

mycall op = do
  sid <- getSid
  call sid op

-- ---------------------------------------------------------------------

data TableStorage = DiscOnlyCopies
                  | DiscCopies
                  | RamCopies
                  | StorageNone
                  deriving (Show,Ord,Eq)

instance Binary TableStorage where
  put DiscOnlyCopies = put 'O'
  put DiscCopies     = put 'D'
  put RamCopies      = put 'R'
  put StorageNone    = put 'N'

  get = do
    v <- get
    case v of
      'O' -> return DiscOnlyCopies
      'D' -> return DiscCopies
      'R' -> return RamCopies
      'N' -> return StorageNone

-- ---------------------------------------------------------------------

data RecordType = RecordTypeMeta
                | RecordTypeQueueEntry
                | RecordTypeConsumerLocal
                deriving (Show)

instance Binary RecordType where
  put RecordTypeMeta          = put (1::Word8)
  put RecordTypeQueueEntry    = put (2::Word8)
  put RecordTypeConsumerLocal = put (3::Word8)

  get = do
    v <- getWord8
    case v of
      1 -> return RecordTypeMeta
      2 -> return RecordTypeQueueEntry
      3 -> return RecordTypeConsumerLocal


--------------------------------------------------------------------------------
-- Implementation                                                             --
--------------------------------------------------------------------------------

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
     apiHandlers = [
          handleCall handleChangeTableCopyType
        , handleCall handleCreateTable
        , handleCall handleDeleteTable
        , handleCall handleCreateSchema
        , handleCall handleDeleteSchema
        , handleCall handleDirtyAllKeys
        , handleCall handleDirtyRead
        , handleCall handleDirtyReadQ
        , handleCall handleDirtyWrite
        , handleCall handleDirtyWriteQ
        , handleCall handleTableInfo
        , handleCall handleWaitForTables

        -- * Debug routines
        , handleCall handleGetState
        ]
    , infoHandlers =
        [
        -- handleInfo_ (\(ProcessMonitorNotification _ _ r) -> logm $ show r >> continue_)
         handleInfo (\dict (ProcessMonitorNotification _ _ r) -> do {logm $ show r; continue dict })
        ]
     , timeoutHandler = \_ _ -> stop $ TerminateOther "timeout az"
     , terminateHandler = \_ reason -> do { logm $ "HroqMnesia terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State

-- ---------------------------------------------------------------------
-- Handlers

handleChangeTableCopyType :: State -> ChangeTableCopyType -> Process (ProcessReply State ())
handleChangeTableCopyType s (ChangeTableCopyType tableName storage) = do
    s' <- do_change_table_copy_type s tableName storage
    reply () s'

handleCreateTable :: State -> CreateTable -> Process (ProcessReply State ())
handleCreateTable s (CreateTable storage tableName recordType) = do
    s' <- do_create_table s storage tableName recordType
    reply () s'

handleDeleteTable :: State -> DeleteTable -> Process (ProcessReply State ())
handleDeleteTable s (DeleteTable tableName) = do
    s' <- do_delete_table s tableName
    reply () s'

handleCreateSchema :: State -> CreateSchema -> Process (ProcessReply State ())
handleCreateSchema s (CreateSchema) = do
    s' <- do_create_schema s
    reply () s'

handleDeleteSchema :: State -> DeleteSchema -> Process (ProcessReply State ())
handleDeleteSchema s _ = do
    s' <- do_delete_schema s
    reply () s'

handleDirtyAllKeys :: State -> DirtyAllKeys -> Process (ProcessReply State [QKey])
handleDirtyAllKeys s (DirtyAllKeys tableName) = do
    res <- do_dirty_all_keys tableName
    reply res s

handleDirtyRead :: State -> DirtyRead -> Process (ProcessReply State (Maybe Meta))
handleDirtyRead s (DirtyRead tableName key) = do
    res <- do_dirty_read tableName key
    reply res s

handleDirtyReadQ :: State -> DirtyReadQ -> Process (ProcessReply State (Maybe QEntry))
handleDirtyReadQ s (DirtyReadQ tableName key) = do
    res <- do_dirty_read_q tableName key
    reply res s

handleDirtyWrite :: State -> DirtyWrite -> Process (ProcessReply State ())
handleDirtyWrite s (DirtyWrite tableName val) = do
    s' <- do_dirty_write s tableName val
    reply () s'

handleDirtyWriteQ :: State -> DirtyWriteQ -> Process (ProcessReply State ())
handleDirtyWriteQ s (DirtyWriteQ tableName val) = do
    s' <- do_dirty_write_q s tableName val
    reply () s'


handleTableInfo :: State -> TableInfo -> Process (ProcessReply State TableInfoRsp)
handleTableInfo s (TableInfo tableName req) = do
    (s',res) <- do_table_info s tableName req
    reply res s'

handleWaitForTables :: State -> WaitForTables -> Process (ProcessReply State ())
handleWaitForTables s (WaitForTables tables delay) = do
    s' <- do_wait_for_tables s tables delay
    reply () s'

handleGetState :: State -> GetState -> Process (ProcessReply State State)
handleGetState s _ = reply s s

-- ---------------------------------------------------------------------

do_change_table_copy_type :: State -> TableName -> TableStorage -> Process State
do_change_table_copy_type s bucket DiscOnlyCopies = do
  logm $ "change_table_copy_type to DiscOnlyCopies for:" ++ (show (bucket))
  let (TableMeta _ st rt) = getMetaForTableDefault s bucket
  logm $ "change_table_copy_type:(st,rt)=" ++ (show (st,rt))
  let s' = case rt of
            RecordTypeMeta       -> s {sRamMeta = Map.delete bucket (sRamMeta s)}
            -- RecordTypeQueueEntry -> s {sRamQ    = Map.delete bucket (sRamQ s)}
            RecordTypeQueueEntry -> s {sRamQ    = strictify $ Map.delete bucket (sRamQ s)}
            _                    -> s
  -- logm $ "change_table_copy_type:s'=" ++ (show s')

  let (TableMeta size _ rt) = getMetaForTableDefault s' bucket
  let s'' = s' { sTableInfo = Map.insert bucket (TableMeta size DiscOnlyCopies rt) (sTableInfo s')}
  -- logm $ "change_table_copy_type:s''=" ++ (show (s''))
  persistTableInfo (sTableInfo s'')
  return s''

do_change_table_copy_type s bucket storageType = do
  logm $ "change_table_copy_type undefined for:" ++ (show (bucket,storageType))
  return s

mySyncCheck :: Integer -> Integer -> Integer -> Bool
mySyncCheck _ _ _ = True

-- ---------------------------------------------------------------------

persistTableInfo :: Map.Map TableName TableMeta -> Process ()
persistTableInfo ti = do
  logm $ "persistTableInfo starting for:" ++ (show ti)
  -- liftIO $ threadDelay (1*1000000) -- 1 seconds
  res <- liftIO $ Exception.try $ defaultWrite (tableNameToFileName schemaTable) (encode ti)
  -- liftIO $ threadDelay (1*1000000) -- 1 seconds
  case res of
    Left (e :: SomeException) -> logm $ "persistTableInfo:error:" ++ (show e)
    Right _ -> logm $ "persistTableInfo done"
  -- liftIO $ threadDelay (1*1000000) -- 1 seconds

-- ---------------------------------------------------------------------

-- |Record the table details into the meta information.
do_create_table :: State -> TableStorage -> TableName -> RecordType -> Process State
do_create_table s storage name recordType = do
  logm $ "create_table:" ++ (show (name,storage,recordType))
  -- TODO: check for clash with pre-existing, both in meta/state and
  --       on disk
  let ti' = Map.insert name (TableMeta Nothing storage recordType) (sTableInfo s)
  logm $ "do_create_table:ti'=" ++ (show ti')

  -- TODO: use an incremental write, rather than full dump
  -- TODO: check result?
  persistTableInfo ti'
  return $ s { sTableInfo = ti' }

-- ---------------------------------------------------------------------

do_delete_table :: State -> TableName -> Process State
do_delete_table s name = do
  logm $ "delete_table:" ++ (show name)
  liftIO $ defaultDelete $ tableNameToFileName name
  let ti' = Map.delete name (sTableInfo s)
  return $ s {sTableInfo = ti'}

-- ---------------------------------------------------------------------

-- |Create a new schema
do_create_schema :: State -> Process State
do_create_schema s = do
  logm $ "undefined create_schema"
  return s

-- |Delete the schema
do_delete_schema :: State -> Process State
do_delete_schema s = do
  logm $ "undefined delete_schema"
  return s

-- ---------------------------------------------------------------------

-- |Write the value.
-- Currently only used for the meta table, which only has one entry in it
do_dirty_write :: State -> TableName -> Meta -> Process State
do_dirty_write s tableName record = do
  logm $ "dirty_write:" ++ (show (tableName,record))
  liftIO $ defaultWrite (tableNameToFileName tableName) (encode record)
  let s' = insertEntryMeta s tableName record
  return s'

do_dirty_write_q ::
   State -> TableName -> QEntry -> Process State
do_dirty_write_q s tableName record = do
  logm $ "dirty_write:" ++ (show (tableName,record))
  liftIO $ defaultAppend (tableNameToFileName tableName) (encode record)

  let s' = insertEntryQ s tableName record
  return s'

-- ---------------------------------------------------------------------

do_dirty_read :: TableName -> MetaKey -> Process (Maybe Meta)
do_dirty_read tableName keyVal = do
  logm $ "dirty_read:" ++ (show (tableName)) -- ,keyVal))
  ems <- liftIO $ Exception.try $ decodeFileMeta (tableNameToFileName tableName)
  case ems of
    Left (e::IOError) -> do
      logm $ "do_dirty_read e " ++ (show keyVal) ++ ":" ++ (show e)
      return Nothing
    Right ms -> do
      let ms' = filter (\(MAllBuckets key _ _) -> key == keyVal) ms
      logm $ "do_dirty_read ms' " ++ (show keyVal) ++ ":" ++ (show ms')
      if ms' == [] then return Nothing
                   else return $ Just (head ms')

-- ---------------------------------------------------------------------

do_dirty_read_q :: TableName -> QKey -> Process (Maybe QEntry)
do_dirty_read_q tableName keyVal = do
  logm $ "unimplemented dirty_read_q:" ++ (show (tableName)) -- ,keyVal))
  return Nothing

-- ---------------------------------------------------------------------

do_dirty_all_keys :: TableName -> Process [QKey]
do_dirty_all_keys tableName = do
  logm $ "unimplemented dirty_all_keys:" ++ (show tableName)
  return []

-- ---------------------------------------------------------------------

do_wait_for_tables :: State -> [TableName] -> Delay -> Process State
do_wait_for_tables s tables _maxWait = do
  logm $ "wait_for_tables:sTableInfo=" ++ (show (sTableInfo s))
  s' <- foldM waitForTable s tables
  logm $ "do_wait_for_tables:done: sTableInfo'=" ++ (show (sTableInfo s'))
  return s'


waitForTable :: State -> TableName -> Process State
waitForTable s table = do
      -- TODO: load the table info into the meta zone
      logm $ "waitForTable:" ++ (show table)
      let mm = getMetaForTable s table
      logm $ "waitForTable:me=" ++ (show mm)
      when (isNothing mm) $ logm $ "waitForTable:mm=Nothing,s=" ++ (show s)
      let (TableMeta _ms storage recordType) = fromMaybe (TableMeta Nothing StorageNone RecordTypeMeta) mm

      exists  <- queueExists table
      logm $ "wait_for_tables:(table,exists,recordType)=" ++ (show (table,exists,recordType))
      res <- case exists of
        True -> do
          newS <- do
            case recordType of
              RecordTypeMeta       -> do
                logm $ "waitForTable: RecordTypeMeta"
                -- TODO: refactor this and next case into one fn
                ems <- liftIO $ Exception.try $ decodeFileMeta (tableNameToFileName table)
                logm $ "wait_for_tables:ems=" ++ (show ems)
                case ems of
                  Left (e :: IOError) -> return s
                  Right ms -> return $ updateTableInfoMeta s table storage ms

              RecordTypeQueueEntry -> do
                logm $ "waitForTable: RecordTypeQueueEntry"
                ems <- liftIO $ Exception.try $ decodeFileQEntry (tableNameToFileName table)
                logm $ "wait_for_tables:ems=" ++ (show ems)
                case ems of
                  Left (e :: IOError) -> return s
                  Right ms -> return $ updateTableInfoQ s table storage ms
              _ -> do
                logm "do_wait_for_tables:unknown record type"
                return s

          return newS
        False -> do
          logm $ "wait_for_tables:False, done:" ++ (show table)
          case recordType of
            RecordTypeMeta       -> return $ updateTableInfoMeta s table StorageNone []
            RecordTypeQueueEntry -> return $ updateTableInfoQ    s table StorageNone []
            _                    -> return s
      logm $ "waitForTable done:res=" ++ (show res)
      return res
      -- return (table, TableMeta Nothing StorageNone RecordTypeMeta)

-- ---------------------------------------------------------------------

decodeFileSchema :: FilePath -> IO [Map.Map TableName TableMeta]
decodeFileSchema = decodeFileBinaryList

decodeFileMeta :: FilePath -> IO [Meta]
decodeFileMeta = decodeFileBinaryList

decodeFileQEntry :: FilePath -> IO [QEntry]
decodeFileQEntry = decodeFileBinaryList

-- ---------------------------------------------------------------------

decodeFileBinaryList :: (Binary a) => FilePath -> IO [a]
decodeFileBinaryList filename = do
  -- runGetState :: Get a -> ByteString -> Int64 -> (a, ByteString, Int64)
  s <- L.readFile filename
  go [] s 0
  where
    go acc bs offset = do
      if (L.null bs)
        then return acc
        else do
          (v,bs',offset') <- dm bs offset
          go (acc++[v]) bs' offset'

    dm :: (Binary a) => L.ByteString -> Int64 -> IO (a, L.ByteString, Int64)
    dm bs offset = do
      return $ runGetState (do v <- get
                               m <- isEmpty
                               m `seq` return v) bs offset

-- ---------------------------------------------------------------------

getLengthOfFile ::
  (Exception e, Binary a) => (Either e [a]) -> Process Integer
getLengthOfFile e = do
  case e of
    Left er -> do
      logm $ "getLengthOfFile:(er)=" ++ (show (er))
      return 0
    Right xs -> return $ fromIntegral $ length xs

-- ---------------------------------------------------------------------

data TableInfoReq = TableInfoSize
                  | TableInfoStorageType
                    deriving (Show)

instance Binary TableInfoReq where
  put TableInfoSize = put (1::Word8)
  put TableInfoStorageType = put (2::Word8)

  get = do
    v <- get
    case v of
      (1::Word8) -> return TableInfoSize
      (2::Word8) -> return TableInfoStorageType

data TableInfoRsp = TISize !Integer
                  | TIStorageType !TableStorage
                  | TIError
                    deriving (Show,Typeable)

instance Binary TableInfoRsp where
  put (TISize v)         = put (1::Word8) >> put v
  put (TIStorageType ts) = put (2::Word8) >> put ts
  put TIError            = put (3::Word8)

  get = do
    t <- get
    case t of
      (1::Word8) -> do {v  <- get; return (TISize v)}
      (2::Word8) -> do {ts <- get; return (TIStorageType ts)}
      (3::Word8) -> return TIError

-- ---------------------------------------------------------------------

do_table_info :: State -> TableName -> TableInfoReq -> Process (State,TableInfoRsp)
do_table_info s tableName TableInfoSize        = do
  -- logm $ "table_info:TableInfoSize" ++ (show (tableName))
  getBucketSize s tableName
do_table_info s tableName TableInfoStorageType = do
  -- logm $ "table_info:TableInfoStorageType" ++ (show (tableName))
  getStorageType s tableName
do_table_info s tableName infoReq = do
  -- logm $ "table_info undefined:" ++ (show (tableName,infoReq))
  return (s,TIError)

-- ---------------------------------------------------------------------

getBucketSize :: State -> TableName -> Process (State,TableInfoRsp)
getBucketSize s tableName = do
  logm $ "getBucketSize " ++ (show tableName)
  let mm = getMetaForTable s tableName
  -- logm $ "getBucketSize:mm=" ++ (show mm)
  s' <- if (isNothing mm) then waitForTable s tableName
                          else return s
  let mm' = getMetaForTable s' tableName
  -- logm $ "getBucketSize:mm'=" ++ (show mm')
  case mm' of
    Nothing -> do
      logm $ "  getBucketSize(nonexist) "
      return $ (s',TISize 0)
    Just (TableMeta msize _ _) -> do
      logm $ "  getBucketSize(exists) " ++ (show (tableName,msize))
      case msize of
        Nothing -> do
          s'' <- waitForTable s' tableName
          let mm'' = getMetaForTableDefault s'' tableName
          logm $ "getBucketSize:mm''=" ++ (show mm'')
          return (s'', TISize $ fromMaybe 0 (tSize mm''))
        Just size -> return (s',TISize size)
{-
  exists <- queueExists tableName
  logm $ "getBucketSize exists=" ++ (show exists)
  case exists of
    True -> do
      -- logm $ "  getBucketSize(exists) " ++ (show (tableName,res))
      return $ TISize 0
    False -> do
      logm $ "  getBucketSize(nonexist) "
      return $ TISize 0
-}

getStorageType :: State -> TableName -> Process (State,TableInfoRsp)
getStorageType s tableName = do
  e  <- queueExists tableName
  let storage = if e then DiscCopies else StorageNone
  logm $ "getStorageType:" ++ (show (tableName,storage))
  return $ (s,TIStorageType storage)

-- ---------------------------------------------------------------------

tableNameToFileName :: TableName -> FilePath
tableNameToFileName (TN tableName) = directoryPrefix ++ tableName

-- ---------------------------------------------------------------------
-- TCache specific functions used here

queueExists :: TableName -> Process Bool
queueExists tableName = do
    res <- liftIO $ doesFileExist $ tableNameToFileName tableName
    return res

-- ---------------------------------------------------------------------

decodeFileMaybe ::
  (Exception e, Binary a) => FilePath -> IO (Either e [a])
decodeFileMaybe filename = do
  Exception.try (decodeFile filename)

{-
-- decodeFileMaybe :: (Binary a) => FilePath -> IO (Either String a)
decodeFileMaybe filename = handle handler $ Right (decodeFile filename)
  where
   -- handler :: IOError -> IO (Either String a)
   handler (e::IOError) = return $ Left ("decodeFileMaybe failed, got:" ++ (show e))
-}
-- =====================================================================
-- ---------------------------------------------------------------------
-- The following functions are courtesy of TCache by agocorona
-- https://github.com/agocorona/TCache


-- ---------------------------------------------------------------------

defaultWrite  :: FilePath -> B.ByteString -> IO ()
defaultWrite  filename x = safeFileOp B.writeFile  filename x

defaultAppend :: FilePath -> B.ByteString -> IO ()
defaultAppend filename x = safeFileOp B.appendFile filename x

-- ---------------------------------------------------------------------

safeFileOp :: (FilePath -> B.ByteString -> IO ()) -> FilePath -> B.ByteString -> IO ()
safeFileOp op filename str= handle  handler  $ op filename str  -- !> ("write "++filename)
     where
     handler e-- (e :: IOError)
       | isDoesNotExistError e=do
                  createDirectoryIfMissing True $ take (1+(last $ elemIndices '/' filename)) filename   --maybe the path does not exist
                  safeFileOp op filename str

       | otherwise= if ("invalid" `isInfixOf` ioeGetErrorString e)
             then
                error  $ "writeResource: " ++ show e ++ " defPath and/or keyResource are not suitable for a file path"
             else do
                hPutStrLn stderr $ "defaultWriteResource:  " ++ show e ++  " in file: " ++ filename ++ " retrying"
                safeFileOp op filename str

-- ---------------------------------------------------------------------

defaultDelete :: FilePath -> IO ()
defaultDelete filename =do
     handle (handler filename) $ removeFile filename
     --print  ("delete "++filename)
     where

     handler :: String -> IOException -> IO ()
     handler file e
       | isDoesNotExistError e= return ()  --`debug` "isDoesNotExistError"
       | isAlreadyInUseError e= do
            hPutStrLn stderr $ "defaultDelResource: busy"  ++  " in file: " ++ filename ++ " retrying"
--            threadDelay 100000   --`debug`"isAlreadyInUseError"
            defaultDelete filename
       | otherwise = do
           hPutStrLn stderr $ "defaultDelResource:  " ++ show e ++  " in file: " ++ filename ++ " retrying"
--           threadDelay 100000     --`debug` ("otherwise " ++ show e)
           defaultDelete filename


-- ---------------------------------------------------------------------

-- | Strict read from file, needed for default file persistence
readFileStrict :: FilePath -> IO B.ByteString
readFileStrict f = openFile f ReadMode >>= \ h -> readIt h `finally` hClose h
  where
  readIt h= do
      s   <- hFileSize h
      let n= fromIntegral s
      str <- B.hGet h n -- replicateM n (B.hGetChar h)
      return str

-- ---------------------------------------------------------------------


strictify :: (Map.Map TableName [QEntry]) -> (Map.Map TableName [QEntry])
strictify m = Map.fromList $ Map.empty `seq` Map.toList m

