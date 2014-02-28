{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE DeriveGeneric       #-}
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
  , dirty_write_q_sid
  -- , dirty_write_ls
  , dirty_read
  , dirty_read_q
  -- , dirty_read_ls
  , dirty_delete_q
  -- , dirty_delete_ls
  , dirty_all_keys
  , wait_for_tables

  -- * table_info stuff
  , RecordType(..)
  , TableInfoReq(..)
  , TableInfoRsp(..)
  , table_info



  , startHroqMnesia
  , getSid

  -- * debug
  , queueExists
  , log_state
  , tableNameToFileName

  , State (..)
  , do_dirty_write_q
  )
  where

import Control.Distributed.Process hiding (call,finally)
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Exception as Exception
import Control.Monad(when,foldM)
import Data.Binary
import Data.Binary.Get
import Data.Hroq
import Data.HroqHandlePool
import Data.HroqLogger
import Data.Int
import Data.List(elemIndices,isInfixOf,(\\))
import Data.Maybe(fromMaybe,isNothing)
import Data.Typeable (Typeable)
import Prelude
import System.Directory
import System.IO
import System.IO.Error
import GHC.Generics
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Map as Map

import qualified System.Remote.Monitoring as EKG

-- ---------------------------------------------------------------------


hroqMnesiaName :: String
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
                           deriving (Typeable, Show,Generic)

--  , create_table
data CreateTable = CreateTable !TableStorage !TableName !RecordType
                   deriving (Typeable, Show,Generic)

--  , delete_table
data DeleteTable = DeleteTable !TableName
                   deriving (Typeable, Show,Generic)

--  , create_schema
data CreateSchema = CreateSchema
                   deriving (Typeable, Show,Generic)

--  , delete_schema
data DeleteSchema = DeleteSchema
                   deriving (Typeable, Show,Generic)

--  , dirty_all_keys
data DirtyAllKeys = DirtyAllKeys !TableName
                    deriving (Typeable, Show,Generic)

--  , dirty_read
data DirtyRead = DirtyRead !TableName !MetaKey
                   deriving (Typeable, Show,Generic)

--  , dirty_read
data DirtyReadQ = DirtyReadQ !TableName !QKey
                   deriving (Typeable, Show,Generic)

--  , dirty_read
data DirtyReadLS = DirtyReadLS !TableName !ConsumerName
                   deriving (Typeable, Show,Generic)

--  , dirty_delete_q
data DirtyDeleteQ = DirtyDeleteQ !TableName !QKey
                   deriving (Typeable, Show,Generic)

{-
--  , dirty_delete_ls
data DirtyDeleteLS = DirtyDeleteLS !TableName !ConsumerName
                   deriving (Typeable, Show)
-}

--  , dirty_write
data DirtyWrite = DirtyWrite !TableName !Meta
                   deriving (Typeable, Show,Generic)

--  , dirty_write_q
data DirtyWriteQ = DirtyWriteQ !TableName !QEntry
                   deriving (Typeable, Show,Generic)

{-
--  , dirty_write_ls
data DirtyWriteLS = DirtyWriteLS !TableName !ConsumerMessage
                   deriving (Typeable, Show)
-}
--  , table_info
data TableInfo = TableInfo !TableName !TableInfoReq
                   deriving (Typeable, Show,Generic)

--  , wait_for_tables
data WaitForTables = WaitForTables ![TableName] !Delay
                   deriving (Typeable, Show,Generic)

-- , log_state
data LogState = LogState
                deriving (Typeable,Show,Generic)

-- ---------------------------------------------------------------------
-- Binary instances

instance Binary ChangeTableCopyType where
instance Binary CreateTable where
instance Binary DeleteTable where
instance Binary CreateSchema where
instance Binary DeleteSchema where
instance Binary DirtyAllKeys where
instance Binary DirtyRead where
instance Binary DirtyReadQ where
instance Binary DirtyReadLS where
instance Binary DirtyDeleteQ where
instance Binary DirtyWrite where
instance Binary DirtyWriteQ where
instance Binary TableInfo where
instance Binary WaitForTables where
instance Binary LogState where

-- ---------------------------------------------------------------------

data HroqMnesiaResult = HMResOk
                      | HMResTimeout ![TableName]
                      | HMResError !String
                      deriving (Typeable,Show,Generic)

instance Binary HroqMnesiaResult where

-- ---------------------------------------------------------------------
-- State related functions

data TableMeta = TableMeta
  { tSize       :: !(Maybe Integer) -- ^Size of stored table, if known
  , tStorage    :: !TableStorage
  , tRecordType :: !RecordType
  } deriving (Show,Typeable,Generic)

data State = MnesiaState
  { sTableInfo :: !(Map.Map TableName TableMeta)
  , sRamQ      :: !(Map.Map TableName [QEntry])
  , sRamMeta   :: !(Map.Map TableName [Meta])
  , sEkg       :: !EKG.Server
  } deriving (Show,Typeable,Generic)

instance Show EKG.Server where
  show _ = "EKG.Server"

instance Binary TableMeta where

{-
instance Binary State where
  put (MnesiaState ti rq rm ekg) = put ti >> put rq >> put rm >> put ekg
  get = liftM4 MnesiaState get get get get
-}
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
    s' = s { sTableInfo = ti', sRamQ = rq' }
    -- s' = s { sTableInfo = ti', sRamQ = strictify rq' }

updateTableInfoLS :: State -> TableName -> TableStorage -> [ConsumerMessage] -> State
updateTableInfoLS s tableName _storage vals = s'
  where
    m@(TableMeta _ storage _) = getMetaForTableDefault s tableName
    cnt = fromIntegral $ length vals
    ti' = Map.insert tableName (m { tSize = Just cnt }) (sTableInfo s)
    {-
    rq' = case storage of
            DiscOnlyCopies -> (sRamQ s)
            _ ->  Map.insert tableName vals (sRamQ s)
    s' = s { sTableInfo = ti', sRamQ = rq' }
    -}
    s' = s { sTableInfo = ti' }

-- ---------------------------------------------------------------------

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
    s' = s {sTableInfo = ti', sRamQ = rq'}

-- ---------------------------------------------------------------------

insertEntryLS :: State -> TableName -> ConsumerMessage -> State
insertEntryLS s tableName val = s'
  where
    -- Add one to the Q size
    curr@(TableMeta _ storage _) = getMetaForTableDefault s tableName
    cnt = (fromMaybe 0 (tSize curr)) + 1
    ti' = Map.insert tableName (curr { tSize = Just cnt }) (sTableInfo s)

  -- TODO: add ram caching for local storage
{-
    -- Add the written record to the cache, if not DiscOnlyCopies
    rq' = case storage of
            DiscOnlyCopies -> (sRamQ s)
            _ ->  if Map.member tableName (sRamQ s)
                   then Map.insert tableName (((sRamQ s) Map.! tableName) ++ [val]) (sRamQ s)
                   else Map.insert tableName (                               [val]) (sRamQ s)
    s' = s {sTableInfo = ti', sRamQ = rq'}
-}
    s' = s {sTableInfo = ti'}

-- ---------------------------------------------------------------------

deleteEntryQ :: State -> TableName -> QEntry -> State
deleteEntryQ s tableName val = s'
  where
    -- Delete one from the Q size
    curr@(TableMeta _ storage _) = getMetaForTableDefault s tableName
    cnt = (fromMaybe 0 (tSize curr)) - 1
    -- TODO: delete table if 0 or less
    ti' = Map.insert tableName (curr { tSize = Just cnt }) (sTableInfo s)

    -- remove the deleted record from the cache, if not DiscOnlyCopies
    rq' = case storage of
            DiscOnlyCopies -> (sRamQ s)
            _ ->  if Map.member tableName (sRamQ s)
                   then Map.insert tableName (((sRamQ s) Map.! tableName) \\ [val]) (sRamQ s)
                   -- else Map.insert tableName (                               [val]) (sRamQ s)
                   else (sRamQ s)
    s' = s {sTableInfo = ti', sRamQ = rq'}

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

{-
dirty_read_ls :: TableName -> ConsumerName -> Process (Maybe ConsumerMessage)
dirty_read_ls tableName key = mycall (DirtyReadLS tableName key)
-}

dirty_delete_q :: TableName -> QKey -> Process ()
dirty_delete_q tableName key = mycall (DirtyDeleteQ tableName key)

{-
dirty_delete_ls :: TableName -> ConsumerName -> Process ()
dirty_delete_ls tableName key = mycall (DirtyDeleteLS tableName key)
-}

dirty_write :: TableName -> Meta -> Process ()
dirty_write tableName val = mycall (DirtyWrite tableName val)

dirty_write_q :: TableName -> QEntry -> Process ()
-- dirty_write_q tablename val = mycall (DirtyWriteQ tablename val)
dirty_write_q tablename val = do
  logt $ "dirty_write_q starting"
  res <- mycall (DirtyWriteQ tablename val)
  logt $ "dirty_write_q done"
  return res

dirty_write_q_sid :: ProcessId -> TableName -> QEntry -> Process ()
dirty_write_q_sid sid tablename val = call sid (DirtyWriteQ tablename val)

{-
dirty_write_ls :: TableName -> ConsumerMessage -> Process ()
dirty_write_ls tablename val = mycall (DirtyWriteLS tablename val)
-}

table_info :: TableName -> TableInfoReq -> Process TableInfoRsp
table_info tableName req = mycall (TableInfo tableName req)

wait_for_tables :: [TableName] -> Delay -> Process ()
wait_for_tables tables delay = mycall (WaitForTables tables delay)

log_state :: Process ()
log_state = mycall (LogState)

-- | Start a Queue server
startHroqMnesia :: EKG.Server -> Process ProcessId
startHroqMnesia initParams = do
  let server = serverDefinition
  -- sid <- spawnLocal $ start initParams initFunc server >> return ()
  sid <- spawnLocal $ serve initParams initFunc server >> return ()
  register hroqMnesiaName sid
  return sid

-- init callback
initFunc :: InitHandler EKG.Server State
initFunc ekg = do
  let s = (MnesiaState Map.empty Map.empty Map.empty ekg)
  ems <- liftIO $ Exception.try $ decodeFileSchema (tableNameToFileName schemaTable)
  logm $ "HroqMnesia.initFunc:ems=" ++ (show ems)
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

mycall ::
  (Typeable b, Typeable a, Binary b, Binary a)
  => a -> Process b
mycall op = do
  sid <- getSid
  call sid op

-- ---------------------------------------------------------------------

data TableStorage = DiscOnlyCopies
                  | DiscCopies
                  | RamCopies
                  | StorageNone
                  deriving (Show,Ord,Eq,Generic)

instance Binary TableStorage where

-- ---------------------------------------------------------------------

data RecordType = RecordTypeMeta
                | RecordTypeQueueEntry
                | RecordTypeConsumerLocal
                deriving (Show,Generic)

instance Binary RecordType where

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
        -- , handleCall handleDirtyReadLS
        , handleCall handleDirtyDeleteQ
        -- , handleCall handleDirtyDeleteLS
        , handleCall handleDirtyWrite
        , handleCall handleDirtyWriteQ
        -- , handleCall handleDirtyWriteLS
        , handleCall handleTableInfo
        , handleCall handleWaitForTables

        -- * Debug routines
        , handleCall handleLogState
        ]
    , infoHandlers =
        [
        -- handleInfo_ (\(ProcessMonitorNotification _ _ r) -> logm $ show r >> continue_)
         handleInfo (\dict (ProcessMonitorNotification _ _ r) -> do {logm $ "HroqMnesia:" ++ show r; continue dict })
        ]
     , timeoutHandler = \_ _ -> stop $ ExitOther "timeout az"
     , shutdownHandler = \_ reason -> do { logm $ "HroqMnesia terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State

-- ---------------------------------------------------------------------
-- Handlers

handleChangeTableCopyType :: State -> ChangeTableCopyType -> Process (ProcessReply () State)
handleChangeTableCopyType s (ChangeTableCopyType tableName storage) = do
    logt $ "handleChangeTableCopyType starting"
    s' <- do_change_table_copy_type s tableName storage
    logt $ "handleChangeTableCopyType done"
    reply () s'

handleCreateTable :: State -> CreateTable -> Process (ProcessReply () State)
handleCreateTable s (CreateTable storage tableName recordType) = do
    logt $ "handleCreateTable starting"
    s' <- do_create_table s storage tableName recordType
    logt $ "handleCreateTable done"
    reply () s'

handleDeleteTable :: State -> DeleteTable -> Process (ProcessReply () State)
handleDeleteTable s (DeleteTable tableName) = do
    logt $ "handleDeleteTable starting"
    s' <- do_delete_table s tableName
    logt $ "handleDeleteTable done"
    reply () s'

handleCreateSchema :: State -> CreateSchema -> Process (ProcessReply () State)
handleCreateSchema s (CreateSchema) = do
    logt $ "handleCreateSchema starting"
    s' <- do_create_schema s
    logt $ "handleCreateSchema done"
    reply () s'

handleDeleteSchema :: State -> DeleteSchema -> Process (ProcessReply () State)
handleDeleteSchema s _ = do
    logt $ "handleDeleteSchema starting"
    s' <- do_delete_schema s
    logt $ "handleDeleteSchema done"
    reply () s'

handleDirtyAllKeys :: State -> DirtyAllKeys -> Process (ProcessReply [QKey] State)
handleDirtyAllKeys s (DirtyAllKeys tableName) = do
    logt $ "handleDirtyAllKeys starting"
    res <- do_dirty_all_keys tableName
    logt $ "handleDirtyAllKeys done"
    reply res s

handleDirtyRead :: State -> DirtyRead -> Process (ProcessReply (Maybe Meta) State)
handleDirtyRead s (DirtyRead tableName key) = do
    logt $ "handleDirtyRead starting"
    res <- do_dirty_read tableName key
    logt $ "handleDirtyRead done"
    reply res s

handleDirtyReadQ :: State -> DirtyReadQ -> Process (ProcessReply (Maybe QEntry) State)
handleDirtyReadQ s (DirtyReadQ tableName key) = do
    logt $ "handleDirtyReadQ starting"
    res <- do_dirty_read_q tableName key
    logt $ "handleDirtyReadQ done"
    reply res s

{-
handleDirtyReadLS :: State -> DirtyReadLS -> Process (ProcessReply (Maybe ConsumerMessage) State)
handleDirtyReadLS s (DirtyReadLS consumerName key) = do
    logt $ "handleDirtyReadLS starting"
    res <- do_dirty_read_ls consumerName key
    logt $ "handleDirtyReadLS done"
    reply res s
-}

handleDirtyDeleteQ :: State -> DirtyDeleteQ -> Process (ProcessReply () State)
handleDirtyDeleteQ s (DirtyDeleteQ tableName key) = do
    logt $ "handleDirtyDeleteQ starting"
    s' <- do_dirty_delete_q s tableName key
    logt $ "handleDirtyDeleteQ done"
    reply () s'

{-
handleDirtyDeleteLS :: State -> DirtyDeleteLS -> Process (ProcessReply () State)
handleDirtyDeleteLS s (DirtyDeleteLS tableName key) = do
    logt $ "handleDirtyDeleteLS starting"
    s' <- do_dirty_delete_ls s tableName key
    logt $ "handleDirtyDeleteLS done"
    reply () s'
-}

handleDirtyWrite :: State -> DirtyWrite -> Process (ProcessReply () State)
handleDirtyWrite s (DirtyWrite tableName val) = do
    logt $ "handleDirtyWrite starting"
    s' <- do_dirty_write s tableName val
    logt $ "handleDirtyWrite done"
    reply () s'

handleDirtyWriteQ :: State -> DirtyWriteQ -> Process (ProcessReply () State)
handleDirtyWriteQ s (DirtyWriteQ tableName val) = do
    logt $ "handleDirtyWriteQ starting"
    s' <- do_dirty_write_q s tableName val
    logt $ "handleDirtyWriteQ done"
    reply () s'

{-
handleDirtyWriteLS :: State -> DirtyWriteLS -> Process (ProcessReply () State)
handleDirtyWriteLS s (DirtyWriteLS tableName val) = do
    logt $ "handleDirtyWriteLs starting"
    s' <- do_dirty_write_ls s tableName val
    logt $ "handleDirtyWriteLS done"
    reply () s'
-}

handleTableInfo :: State -> TableInfo -> Process (ProcessReply TableInfoRsp State)
handleTableInfo s (TableInfo tableName req) = do
    logt $ "handleTableInfo starting"
    (s',res) <- do_table_info s tableName req
    logt $ "handleTableInfo done"
    reply res s'

handleWaitForTables :: State -> WaitForTables -> Process (ProcessReply () State)
handleWaitForTables s (WaitForTables tables delay) = do
    logt $ "handleWaitForTables starting"
    s' <- do_wait_for_tables s tables delay
    logt $ "handleWaitForTables done"
    reply () s'

handleLogState :: State -> LogState -> Process (ProcessReply () State)
handleLogState s _ = do
  logm $ "HroqMnesia:current state:" ++ (show s)
  reply () s

-- ---------------------------------------------------------------------

do_change_table_copy_type :: State -> TableName -> TableStorage -> Process State
do_change_table_copy_type s bucket DiscOnlyCopies = do
  -- logm $ "HroqMnesia.change_table_copy_type to DiscOnlyCopies for:" ++ (show (bucket))
  let (TableMeta _ _st rt) = getMetaForTableDefault s bucket
  -- logm $ "HroqMnesia.change_table_copy_type:(st,rt)=" ++ (show (st,rt))
  let s' = case rt of
            RecordTypeMeta       -> s {sRamMeta = Map.delete bucket (sRamMeta s)}
            RecordTypeQueueEntry -> s {sRamQ    = Map.delete bucket (sRamQ s)}
            -- RecordTypeQueueEntry -> s {sRamQ    = strictify $ Map.delete bucket (sRamQ s)}
            _                    -> s
  -- logm $ "HroqMnesia.change_table_copy_type:s'=" ++ (show s')

  let (TableMeta size _ rt2) = getMetaForTableDefault s' bucket
  let s'' = s' { sTableInfo = Map.insert bucket (TableMeta size DiscOnlyCopies rt2) (sTableInfo s')}
  -- logm $ "HroqMnesia.change_table_copy_type:s''=" ++ (show (s''))
  persistTableInfo (sTableInfo s'')
  return s''

do_change_table_copy_type s bucket storageType = do
  logm $ "HroqMnesia.change_table_copy_type undefined for:" ++ (show (bucket,storageType))
  return s

mySyncCheck :: Integer -> Integer -> Integer -> Bool
mySyncCheck _ _ _ = True

-- ---------------------------------------------------------------------

persistTableInfo :: Map.Map TableName TableMeta -> Process ()
persistTableInfo ti = do
  logt $ "HroqMnesia.persistTableInfo starting for"
  -- logm $ "HroqMnesia.persistTableInfo starting for:" ++ (show ti)
  res <- liftIO $ Exception.try $ defaultWrite (tableNameToFileName schemaTable) (encode ti)
  case res of
    Left (e :: SomeException) -> logm $ "HroqMnesia.persistTableInfo:error:" ++ (show e)
    Right _ -> logt $ "HroqMnesia.persistTableInfo done"

-- ---------------------------------------------------------------------

-- |Record the table details into the meta information.
do_create_table :: State -> TableStorage -> TableName -> RecordType -> Process State
do_create_table s storage name recordType = do
  -- logm $ "HroqMnesia.create_table:" ++ (show (name,storage,recordType))
  -- TODO: check for clash with pre-existing, both in meta/state and
  --       on disk
  let ti' = Map.insert name (TableMeta Nothing storage recordType) (sTableInfo s)
  -- logm $ "HroqMnesia.do_create_table:ti'=" ++ (show ti')

  -- TODO: use an incremental write, rather than full dump
  -- TODO: check result?
  persistTableInfo ti'
  return $ s { sTableInfo = ti' }

-- ---------------------------------------------------------------------

do_delete_table :: State -> TableName -> Process State
do_delete_table s name = do
  logm $ "HroqMnesia.delete_table:" ++ (show name)
  liftIO $ defaultDelete $ tableNameToFileName name
  let ti' = Map.delete name (sTableInfo s)
  return $ s {sTableInfo = ti'}

-- ---------------------------------------------------------------------

-- |Create a new schema
do_create_schema :: State -> Process State
do_create_schema s = do
  logm $ "HroqMnesia.undefined create_schema"
  return s

-- |Delete the schema
do_delete_schema :: State -> Process State
do_delete_schema s = do
  logm $ "HroqMnesia.undefined delete_schema"
  return s

-- ---------------------------------------------------------------------

-- |Write the value.
-- Currently only used for the meta table, which only has one entry in it
do_dirty_write :: State -> TableName -> Meta -> Process State
do_dirty_write s tableName record = do
  -- logm $ "HroqMnesia.dirty_write:" ++ (show (tableName,record))
  liftIO $ defaultWrite (tableNameToFileName tableName) (encode record)
  let s' = insertEntryMeta s tableName record
  return s'

do_dirty_write_q ::
   State -> TableName -> QEntry -> Process State
do_dirty_write_q s tableName record = do
  -- logm $ "HroqMnesia.dirty_write_q:" ++ (show (tableName,record))

  -- logm $ "HroqMnesia.not doing physical write" -- ++AZ++
  -- liftIO $ defaultAppend (tableNameToFileName tableName) (encode record)
  hsid <- hroq_handle_pool_server_pid -- TODO: cache this in State
  append hsid (tableNameToFileName tableName) (encode record)

  let s' = insertEntryQ s tableName record
  return s'

{-
do_dirty_write_ls ::
   State -> TableName -> ConsumerMessage -> Process State
do_dirty_write_ls s tableName record = do
  logm $ "dirty_write_ls:" ++ (show (tableName,record))

  liftIO $ defaultAppend (tableNameToFileName tableName) (encode record)

  let s' = insertEntryLS s tableName record
  return s'
-}

-- ---------------------------------------------------------------------

do_dirty_read :: TableName -> MetaKey -> Process (Maybe Meta)
do_dirty_read tableName keyVal = do
  -- logm $ "HroqMnesia.dirty_read:" ++ (show (tableName)) -- ,keyVal))
  ems <- liftIO $ Exception.try $ decodeFileMeta (tableNameToFileName tableName)
  case ems of
    Left (e::IOError) -> do
      logm $ "HroqMnesia.do_dirty_read e " ++ (show keyVal) ++ ":" ++ (show e)
      return Nothing
    Right ms -> do
      let ms' = filter (\(MAllBuckets key _ _) -> key == keyVal) ms
      -- logm $ "HroqMnesia.do_dirty_read ms' " ++ (show keyVal) ++ ":" ++ (show ms')
      if ms' == [] then return Nothing
                   else return $ Just (head ms')

-- ---------------------------------------------------------------------

do_dirty_read_q :: TableName -> QKey -> Process (Maybe QEntry)
do_dirty_read_q tableName keyVal = do
  -- logm $ "HroqMnesia.dirty_read_q:" ++ (show (tableName)) -- ,keyVal))
  -- TODO: check if this is in the RAM cache first
  ems <- liftIO $ Exception.try $ decodeFileQEntry (tableNameToFileName tableName)
  case ems of
    Left (e::IOError) -> do
      logm $ "HroqMnesia.do_dirty_read_q e " ++ (show keyVal) ++ ":" ++ (show e)
      return Nothing
    Right ms -> do
      let ms' = filter (\(QE key _) -> key == keyVal) ms
      -- logm $ "HroqMnesia.do_dirty_read_q ms' " ++ (show keyVal) ++ ":" ++ (show ms')
      if ms' == [] then return Nothing
                   else return $ Just (head ms')

-- ---------------------------------------------------------------------

{-
do_dirty_read_ls :: TableName -> ConsumerName -> Process (Maybe ConsumerMessage)
do_dirty_read_ls tableName keyVal = do
  logm $ "dirty_read_ls:" ++ (show (tableName)) -- ,keyVal))
  -- TODO: check if this is in the RAM cache first
  ems <- liftIO $ Exception.try $ decodeFileConsumerMessage (tableNameToFileName tableName)
  case ems of
    Left (e::IOError) -> do
      logm $ "do_dirty_read_ls e " ++ (show keyVal) ++ ":" ++ (show e)
      return Nothing
    Right ms -> do
      let ms' = filter (\(CM _ key _ _ _) -> key == keyVal) ms
      logm $ "do_dirty_read_ls ms' " ++ (show keyVal) ++ ":" ++ (show ms')
      if ms' == [] then return Nothing
                   else return $ Just (head ms')
-}

-- ---------------------------------------------------------------------

do_dirty_delete_q :: State -> TableName -> QKey -> Process (State)
do_dirty_delete_q s tableName keyVal = do
  logm $ "HroqMnesia.dirty_delete_q:" ++ (show (tableName,keyVal)) -- ,keyVal))
  -- if we have a ramQ version, delete it from there and then dump the
  -- whole thing to disk.
  vals <- if Map.member tableName (sRamQ s)
             then return $ (sRamQ s) Map.! tableName
             else do qes <- loadTableIntoRamQ tableName
                     return qes
  let vals' = filter (\(QE key _) -> key /= keyVal) vals
  let (TableMeta _s storage _type) = getMetaForTableDefault s tableName
  let s' = updateTableInfoQ s tableName storage vals'

  case vals' of
    [] -> do
            liftIO $ removeIfExists (tableNameToFileName tableName)
            return ()
    _ -> do liftIO $ defaultWrite (tableNameToFileName tableName) (encode $ head vals')
            mapM_ (\v -> liftIO $ defaultAppend (tableNameToFileName tableName) (encode v)) $ tail vals'
  -- logm $ "HroqMnesia.dirty_delete_q:done"
  return s'

-- ---------------------------------------------------------------------

loadTableIntoRamQ :: TableName -> Process [QEntry]
loadTableIntoRamQ table = do
  logm $ "HroqMnesia.loadTableIntoRamQ:" ++ (show table)
  ems <- liftIO $ Exception.try $ decodeFileQEntry (tableNameToFileName table)
  logm $ "HroqMnesia.loadTableIntoRamQ:ems=" ++ (show ems)
  case ems of
    Left (e :: IOError) -> do
      logm $ "HroqMnesia.loadTableIntoRamQ:error=" ++ (show e)
      return []
    Right ms -> return $ ms


-- ---------------------------------------------------------------------

{-
do_dirty_delete_ls :: State -> TableName -> ConsumerName -> Process (State)
do_dirty_delete_ls s tableName keyVal = do
  logm $ "dirty_delete_ls:" ++ (show (tableName,keyVal)) -- ,keyVal))
  -- if we have a ramQ version, delete it from there and then dump the
  -- whole thing to disk.
{-
  vals <- if Map.member tableName (sRamQ s)
             then return $ (sRamQ s) Map.! tableName
             else do qes <- loadTableIntoRamQ tableName
                     return qes
-}
  vals <- loadTableIntoRamLS tableName
  let vals' = filter (\(CM _ key _ _ _) -> key /= keyVal) vals

  let (TableMeta _s storage _type) = getMetaForTableDefault s tableName
  let s' = updateTableInfoLS s tableName storage vals'

  case vals' of
    [] -> return ()
    _ -> do liftIO $ defaultWrite (tableNameToFileName tableName) (encode $ head vals')
            mapM_ (\v -> liftIO $ defaultAppend (tableNameToFileName tableName) (encode v)) $ tail vals'

  return s'
-}

-- ---------------------------------------------------------------------

loadTableIntoRamLS :: TableName -> Process [ConsumerMessage]
loadTableIntoRamLS table = do
  logm $ "HroqMnesia.loadTableIntoRamLS:" ++ (show table)
  ems <- liftIO $ Exception.try $ decodeFileConsumerMessage (tableNameToFileName table)
  logm $ "HroqMnesia.loadTableIntoRamLS:ems=" ++ (show ems)
  case ems of
    Left (e :: IOError) -> do
      logm $ "HroqMnesia.loadTableIntoRamLs:error=" ++ (show e)
      return []
    Right ms -> return $ ms


-- ---------------------------------------------------------------------

do_dirty_all_keys :: TableName -> Process [QKey]
do_dirty_all_keys tableName = do
  logm $ "HroqMnesia.unimplemented dirty_all_keys:" ++ (show tableName)
  return []

-- ---------------------------------------------------------------------

do_wait_for_tables :: State -> [TableName] -> Delay -> Process State
do_wait_for_tables s tables _maxWait = do
  logm $ "HroqMnesia.wait_for_tables:sTableInfo=" ++ (show (sTableInfo s))
  s' <- foldM waitForTable s tables
  logm $ "HroqMnesia.do_wait_for_tables:done: sTableInfo'=" ++ (show (sTableInfo s'))
  return s'


waitForTable :: State -> TableName -> Process State
waitForTable s table = do
      -- TODO: load the table info into the meta zone
      logm $ "HroqMnesia.waitForTable:" ++ (show table)
      let mm = getMetaForTable s table
      logm $ "HroqMnesia.waitForTable:me=" ++ (show mm)
      when (isNothing mm) $ logm $ "HroqMnesia.waitForTable:mm=Nothing,s=" ++ (show s)
      let (TableMeta _ms storage recordType) = fromMaybe (TableMeta Nothing StorageNone RecordTypeMeta) mm

      exists  <- queueExists table
      logm $ "HroqMnesia.wait_for_tables:(table,exists,recordType)=" ++ (show (table,exists,recordType))
      res <- case exists of
        True -> do
          newS <- do
            case recordType of
              RecordTypeMeta       -> do
                logm $ "HroqMnesia.waitForTable: RecordTypeMeta"
                -- TODO: refactor this and next case into one fn
                ems <- liftIO $ Exception.try $ decodeFileMeta (tableNameToFileName table)
                logm $ "HroqMnesia.wait_for_tables:ems=" ++ (show ems)
                case ems of
                  Left (e :: IOError) -> return s
                  Right ms -> return $ updateTableInfoMeta s table storage ms

              RecordTypeQueueEntry -> do
                logm $ "HroqMnesia.waitForTable: RecordTypeQueueEntry"
                ems <- liftIO $ Exception.try $ decodeFileQEntry (tableNameToFileName table)
                logm $ "HroqMnesia.wait_for_tables:ems=" ++ (show ems)
                case ems of
                  Left (e :: IOError) -> return s
                  Right ms -> return $ updateTableInfoQ s table storage ms
              _ -> do
                logm "HroqMnesia.do_wait_for_tables:unknown record type"
                return s

          return newS
        False -> do
          logm $ "HroqMnesia.wait_for_tables:False, done:" ++ (show table)
          case recordType of
            RecordTypeMeta       -> return $ updateTableInfoMeta s table StorageNone []
            RecordTypeQueueEntry -> return $ updateTableInfoQ    s table StorageNone []
            _                    -> return s
      logm $ "HroqMnesia.waitForTable done:res=" ++ (show res)
      return res
      -- return (table, TableMeta Nothing StorageNone RecordTypeMeta)

-- ---------------------------------------------------------------------

decodeFileSchema :: FilePath -> IO [Map.Map TableName TableMeta]
decodeFileSchema = decodeFileBinaryList

decodeFileMeta :: FilePath -> IO [Meta]
decodeFileMeta = decodeFileBinaryList

decodeFileQEntry :: FilePath -> IO [QEntry]
decodeFileQEntry = decodeFileBinaryList

decodeFileConsumerMessage :: FilePath -> IO [ConsumerMessage]
decodeFileConsumerMessage = decodeFileBinaryList

-- ---------------------------------------------------------------------

decodeFileBinaryList :: (Binary a) => FilePath -> IO [a]
decodeFileBinaryList filename = do
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
      logm $ "HroqMnesia.getLengthOfFile:(er)=" ++ (show (er))
      return 0
    Right xs -> return $ fromIntegral $ length xs

-- ---------------------------------------------------------------------

data TableInfoReq = TableInfoSize
                  | TableInfoStorageType
                    deriving (Show,Generic)
instance Binary TableInfoReq where

data TableInfoRsp = TISize !Integer
                  | TIStorageType !TableStorage
                  | TIError
                    deriving (Show,Typeable,Generic)

instance Binary TableInfoRsp where

-- ---------------------------------------------------------------------

do_table_info :: State -> TableName -> TableInfoReq -> Process (State,TableInfoRsp)
do_table_info s tableName TableInfoSize        = do
  -- logm $ "HroqMnesia.table_info:TableInfoSize" ++ (show (tableName))
  getBucketSize s tableName
do_table_info s tableName TableInfoStorageType = do
  -- logm $ "HroqMnesia.table_info:TableInfoStorageType" ++ (show (tableName))
  getStorageType s tableName
do_table_info s tableName infoReq = do
  -- logm $ "HroqMnesia.table_info undefined:" ++ (show (tableName,infoReq))
  return (s,TIError)

-- ---------------------------------------------------------------------

getBucketSize :: State -> TableName -> Process (State,TableInfoRsp)
getBucketSize s tableName = do
  -- logm $ "HroqMnesia.getBucketSize " ++ (show tableName)
  let mm = getMetaForTable s tableName
  s' <- if (isNothing mm) then waitForTable s tableName
                          else return s
  let mm' = getMetaForTable s' tableName
  case mm' of
    Nothing -> do
      -- logm $ "  getBucketSize(nonexist) "
      return $ (s',TISize 0)
    Just (TableMeta msize _ _) -> do
      -- logm $ "  getBucketSize(exists) " ++ (show (tableName,msize))
      case msize of
        Nothing -> do
          s'' <- waitForTable s' tableName
          let mm'' = getMetaForTableDefault s'' tableName
          logm $ "HroqMnesia.getBucketSize:mm''=" ++ (show mm'')
          return (s'', TISize $ fromMaybe 0 (tSize mm''))
        Just size -> return (s',TISize size)

getStorageType :: State -> TableName -> Process (State,TableInfoRsp)
getStorageType s tableName = do
  e  <- queueExists tableName
  let storage = if e then DiscCopies else StorageNone
  logm $ "HroqMnesia.getStorageType:" ++ (show (tableName,storage))
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


-- ---------------------------------------------------------------------


-- From http://stackoverflow.com/questions/8502201/remove-file-if-it-exists-in-haskell
removeIfExists :: FilePath -> IO ()
removeIfExists fileName = removeFile fileName `Exception.catch` handleExists
  where handleExists e
          | isDoesNotExistError e = return ()
          | otherwise = throwIO e
