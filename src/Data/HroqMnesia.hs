{-# LANGUAGE DeriveDataTypeable #-}
{-# Language ScopedTypeVariables #-}
module Data.HroqMnesia
  (
    TableStorage(..)
  -- , Storable(..)
  , change_table_copy_type
  , create_table
  , delete_table
  , dirty_write
  , dirty_write_q
  , dirty_read
  , dirty_all_keys
  , wait_for_tables

  , RecordType(..)
  , TableInfoReq(..)
  , TableInfoRsp(..)
  , table_info

  , startHroqMnesia

  -- * debug
  , queueExists
  , get_state
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
import Data.DeriveTH
import Data.Hroq
import Data.HroqLogger
import Data.IORef
import Data.List(elemIndices,isInfixOf)
import Data.Maybe(fromJust,fromMaybe)
import Data.RefSerialize
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import System.Directory
import System.IO
import System.IO.Error
import System.IO.Unsafe
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Map as Map

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

--  , dirty_all_keys
data DirtyAllKeys = DirtyAllKeys !TableName
                    deriving (Typeable, Show)

--  , dirty_read
data DirtyRead = DirtyRead !TableName !Meta
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

instance Binary DirtyAllKeys where
  put (DirtyAllKeys tn) = put tn
  get = liftM DirtyAllKeys get

instance Binary DirtyRead where
  put (DirtyRead tn key) = put tn >> put key
  get = liftM2 DirtyRead get get

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
                      | HMResTimeout [TableName]
                      | HMResError String
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
  { tSize       :: Maybe Integer -- ^Size of stored table, if known
  , tStorage    :: TableStorage
  , tRecordType :: RecordType
  } deriving (Show,Typeable)

data State = MnesiaState
  {
  sTableInfo :: Map.Map TableName TableMeta
  } deriving (Show,Typeable)

instance Binary TableMeta where
  put (TableMeta size storage typ) = put size >> put storage >> put typ
  get = liftM3 TableMeta get get get

instance Binary State where
  put (MnesiaState ti) = put ti
  get = liftM MnesiaState get

getMetaForTable :: State -> TableName -> Maybe (TableMeta)
getMetaForTable s tableName = Map.lookup tableName (sTableInfo s)

getMetaForTableDefault s tableName =
  fromMaybe (TableMeta Nothing StorageNone RecordTypeQueueEntry)
           $ getMetaForTable s tableName

-- ---------------------------------------------------------------------
{-
-- may need this, to store the meta information permanently
data MnesiaSchema = Schema
  { sTables = Map.Map TableName
  }
-}

--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------

change_table_copy_type :: TableName -> TableStorage -> Process ()
change_table_copy_type tableName storage = mycall (ChangeTableCopyType tableName storage)

create_table :: TableStorage -> TableName -> RecordType -> Process ()
create_table storage tableName recordType = mycall (CreateTable storage tableName recordType)

delete_table :: TableName -> Process ()
delete_table tableName = mycall (DeleteTable tableName)

dirty_all_keys :: TableName -> Process [QKey]
dirty_all_keys tableName = mycall (DirtyAllKeys tableName)

dirty_read :: TableName -> Meta -> Process (Maybe Meta)
dirty_read tableName key = mycall (DirtyRead tableName key)

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
  return $ InitOk (MnesiaState Map.empty) Infinity

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
                  | StorageNone
                  deriving (Show,Ord,Eq)

instance Binary TableStorage where
  put DiscOnlyCopies = put (1::Word8)
  put DiscCopies     = put (2::Word8)
  put StorageNone    = put (3::Word8)

  get = do
    v <- get
    case v of
      (1::Word8) -> return DiscOnlyCopies
      (2::Word8) -> return DiscCopies
      (3::Word8) -> return StorageNone

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
        , handleCall handleDirtyAllKeys
        , handleCall handleDirtyRead
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
    } :: ProcessDefinition State

-- ---------------------------------------------------------------------
-- Handlers

handleChangeTableCopyType :: State -> ChangeTableCopyType -> Process (ProcessReply State ())
handleChangeTableCopyType s (ChangeTableCopyType tableName storage) = do
    do_change_table_copy_type tableName storage
    reply () s

handleCreateTable :: State -> CreateTable -> Process (ProcessReply State ())
handleCreateTable s (CreateTable storage tableName recordType) = do
    s' <- do_create_table s storage tableName recordType
    reply () s'

handleDeleteTable :: State -> DeleteTable -> Process (ProcessReply State ())
handleDeleteTable s (DeleteTable tableName) = do
    do_delete_table tableName
    reply () s

handleDirtyAllKeys :: State -> DirtyAllKeys -> Process (ProcessReply State [QKey])
handleDirtyAllKeys s (DirtyAllKeys tableName) = do
    res <- do_dirty_all_keys tableName
    reply res s

handleDirtyRead :: State -> DirtyRead -> Process (ProcessReply State (Maybe Meta))
handleDirtyRead s (DirtyRead tableName key) = do
    res <- do_dirty_read tableName key
    reply res s

handleDirtyWrite :: State -> DirtyWrite -> Process (ProcessReply State ())
handleDirtyWrite s (DirtyWrite tableName val) = do
    do_dirty_write tableName val
    reply () s

handleDirtyWriteQ :: State -> DirtyWriteQ -> Process (ProcessReply State ())
handleDirtyWriteQ s (DirtyWriteQ tableName val) = do
    s' <- do_dirty_write_q s tableName val
    reply () s'

handleTableInfo :: State -> TableInfo -> Process (ProcessReply State TableInfoRsp)
handleTableInfo s (TableInfo tableName req) = do
    res <- do_table_info s tableName req
    reply res s

handleWaitForTables :: State -> WaitForTables -> Process (ProcessReply State ())
handleWaitForTables s (WaitForTables tables delay) = do
    s' <- do_wait_for_tables s tables delay
    reply () s'

handleGetState :: State -> GetState -> Process (ProcessReply State State)
handleGetState s _ = reply s s

-- ---------------------------------------------------------------------


{-
data SKey = SK String

data Storable a = Store SKey a
                 deriving (Show,Read,Typeable)
-}

do_change_table_copy_type :: TableName -> TableStorage -> Process ()
do_change_table_copy_type bucket DiscOnlyCopies = do
  logm $ "change_table_copy_type to DiscOnlyCopies (undefined) for:" ++ (show (bucket))

do_change_table_copy_type bucket storageType = do
  logm $ "change_table_copy_type undefined for:" ++ (show (bucket,storageType))

mySyncCheck :: Integer -> Integer -> Integer -> Bool
mySyncCheck _ _ _ = True

-- ---------------------------------------------------------------------

-- |Record the table details into the meta information.
-- TODO: store this in the schema table too.
do_create_table :: State -> TableStorage -> TableName -> RecordType -> Process State
do_create_table s storage name recordType = do
  logm $ "create_table:" ++ (show (name,storage,recordType))
  -- TODO: check for clash with pre-existing, both in meta/state and
  --       on disk
  let ti' = Map.insert name (TableMeta Nothing storage recordType) (sTableInfo s)
  return $ s { sTableInfo = ti' }

-- ---------------------------------------------------------------------

do_delete_table :: TableName -> Process ()
do_delete_table name = do
  logm $ "delete_table:" ++ (show name)
  liftIO $ defaultDelete $ tableNameToFileName name

-- ---------------------------------------------------------------------

-- |Write the value.
-- Currently only used for the meta table, which only has one entry in it
do_dirty_write :: TableName -> Meta -> Process ()
do_dirty_write tableName record = do
  logm $ "dirty_write:" ++ (show (tableName,record))
  liftIO $ defaultWrite (tableNameToFileName tableName) (encode record)
  return ()

do_dirty_write_q ::
   State -> TableName -> QEntry -> Process State
do_dirty_write_q s tableName record = do
  logm $ "dirty_write:" ++ (show (tableName,record))
  liftIO $ defaultAppend (tableNameToFileName tableName) (encode record)

  -- Add one to the Q size
  -- TODO: harvest this
  let curr = getMetaForTableDefault s tableName
  let cnt = (fromMaybe 0 (tSize curr)) + 1
  let ti' = Map.insert tableName (curr { tSize = Just cnt }) (sTableInfo s)

  return $ s {sTableInfo = ti'}

-- ---------------------------------------------------------------------

do_dirty_read :: (Binary c)
  => TableName -> b -> Process (Maybe c)
do_dirty_read tableName keyVal = do
  logm $ "dirty_read:" ++ (show (tableName)) -- ,keyVal))
  return Nothing

-- ---------------------------------------------------------------------

do_dirty_all_keys :: TableName -> Process [QKey]
do_dirty_all_keys tableName = do
  logm $ "dirty_all_keys:" ++ (show tableName)
  return []

-- ---------------------------------------------------------------------

do_wait_for_tables :: State -> [TableName] -> Delay -> Process State
do_wait_for_tables s tables _maxWait = do
  logm $ "wait_for_tables"
  r <- mapM (waitForTable s ) tables
  -- r <- waitForTable s (head tables)
  logm $ "do_wait_for_tables:r=" ++ (show r)
  -- logm $ "do_wait_for_tables:blah"
  return s

  -- where
waitForTable :: State -> TableName -> Process (TableName,TableMeta)
waitForTable s table = do
      logm $ "waitForTable:" ++ (show table)
      let mm = getMetaForTable s table
      logm $ "waitForTable:me=" ++ (show mm)
      let (TableMeta _ms storage recordType) = fromMaybe (TableMeta Nothing StorageNone RecordTypeMeta) mm
      exists  <- queueExists table
      logm $ "wait_for_tables:(table,exists)=" ++ (show (table,exists))
      case exists of
        True -> do
          numRecords <-
            case recordType of
              RecordTypeMeta       -> do
                ms <- liftIO ((decodeFile (tableNameToFileName table)) :: IO [Meta])
                logm $ "wait_for_tables:ms=" ++ (show ms)
                return $ fromIntegral $length ms
              RecordTypeQueueEntry -> do
                qs <- liftIO ((decodeFile (tableNameToFileName table)) :: IO [QEntry])
                logm $ "wait_for_tables:qs=" ++ (show qs)
                return $ fromIntegral $length qs
              _ -> do logm "do_wait_for_tables:unknown record type"
                      return 0
          return (table,TableMeta (Just numRecords) storage recordType)
        False -> do
          logm $ "wait_for_tables:False, done:" ++ (show table)
          return (table,TableMeta Nothing StorageNone recordType)
      -- logm $ "waitForTable done:res=" ++ (show res)
      -- return res
      -- return (table, TableMeta Nothing StorageNone RecordTypeMeta)

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

do_table_info :: State -> TableName -> TableInfoReq -> Process TableInfoRsp
do_table_info s tableName TableInfoSize        = do
  -- logm $ "table_info:TableInfoSize" ++ (show (tableName))
  getBucketSize s tableName
do_table_info s tableName TableInfoStorageType = do
  -- logm $ "table_info:TableInfoStorageType" ++ (show (tableName))
  getStorageType tableName
do_table_info s tableName infoReq = do
  -- logm $ "table_info undefined:" ++ (show (tableName,infoReq))
  return TIError

-- ---------------------------------------------------------------------

getBucketSize :: State -> TableName -> Process TableInfoRsp
getBucketSize s tableName = do
  logm $ "getBucketSize " ++ (show tableName)
  let mm = getMetaForTable s tableName
  case mm of
    Nothing -> do
      logm $ "  getBucketSize(nonexist) "
      return $ TISize 0
    Just (TableMeta msize _ _) -> do
      logm $ "  getBucketSize(exists) " ++ (show (tableName,msize))
      return $ TISize $ fromMaybe 0 msize
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

getStorageType :: TableName -> Process TableInfoRsp
getStorageType tableName = do
  e  <- queueExists tableName
  let storage = if e then DiscCopies else StorageNone
  logm $ "getStorageType:" ++ (show (tableName,storage))
  return $ TIStorageType storage


-- ---------------------------------------------------------------------

tableNameToFileName :: TableName -> FilePath
tableNameToFileName (TN tableName) = directoryPrefix ++ tableName

-- ---------------------------------------------------------------------
-- TCache specific functions used here

queueExists :: TableName -> Process Bool
queueExists tableName = do
    res <- liftIO $ doesFileExist $ tableNameToFileName tableName
    return res

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


