{-# LANGUAGE DeriveDataTypeable #-}
module Data.HroqMnesia
  (
    TableStorage(..)
  , TableName(..)
  -- , Storable(..)
  , change_table_copy_type
  , create_table
  , delete_table
  , dirty_write
  , dirty_write_q
  , dirty_read
  , dirty_all_keys
  , wait_for_tables

  , TableInfoReq(..)
  , TableInfoRsp(..)
  , table_info

  -- * debug
  , queueExists
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
import Control.Monad(when,replicateM)
import Data.Binary
import Data.DeriveTH
import Data.Hroq
import Data.HroqLogger
import Data.IORef
import Data.List(elemIndices,isInfixOf)
import Data.Maybe(fromJust)
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

maxCacheSize :: Int
maxCacheSize = fromIntegral $ maxBucketSizeConst * 3

directoryPrefix :: String
directoryPrefix = ".hroqdata/"


-- ---------------------------------------------------------------------

data TableStorage = DiscOnlyCopies
                  | DiscCopies
                  | StorageNone
                  deriving (Show)

data TableName = TN !String
                 deriving (Show,Read,Typeable,Eq)

instance Binary TableName where
  put (TN s) = put s
  get = do
    s <- get
    return (TN s)

-- ---------------------------------------------------------------------

{-
data SKey = SK String

data Storable a = Store SKey a
                 deriving (Show,Read,Typeable)
-}

change_table_copy_type :: TableName -> TableStorage -> Process ()
change_table_copy_type bucket DiscOnlyCopies = do
  logm $ "change_table_copy_type to DiscOnlyCopies (undefined) for:" ++ (show (bucket))

change_table_copy_type bucket storageType = do
  logm $ "change_table_copy_type undefined for:" ++ (show (bucket,storageType))

mySyncCheck :: Integer -> Integer -> Integer -> Bool
mySyncCheck _ _ _ = True

-- ---------------------------------------------------------------------

create_table :: TableStorage -> TableName -> Process ()
create_table storage name = do
  logm "create_table undefined"

-- ---------------------------------------------------------------------

delete_table :: TableName -> Process ()
delete_table name = do
  logm $ "delete_table:" ++ (show name)
  liftIO $ defaultDelete $ tableNameToFileName name

-- ---------------------------------------------------------------------

-- |Write the value to a TCache Q
-- (as a new entry, no check for prior existence/overwrite)

dirty_write :: (Show b, Typeable b, Serialize b)
   => TableName -> b -> Process ()
dirty_write tableName record = do
  logm $ "dirty_write:" ++ (show (tableName,record))
  return ()

dirty_write_q ::
   TableName -> QEntry -> Process ()
dirty_write_q tableName record = do
  logm $ "dirty_write:" ++ (show (tableName,record))
  liftIO $ defaultAppend (tableNameToFileName tableName) (encode record)
  return ()

-- ---------------------------------------------------------------------

dirty_read ::
  (Show b,
   Typeable c, Serialize c)
  => TableName -> b -> Process (Maybe c)
dirty_read tableName keyVal = do
  logm $ "dirty_read:" ++ (show (tableName,keyVal))
  return Nothing

-- ---------------------------------------------------------------------

dirty_all_keys :: TableName -> Process [QKey]
dirty_all_keys tableName = do
  logm $ "dirty_all_keys:" ++ (show tableName)
  return []

-- ---------------------------------------------------------------------

wait_for_tables :: [TableName] -> Delay -> Process ()
wait_for_tables tables maxWait = do
  logm $ "wait_for_tables undefined"


-- ---------------------------------------------------------------------

data TableInfoReq = TableInfoSize
                  | TableInfoStorageType
                    deriving (Show)

data TableInfoRsp = TISize !Integer
                  | TIStorageType !TableStorage
                  | TIError
                    deriving (Show)

table_info :: TableName -> TableInfoReq -> Process TableInfoRsp
table_info tableName TableInfoSize        = do
  -- logm $ "table_info:TableInfoSize" ++ (show (tableName))
  getBucketSize  tableName
table_info tableName TableInfoStorageType = do
  -- logm $ "table_info:TableInfoStorageType" ++ (show (tableName))
  getStorageType tableName
table_info tableName infoReq = do
  -- logm $ "table_info undefined:" ++ (show (tableName,infoReq))
  return TIError

-- ---------------------------------------------------------------------

getBucketSize :: TableName -> Process TableInfoRsp
getBucketSize tableName = do
  -- logm $ "getBucketSize " ++ (show tableName)
  exists <- queueExists tableName
  -- logm $ "getBucketSize exists=" ++ (show exists)

  case exists of
    True -> do
      -- logm $ "  getBucketSize(exists) " ++ (show (tableName,res))
      return $ TISize 0
    False -> do
      -- logm $ "  getBucketSize(nonexist) "
      return $ TISize 0

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

defaultReadByKey :: FilePath -> IO (Maybe B.ByteString)
defaultReadByKey k = iox   -- !> "defaultReadByKey"
     where
     iox = handle handler $ do
             s <-  readFileStrict  k
             return $ Just   s                                                       -- `debug` ("read "++ filename)

     handler ::  IOError ->  IO (Maybe B.ByteString)
     handler  e
      | isAlreadyInUseError e = defaultReadByKey  k
      | isDoesNotExistError e = return Nothing
      | otherwise= if ("invalid" `isInfixOf` ioeGetErrorString e)
         then
            error $  "readResource: " ++ show e ++ " defPath and/or keyResource are not suitable for a file path"

         else defaultReadByKey  k

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


