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
  , getQid
  , queueExists
  )
  where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Data.Binary
import Data.DeriveTH
import Data.Hroq
import Data.Persistent.Collection
import Data.RefSerialize
import Data.TCache
import Data.TCache.Defs
import Data.TCache.IResource
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import qualified Data.Map as Map


data TableStorage = DiscOnlyCopies
                  | DiscCopies
                  | StorageNone
                  deriving (Show)

data TableName = TN String
                 deriving (Show,Read,Typeable,Eq)

-- ---------------------------------------------------------------------

{-
data SKey = SK String

data Storable a = Store SKey a
                 deriving (Show,Read,Typeable)
-}

change_table_copy_type :: TableName -> TableStorage -> Process ()
change_table_copy_type bucket storageType = do
  say $ "change_table_copy_type undefined for:" ++ (show (bucket,storageType))

-- ---------------------------------------------------------------------

create_table :: TableStorage -> TableName -> Process ()
create_table storage name = do
  say "create_table undefined"

-- ---------------------------------------------------------------------

delete_table :: TableName -> Process ()
delete_table name = do
  say "create_table undefined"

-- ---------------------------------------------------------------------

-- |Write the value to a TCache Q
-- (as a new entry, no check for prior existence/overwrite)

dirty_write :: (Show b, Typeable b, Serialize b, Indexable b)
   => TableName -> b -> Process ()
dirty_write tableName record = do
  say $ "dirty_write:" ++ (show (tableName,record))
  let qid = getQid tableName
  liftIO $ deleteElem qid record
  say $ "dirty_write deleteElem done:"
  liftIO $ push qid record
  say $ "dirty_write push done:"
  liftIO $ syncCache
  say $ "dirty_write done:" ++ (show (tableName,record))
  return ()

dirty_write_q :: 
   TableName -> QEntry -> Process ()
dirty_write_q tableName record = do
  say $ "dirty_write:" ++ (show (tableName,record))
  let qid = getQueue tableName
  liftIO $ deleteElem qid record
  say $ "dirty_write deleteElem done:"
  liftIO $ push qid record
  say $ "dirty_write push done:"
  liftIO $ syncCache
  say $ "dirty_write done:" ++ (show (tableName,record))
  return ()

-- ---------------------------------------------------------------------

dirty_read ::
  (Show b, Indexable b,
   Typeable c, Serialize c, Indexable c)
  => TableName -> b -> Process (Maybe c)
dirty_read tableName keyVal = do
  say $ "dirty_read:" ++ (show (tableName,keyVal))
  let qid = getQid tableName
  res <- liftIO $ pickElem qid (key keyVal)
  return res

-- ---------------------------------------------------------------------

dirty_all_keys :: TableName -> Process [QKey]
dirty_all_keys tableName = do
  say $ "dirty_all_keys:" ++ (show tableName)
  let qid = getQueue tableName
  res <- liftIO $ pickAll qid
  say $ "  dirty_all_keys:res=" ++ (show res)
  return $ map (\(QE k _) -> k) res

-- ---------------------------------------------------------------------

wait_for_tables :: [TableName] -> Delay -> Process ()
wait_for_tables tables maxWait = do
  say $ "wait_for_tables undefined"


-- ---------------------------------------------------------------------

data TableInfoReq = TableInfoSize
                  | TableInfoStorageType
                    deriving (Show)

data TableInfoRsp = TISize Integer
                  | TIStorageType TableStorage
                  | TIError
                    deriving (Show)

table_info :: TableName -> TableInfoReq -> Process TableInfoRsp
table_info tableName TableInfoSize        = do
  say $ "table_info:TableInfoSize" ++ (show (tableName))
  getBucketSize  tableName
table_info tableName TableInfoStorageType = do
  say $ "table_info:TableInfoStorageType" ++ (show (tableName))
  getStorageType tableName
table_info tableName infoReq = do
  say $ "table_info undefined:" ++ (show (tableName,infoReq))
  return TIError

-- ---------------------------------------------------------------------

getBucketSize :: TableName -> Process TableInfoRsp
getBucketSize tableName = do
  say $ "getBucketSize " ++ (show tableName) 
  let qid = (getQueue tableName) :: RefQueue QEntry
  exists <- queueExists qid
  -- let exists = False
  say $ "getBucketSize exists=" ++ (show exists)

  case exists of
    True -> do
      res <- liftIO $ pickAll qid
      say $ "  getBucketSize(exists) " ++ (show (tableName,res)) 
      return $ TISize (fromIntegral $ length res)
    False -> do
      say $ "  getBucketSize(nonexist) " 
      return $ TISize 0

getStorageType :: TableName -> Process TableInfoRsp
getStorageType tableName = do
  let qid = (getQueue tableName ) :: RefQueue QEntry
  e  <- queueExists qid
  let storage = if e then DiscCopies else StorageNone
  say $ "getStorageType:" ++ (show (tableName,storage))
  return $ TIStorageType storage

-- ---------------------------------------------------------------------
-- TCache specific functions used here

-- | Provide an identification of a specific Q
getQueue :: TableName -> RefQueue QEntry
-- getQueue :: (Typeable b, Serialize b) => TableName -> RefQueue b
getQueue (TN name) = getQRef name

getQid :: (Show a, Typeable b, Serialize b) => a -> RefQueue b
getQid x = getQRef $ show x



queueExists :: (Typeable a, Serialize a)  => RefQueue a -> Process Bool
queueExists tv= do
    say $ "queueExists:" ++ (show tv)
    mdx <- liftIO $ atomically $ readDBRef tv
    say $ "queueExists:mdx done"
    case mdx of
     Nothing -> return False
     Just dx -> return True
