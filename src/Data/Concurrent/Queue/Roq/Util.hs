module Data.Concurrent.Queue.Roq.Util
  (
    generate_key
  , retry_dirty_write
  , retry_dirty_read

  )
  where

import Prelude -- hiding (catch)
import Control.Distributed.Process hiding (call)
import Data.Concurrent.Queue.Roq.Hroq
import Data.Concurrent.Queue.Roq.Mnesia
-- import Data.Time.Clock
import Data.Thyme.Clock
import System.IO.Unsafe

-- ---------------------------------------------------------------------

generate_key :: Process QKey
generate_key = do
  -- k <- liftIO $ getCurrentTime
  let k = unsafePerformIO $ getCurrentTime
  return $ QK (show k)

-- ---------------------------------------------------------------------
{-
retry_dirty_write(0, _, _) ->
    error;
retry_dirty_write(N, TableName, Record) ->
    case catch(mnesia:dirty_write(TableName, Record)) of
    ok ->
        ok;
    {'EXIT', Reason} ->
        ?warn({dirty_write, TableName, N, Record, Reason}),
        timer:sleep(100),
        retry_dirty_write(N-1, TableName, Record)
    end.
-}
-- retry_dirty_write :: Int -> TableName -> QEntry -> Process ()
retry_dirty_write n tableName record = do
  dirty_write tableName record
{-
  catch op handler
  where
    op = dirty_write tableName record
    handler :: SomeException -> Process ()
    handler e = do
      logm $ "retry_dirty_write:" ++ (show (tableName, n, record, e))
      liftIO $ threadDelay (100*1000) -- Haskell sleep takes us
      retry_dirty_write (n-1) tableName record
-}

-- ---------------------------------------------------------------------
{-
retry_dirty_read(0, _, _) ->
    error;
retry_dirty_read(N, TableName, Key) ->
    case catch(mnesia:dirty_read(TableName, Key)) of
    {'EXIT', Reason} ->
        ?warn({dirty_read, TableName, N, Key, Reason}),
        retry_dirty_read(N-1, TableName, Key);
    Data ->
        {ok, Data}
    end.
-}
{-
retry_dirty_read :: (Show a,Indexable a, Typeable b) -- , Serialize b) 
  => Int -> TableName -> a -> Process [b]
retry_dirty_read 0 tableName key = do
  logm $ "retry_dirty_read:giving up" ++ (show (0,tableName,key))
  return [] -- TODO: throw an exception, blow up, etc
retry_dirty_read n tableName key = do
  logm $ "retry_dirty_read:" ++ (show (n,tableName,key))
  catch op handler
  where
    op = dirty_read tableName key :: [b]
    handler :: (Typeable b) => SomeException -> Process [b]
    handler e = do
      logm $ "retry_dirty_read:exception" ++ (show (n,tableName,key,e))
      liftIO $ threadDelay (100*1000) -- Haskell sleep takes us
      retry_dirty_read (n - 1) tableName key
-}
retry_dirty_read ::
  Integer -> TableName -> MetaKey -> Process (Maybe Meta)
retry_dirty_read n tableName key = do
  -- logm $ "retry_dirty_read:" ++ (show (n,tableName,key))
  dirty_read tableName key




