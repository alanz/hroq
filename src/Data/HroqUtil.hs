module Data.HroqUtil
  (
    generate_key
  , retry_dirty_write
  )
  where

import Prelude hiding (catch)
import Control.Concurrent
import Control.Exception (SomeException)
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.Async
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Data.Binary
import Data.DeriveTH
import Data.Typeable (Typeable)
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import qualified Data.Map as Map
import Data.Hroq
import Data.HroqMnesia

-- ---------------------------------------------------------------------

generate_key :: Process QKey
generate_key = do
  say "generate_key undefined"
  error "foo"

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
retry_dirty_write :: Int -> TableName -> QEntry -> Process ()
retry_dirty_write n tableName record = do
  catch op handler
  where
    op = dirty_write tableName record
    handler :: SomeException -> Process ()
    handler e = do
      say $ "retry_dirty_write:" ++ (show (tableName, n, record, e))
      liftIO $ threadDelay (100*1000) -- Haskell sleep takes us
      retry_dirty_write (n-1) tableName record

-- ---------------------------------------------------------------------
