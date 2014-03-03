{-# LANGUAGE TemplateHaskell      #-}
{-# LANGUAGE DeriveDataTypeable   #-}
{-# LANGUAGE DeriveGeneric        #-}

-- | A pool of handles
module Data.Concurrent.Queue.Roq.HandlePool
  (
  -- * Starting the server
    hroq_handle_pool_server
  , hroq_handle_pool_server_closure
  , hroq_handle_pool_server_pid

  -- * API
  , append

  -- * Types
  -- , CallbackFun

  -- * Debug
  , ping
  , safeOpenFile

  -- * Remote Table
  , Data.Concurrent.Queue.Roq.HandlePool.__remoteTable

  ) where


import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Serializable()
import Control.Exception hiding (try,catch)
import Data.Binary
import Data.Concurrent.Queue.Roq.Logger
import Data.List
import Data.Typeable (Typeable)
import GHC.Generics
import System.Directory
import System.IO
import System.IO.Error
import qualified Data.ByteString.Lazy.Char8 as B
import qualified Data.Map as Map

--------------------------------------------------------------------------------
-- Types                                                                      --
--------------------------------------------------------------------------------

{-
-define(MONITOR_INTERVAL_MS, 30000).
-}
mONITOR_INTERVAL_MS :: Delay
-- mONITOR_INTERVAL_MS = Delay $ milliSeconds 30000
mONITOR_INTERVAL_MS = Delay $ milliSeconds 9000


------------------------------------------------------------------------
-- Data Types
------------------------------------------------------------------------

-- Call operations

data AppendFile = AppendFile FilePath B.ByteString
                deriving (Show,Typeable,Generic)
instance Binary AppendFile where


data CloseAppend = CloseAppend FilePath
                deriving (Show,Typeable,Generic)
instance Binary CloseAppend where


-- Cast operations

data Ping = Ping
  deriving (Typeable, Generic, Eq, Show)
instance Binary Ping where

-- ---------------------------------------------------------------------

data State = ST { stAppends :: Map.Map FilePath Handle
                }
           deriving (Show)

emptyState :: State
emptyState = ST Map.empty


--------------------------------------------------------------------------------
-- API                                                                        --
--------------------------------------------------------------------------------

hroq_handle_pool_server :: Process ()
hroq_handle_pool_server = do
  logm $ "HroqHandlePool:hroq_handle_pool_server entered"
  start_handle_pool_server

hroq_handle_pool_server_pid :: Process ProcessId
hroq_handle_pool_server_pid = getServerPid

-- ---------------------------------------------------------------------

append :: ProcessId -> FilePath -> B.ByteString -> Process ()
append pid filename val =
  call pid (AppendFile filename val)


closeAppend :: ProcessId -> FilePath -> Process ()
closeAppend pid filename =
  call pid (CloseAppend filename)


ping :: Process ()
ping = do
  pid <- getServerPid
  cast pid Ping


-- ---------------------------------------------------------------------

getServerPid :: Process ProcessId
getServerPid = do
  mpid <- whereis hroqHandlePoolServerProcessName
  case mpid of
    Just pid -> return pid
    Nothing -> do
      logm "HroqHandlePool:getServerPid failed"
      error "HroqHandlePool:blow up"

-- -------------------------------------

hroqHandlePoolServerProcessName :: String
hroqHandlePoolServerProcessName = "HroqHandlePool"

-- ---------------------------------------------------------------------

start_handle_pool_server :: Process ()
start_handle_pool_server = do
  logm $ "HroqHandlePool:start_handle_pool_server entered"

  self <- getSelfPid
  register hroqHandlePoolServerProcessName self
  serve () initFunc serverDefinition
  where initFunc :: InitHandler () State
        initFunc _ = do
          logm $ "HroqHandlePool:start.initFunc"
          return $ InitOk emptyState mONITOR_INTERVAL_MS

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
     apiHandlers = [
          handleCall handleAppendFileCall
        , handleCall handleCloseAppendCall

        -- , handleCast handlePublishConsumerStatsCast
        , handleCast (\s Ping -> do {logm $ "HroqHandlePool:ping"; continue s })

        ]
    , infoHandlers =
        [
        handleInfo handleInfoProcessMonitorNotification
        ]
     , timeoutHandler = handleTimeout
     , shutdownHandler = \_ reason -> do
           { logm $ "HroqHandlePool terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State

-- ---------------------------------------------------------------------
-- Implementation
-- ---------------------------------------------------------------------



-- ---------------------------------------------------------------------

handleAppendFileCall :: State -> AppendFile -> Process (ProcessReply () State)
handleAppendFileCall st (AppendFile fileName val) = do
  -- logm $ "HroqHandlePool.handleAppendFileCall entered for :" ++ show fileName
  (st',h) <- case Map.lookup fileName (stAppends st) of
    Just h -> do
      -- logm $ "HroqHandlePool.handleAppendFileCall re-using handle"
      return (st,h)
    Nothing -> do
      logm $ "HroqHandlePool.handleAppendFileCall opening file:" ++ show fileName
      h <- liftIO $ safeOpenFile fileName AppendMode
      return (st { stAppends = Map.insert fileName h (stAppends st) },h)
  -- logm $ "HroqHandlePool.handleAppendFileCall writing:" ++ show val
  liftIO $ B.hPut h val -- >> hFlush h
  reply () st'

-- ---------------------------------------------------------------------

handleCloseAppendCall :: State -> CloseAppend -> Process (ProcessReply () State)
handleCloseAppendCall st (CloseAppend fileName) = do
  logm $ "HroqHandlePool.handleCloseAppendCall entered for :" ++ show fileName
  st' <- case Map.lookup fileName (stAppends st) of
    Just h -> do
      liftIO $ hClose h
      return st { stAppends = Map.delete fileName (stAppends st) }
    Nothing -> do
      return st
  reply () st'

-- ---------------------------------------------------------------------

-- |Try to open a file in the given mode, creating any required
-- directory structure if necessary
safeOpenFile :: FilePath -> IOMode -> IO Handle
safeOpenFile filename mode = handle handler (openBinaryFile filename mode)
  where
  handler e
    | isDoesNotExistError e = do
        createDirectoryIfMissing True $ take (1+(last $ elemIndices '/' filename)) filename   -- maybe the path does not exist
        safeOpenFile filename mode

    | otherwise= if ("invalid" `isInfixOf` ioeGetErrorString e)
          then
             error  $ "writeResource: " ++ show e ++ " defPath and/or keyResource are not suitable for a file path"
          else do
             hPutStrLn stderr $ "defaultWriteResource:  " ++ show e ++  " in file: " ++ filename ++ " retrying"
             safeOpenFile filename mode



-- ---------------------------------------------------------------------

handleTimeout :: TimeoutHandler State
handleTimeout st currDelay = do
  logm $ "HroqHandlePool:handleTimeout entered"
  mapM_ (\h -> liftIO $ hClose h) $ Map.elems (stAppends st)
  timeoutAfter currDelay (st {stAppends = Map.empty })
  -- timeoutAfter currDelay st

-- ---------------------------------------------------------------------

handleShutdown :: ShutdownHandler State
handleShutdown st reason = do
  logm $ "HroqHandlePool.handleShutdown called for:" ++ show reason
  return ()

-- ---------------------------------------------------------------------

handleInfoProcessMonitorNotification :: State -> ProcessMonitorNotification -> Process (ProcessAction State)
handleInfoProcessMonitorNotification st n@(ProcessMonitorNotification ref _pid _reason) = do
  logm $ "HroqHandlePool:handleInfoProcessMonitorNotification called with: " ++ show n
  let m = stAppends st
{-
  case Map.lookup ref m of
        Just key -> continue st { stMdict = Map.delete ref m
                                , stGdict = Map.delete key g }
        Nothing                -> continue st
-}
  continue st

-- ---------------------------------------------------------------------

-- NOTE: the TH crap has to be a the end, as it can only see the stuff lexically before it in the file


$(remotable [ 'hroq_handle_pool_server
            ])

hroq_handle_pool_server_closure :: Closure (Process ())
hroq_handle_pool_server_closure = ( $(mkStaticClosure 'hroq_handle_pool_server))


-- EOF

