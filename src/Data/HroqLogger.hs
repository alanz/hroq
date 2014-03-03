{-# LANGUAGE ScopedTypeVariables #-}

module Data.HroqLogger
  (
    startLoggerProcess

  , logm
  , logt
  )
  where

-- |Provide a general purpose interface to a logging subsystem.
-- It is based partly on the say infrastructure in
-- Control.Distributed.Process, but using a decent logger.

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node
-- import Data.Time.Clock (getCurrentTime)
import Data.Thyme.Clock (getCurrentTime)
import Data.Thyme.Format (formatTime)
import System.IO
import System.IO.Unsafe
import System.Locale (defaultTimeLocale)
import System.Log.Handler.Simple
import System.Log.Logger

-- ---------------------------------------------------------------------

registeredLoggerName :: String
registeredLoggerName = "hroqlogger"

-- ---------------------------------------------------------------------

-- | Start and register the logger process on a node
startLoggerProcess :: LocalNode -> IO ()
startLoggerProcess node = do
  -- Set up the logger. It is already global, but we choose to route
  -- logging through here to maintain a single stream through it.
  s <- streamHandler stdout DEBUG
  updateGlobalLogger rootLoggerName (setHandlers [s])

  logger <- forkProcess node loop
  runProcess node $ register registeredLoggerName logger
 where
  loop = do
    receiveWait
      [ match $ \((time, pid, string) ::(String, ProcessId, String)) -> do
          -- liftIO . hPutStrLn stderr $ time ++ " " ++ show pid ++ ": " ++ string

          liftIO . warningM (show pid) $ time ++ " " ++ show pid ++ ": " ++ string

          {-
          now <- liftIO getCurrentTime
          let timeStr = drop 17 $ show now
          liftIO . warningM (show pid) $ time ++ " " ++ timeStr ++ " " ++ show pid ++ ": " ++ string
          -}

          loop
      {-
      , match $ \((time, string) :: (String, String)) -> do
          -- this is a 'trace' message from the local node tracer
          liftIO . hPutStrLn stderr $ time ++ " [trace] " ++ string
          loop
      -}
      , match $ \(ch :: SendPort ()) -> -- a shutdown request
          sendChan ch ()
      ]

-- ---------------------------------------------------------------------

-- | Log a string
--
-- @logm message@ sends a message (time, pid of the current process, message)
-- to the process registered as 'hroqlogger'.
logm :: String -> Process ()
logm string = do
  now <- liftIO getCurrentTime
  us  <- getSelfPid
  -- let timeStr = formatTime defaultTimeLocale "%c" now
  let timeStr = show now -- Include us timing
  nsend registeredLoggerName (timeStr, us, string)
  return ()


-- | Log a string
--
-- @logm message@ sends a message (time, pid of the current process, message)
-- to the process registered as 'hroqlogger'.
logt :: String -> Process ()
logt string = do
  if False
    then do
      now <- liftIO $ getCurrentTime
      us  <- getSelfPid
      -- let timeStr = formatTime defaultTimeLocale "%c" now
      let timeStr = show now -- Include us timing
      nsend registeredLoggerName (timeStr, us, string)
    else return ()
  return ()


