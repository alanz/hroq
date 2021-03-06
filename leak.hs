{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{- # LANGUAGE TemplateHaskell # -}

import Control.Concurrent
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Internal.Types (createMessage,messageToPayload,payloadToMessage)
import Control.Distributed.Process.Node 
import Control.Distributed.Process.Platform
import Control.Distributed.Process.Platform.ManagedProcess hiding (runProcess)
import Control.Distributed.Process.Platform.Time
import Control.Exception as Exception
import Control.Monad(when,replicateM,foldM,liftM3,liftM2,liftM)
import Data.Binary
import Data.DeriveTH
import Data.List(elemIndices,isInfixOf)
import Data.Maybe
import Data.RefSerialize
import Data.Typeable (Typeable)
import GHC.Generics
import Network.Transport.TCP (createTransportExposeInternals, defaultTCPParameters)
import System.Directory
import System.IO
import System.IO.Error
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy as B
import qualified Data.Map as Map

-- https://github.com/tibbe/ekg
import qualified System.Remote.Monitoring as EKG



-- ---------------------------------------------------------------------

main = do
  -- EKG.forkServer "localhost" 8000

  node <- startLocalNode

  runProcess node worker

  closeLocalNode node
  
  return ()

-- ---------------------------------------------------------------------

worker :: Process ()
worker = do
  sid <- startHroqMnesia ()
  say "mnesia started"
  
  mapM_ (\n -> (call sid ("bar" ++ (show n))) :: Process ()  ) [1..800]

  {-
  let x = [] `seq` map (\n -> messageToPayload $ createMessage $ ("bar" ++ (show n)) ) [1..800]
  let y = [] `seq` map (\m -> payloadToMessage m) x
  say $ "messages=" ++ (show (x)) -- Force evaluation of x
  say $ "messages=" ++ (show (y)) -- Force evaluation of y
  -}

  liftIO $ threadDelay (1*1000000) -- 1 seconds

  say $ "mnesia blurble"


  return ()

-- ---------------------------------------------------------------------

startLocalNode :: IO LocalNode
startLocalNode = do
    -- [role, host, port] <- getArgs
  let [_role, host, port] = ["foo","127.0.0.1", "10519"]
  Right (transport,_internals) <- createTransportExposeInternals host port defaultTCPParameters
  node <- newLocalNode transport initRemoteTable
  return node
  
-- ---------------------------------------------------------------------

startHroqMnesia :: a -> Process ProcessId
startHroqMnesia initParams = do
  let server = serverDefinition
  sid <- spawnLocal $ start initParams initFunc server >> return ()
  return sid

-- data State = ST Int
type State = Int

-- init callback
initFunc :: InitHandler a Int
initFunc _ = do
  let s = 0
  return $ InitOk s Infinity


--------------------------------------------------------------------------------
-- Implementation                                                             --
--------------------------------------------------------------------------------

serverDefinition :: ProcessDefinition State
serverDefinition = defaultProcess {
     apiHandlers = [
          handleCall ((\s v -> reply () s) :: State -> String -> Process (ProcessReply State ()))
        ]
    , infoHandlers =
        [
        -- handleInfo_ (\(ProcessMonitorNotification _ _ r) -> say $ show r >> continue_)
         handleInfo (\dict (ProcessMonitorNotification _ _ r) -> do {say $ show r; continue dict })
        ]
     , timeoutHandler = \_ _ -> stop $ TerminateOther "timeout az"
     , terminateHandler = \_ reason -> do { say $ "HroqMnesia terminateHandler:" ++ (show reason) }
    } :: ProcessDefinition State

