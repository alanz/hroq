
*Main> :i TimeoutHandler
type TimeoutHandler s = s -> Delay -> Process (ProcessAction s)

*Main> :i ProcessAction
data ProcessAction s
  = ProcessContinue s
  | ProcessTimeout TimeInterval s
  | ProcessHibernate TimeInterval s
  | ProcessStop ExitReason
  | ProcessStopping s ExitReason

*Main> :i Delay
data Delay = Delay TimeInterval | Infinity




*Main> :i sleepFor
sleepFor :: Int -> TimeUnit -> Process ()

*Main> :i TimeUnit
data TimeUnit = Days | Hours | Minutes | Seconds | Millis | Micros

*Main> :i TimeInterval
data TimeInterval
  = Control.Distributed.Process.Platform.Time.TimeInterval TimeUnit
                                                           Int
