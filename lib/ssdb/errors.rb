class SSDB
  Error           = Class.new(RuntimeError)
  ConnectionError = Class.new(Error)
  TimeoutError    = Class.new(Error)
  CommandError    = Class.new(Error)
  FutureNotReady  = Class.new(Error)
end