class SSDB::Future < ::BasicObject

  def initialize(command)
    @command = command
  end

  def inspect
    "<SSDB::Future #{@command.inspect}>"
  end

  def value=(value)
    @value = value
  end

  def value
    unless defined?(@value)
      ::Kernel.raise ::SSDB::FutureNotReady, "Value of #{@command.inspect} is not ready"
    end
    @value
  end

  def instance_of?(klass)
    klass == ::SSDB::Future
  end

  def class
    ::SSDB::Future
  end

end