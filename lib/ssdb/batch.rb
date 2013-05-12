class SSDB::Batch < Array

  # Constructor
  def initialize
    @futures = []
    super
  end

  # Call command
  # @param [Hash] opts the command options
  def call(opts)
    push(opts)

    future = SSDB::Future.new(opts[:cmd])
    @futures.push(future)
    future
  end

  # @param [Array] values
  def values=(values)
    values.each_with_index do |value, index|
      future = @futures[index]
      future.value = value if future
    end
  end

end