require "monitor"

class SSDB
  include MonitorMixin

  Error           = Class.new(RuntimeError)
  ConnectionError = Class.new(Error)
  TimeoutError    = Class.new(Error)
  CommandError    = Class.new(Error)
  FutureNotReady  = Class.new(Error)

  T_BOOL   = ->r { r == "1" }
  T_INT    = ->r { r.to_i }
  T_CINT   = ->r { r.to_i if r }
  T_VBOOL  = ->r { r.each_slice(2).map {|_, v| v == "1" }}
  T_VINT   = ->r { r.each_slice(2).map {|_, v| v.to_i }}
  T_STRSTR = ->r { r.each_slice(2).to_a }
  T_STRINT = ->r { r.each_slice(2).map {|v, s| [v, s.to_i] } }
  T_MAPINT = ->r,n { h = {}; r.each_slice(2) {|k, v| h[k] = v }; n.map {|k| h[k].to_i } }
  T_MAPSTR = ->r,n { h = {}; r.each_slice(2) {|k, v| h[k] = v }; n.map {|k| h[k] } }
  BLANK    = "".freeze

  # @attr_reader [SSDB::Client] the client
  attr_reader :client

  # @return [SSDB] the current/global SSDB connection
  def self.current
    @current ||= SSDB.new
  end

  # @param [SSDB] ssdb the current/global SSDB connection
  def self.current=(ssdb)
    @current = ssdb
  end

  # @see SSDB::Client#initialize
  def initialize(*a)
    @client = Client.new(*a)
    super() # Monitor#initialize
  end

  # Execute a batch operation
  #
  # @example simple batch
  #
  #   ssdb.batch do
  #     ssdb.set "foo", "5"
  #     ssdb.get "foo"
  #     ssdb.incr "foo"
  #   end
  #   # => [true, "5", 6]
  #
  # @example batch with futures
  #
  #   ssdb.batch do
  #     v = ssdb.set "foo", "5"
  #     w = ssdb.incr "foo"
  #   end
  #
  #   v.value
  #   # => true
  #   w.value
  #   # => 6
  #
  def batch
    mon_synchronize do
      begin
        original, @client = @client, SSDB::Batch.new
        yield(self)
        @client.values = original.perform(@client)
      ensure
        @client = original
      end
    end
  end

  # Returns value at `key`.
  #
  # @param [String] key the key
  # @return [String] the value
  def get(key)
    mon_synchronize do
      perform ["get", key]
    end
  end

  # Sets `value` at `key`.
  #
  # @param [String] key the key
  # @param [String] value the value
  def set(key, value)
    mon_synchronize do
      perform ["set", key, value], proc: T_BOOL
    end
  end

  # Increments a `key` by value
  #
  # @param [String] key the key
  # @param [Integer] value the increment
  def incr(key, value = 1)
    mon_synchronize do
      perform ["incr", key, value], proc: T_INT
    end
  end

  # Decrements a `key` by value
  #
  # @param [String] key the key
  # @param [Integer] value the decrement
  def decr(key, value = 1)
    mon_synchronize do
      perform ["decr", key, value], proc: T_INT
    end
  end

  # Checks existence of `key`.
  #
  # @param [String] key the key
  # @return [Boolean] true if exists
  def exists(key)
    mon_synchronize do
      perform ["exists", key], proc: T_BOOL
    end
  end
  alias_method :exists?, :exists

  # Delete `key`.
  #
  # @param [String] key the key
  def del(key)
    mon_synchronize do
      perform ["del", key]
    end
  end

  # Scans keys between `start` and `stop`.
  #
  # @param [String] start start at this key
  # @param [String] stop stop at this key
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<String>] matching keys
  def keys(start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["keys", start, stop, limit], multi: true
    end
  end

  # Scans keys between `start` and `stop`.
  #
  # @param [String] start start at this key
  # @param [String] stop stop at this key
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<Array<String,String>>] key/value pairs
  def scan(start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["scan", start, stop, limit], multi: true, proc: T_STRSTR
    end
  end

  # Reverse-scans keys between `start` and `stop`.
  #
  # @param [String] start start at this key
  # @param [String] stop stop at this key
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<Array<String,String>>] key/value pairs in reverse order
  def rscan(start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["rscan", start, stop, limit], multi: true, proc: T_STRSTR
    end
  end

  # Sets multiple keys
  #
  # @param [Hash] pairs key/value pairs
  def multi_set(pairs)
    mon_synchronize do
      perform ["multi_set", *pairs.to_a].flatten, proc: T_INT
    end
  end

  # Retrieves multiple keys
  #
  # @param [Array<String>] keys
  # @return [Array<String>] values
  def multi_get(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_get", *keys], multi: true, proc: T_MAPSTR, args: [keys]
    end
  end

  # Deletes multiple keys
  #
  # @param [Array<String>] keys
  def multi_del(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_del", *keys], proc: T_INT
    end
  end

  # Checks existence of multiple keys
  #
  # @param [Array<String>] keys
  # @return [Array<Boolean>] results
  def multi_exists(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_exists", *keys], multi: true, proc: T_VBOOL
    end
  end

  # Returns the score of `member` at `key`.
  #
  # @param [String] key the key
  # @param [String] member the member
  # @return [Float] the score
  def zget(key, member)
    mon_synchronize do
      perform ["zget", key, member], proc: T_CINT
    end
  end

  # Sets the `score` of `member` at `key`.
  #
  # @param [String] key the key
  # @param [String] member the member
  # @param [Numeric] score the score
  def zset(key, member, score)
    mon_synchronize do
      perform ["zset", key, member, score], proc: T_BOOL
    end
  end

  # Redis 'compatibility'.
  #
  # @param [String] key the key
  # @param [Numeric] score the score
  # @param [String] member the member
  def zadd(key, score, member)
    zset(key, member, score)
  end

  # Increments the `member` in `key` by `score`
  #
  # @param [String] key the key
  # @param [String] member the member
  # @param [Integer] score the increment
  def zincr(key, member, score = 1)
    mon_synchronize do
      perform ["zincr", key, member, score], proc: T_INT
    end
  end

  # Decrements the `member` in `key` by `score`
  #
  # @param [String] key the key
  # @param [String] member the member
  # @param [Integer] score the decrement
  def zdecr(key, member, score = 1)
    mon_synchronize do
      perform ["zdecr", key, member, score], proc: T_INT
    end
  end

  # Checks existence of a zset at `key`.
  #
  # @param [String] key the key
  # @return [Boolean] true if exists
  def zexists(key)
    mon_synchronize do
      perform ["zexists", key], proc: T_BOOL
    end
  end
  alias_method :zexists?, :zexists

  # Returns the cardinality of a set `key`.
  #
  # @param [String] key the key
  def zsize(key)
    mon_synchronize do
      perform ["zsize", key], proc: T_INT
    end
  end

  # Delete an `member` from a zset `key`.
  #
  # @param [String] key the key
  # @param [String] member the member
  def zdel(key, member)
    mon_synchronize do
      perform ["zdel", key, member], proc: T_BOOL
    end
  end

  # List zset keys between `start` and `stop`.
  #
  # @param [String] start start at this key
  # @param [String] stop stop at this key
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<String>] matching zset keys
  def zlist(start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["zlist", start, stop, limit], multi: true
    end
  end

  # Lists members at `key` starting at `start_member`
  # between `start` and `stop` scores.
  #
  # @param [String] key the zset
  # @param [Float] start start at this score
  # @param [Float] stop stop at this score
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<String>] matching members
  def zkeys(key, start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["zkeys", key, BLANK, start, stop, limit], multi: true
    end
  end

  # Scans for members at `key` starting at `start_member`
  # between `start` and `stop` scores.
  #
  # @param [String] key the zset
  # @param [Float] start start at this score
  # @param [Float] stop stop at this score
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<Array<String,Float>>] member/score pairs
  def zscan(key, start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["zscan", key, BLANK, start, stop, limit], multi: true, proc: T_STRINT
    end
  end

  # Reverse scans for members at `key` starting at `start_member`
  # between `start` and `stop` scores.
  #
  # @param [String] key the zset
  # @param [Float] start start at this score
  # @param [Float] stop stop at this score
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<Array<String,Float>>] member/score pairs
  def zrscan(key, start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["zrscan", key, BLANK, start, stop, limit], multi: true, proc: T_STRINT
    end
  end

  # Checks existence of multiple sets
  #
  # @param [Array<String>] keys
  # @return [Array<Boolean>] results
  def multi_zexists(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_zexists", *keys], multi: true, proc: T_VBOOL
    end
  end

  # Returns cardinalities of multiple sets
  #
  # @param [Array<String>] keys
  # @return [Array<Boolean>] results
  def multi_zsize(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_zsize", *keys], multi: true, proc: T_VINT
    end
  end

  # Sets multiple members of `key`
  #
  # @param [String] key the zset
  # @param [Hash] pairs key/value pairs
  def multi_zset(key, pairs)
    mon_synchronize do
      perform ["multi_zset", key, *pairs.to_a].flatten, proc: T_INT
    end
  end

  # Retrieves multiple scores from `key`
  #
  # @param [String] key the zset
  # @param [Array<String>] members
  # @return [Array<Float>] scores
  def multi_zget(key, members)
    members = Array(members) unless members.is_a?(Array)
    mon_synchronize do
      perform ["multi_zget", key, *members], multi: true, proc: T_MAPINT, args: [members]
    end
  end

  # Deletes multiple members from `key`
  #
  # @param [String] key the zset
  # @param [Array<String>] members
  def multi_zdel(key, members)
    members = Array(members) unless members.is_a?(Array)
    mon_synchronize do
      perform ["multi_zdel", key, *members], proc: T_INT
    end
  end

  private

    def perform(chain, opts = {})
      opts[:cmd] = chain.map(&:to_s)
      client.call(opts)
    end

end

%w|version client batch future|.each do |name|
  require "ssdb/#{name}"
end
