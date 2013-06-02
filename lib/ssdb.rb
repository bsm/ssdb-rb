require "monitor"

class SSDB
  include MonitorMixin

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
  # @example Simple batch
  #
  #   ssdb.batch do
  #     ssdb.set "foo", "5"
  #     ssdb.get "foo"
  #     ssdb.incr "foo"
  #   end
  #   # => [true, "5", 6]
  #
  # @example Using futures
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

  # Returns info
  # @return [Hash] info attributes
  def info
    mon_synchronize do
      perform ["info"], proc: T_INFO
    end
  end

  # Evaluates scripts
  #
  # @param [String] script
  # @param [multiple<String>] args
  # @return [Array] results
  #
  # @example
  #   script =<<-LUA
  #     local x = math.pi * 10
  #     return x
  #   LUA
  #   ssdb.eval(script)
  #   # => ["31.425926"]
  #
  def eval(script, *args)
    mon_synchronize do
      perform ["eval", script, *args]
    end
  end

  # Returns value at `key`.
  #
  # @param [String] key the key
  # @return [String] the value
  #
  # @example
  #   ssdb.get("foo") # => "val"
  def get(key)
    mon_synchronize do
      perform ["get", key]
    end
  end

  # Sets `value` at `key`.
  #
  # @param [String] key the key
  # @param [String] value the value
  #
  # @example
  #   ssdb.set("foo", "val") # => true
  def set(key, value)
    mon_synchronize do
      perform ["set", key, value], proc: T_BOOL
    end
  end

  # Increments a `key` by value
  #
  # @param [String] key the key
  # @param [Integer] value the increment
  #
  # @example
  #   ssdb.incr("foo") # => 1
  def incr(key, value = 1)
    mon_synchronize do
      perform ["incr", key, value], proc: T_INT
    end
  end

  # Decrements a `key` by value
  #
  # @param [String] key the key
  # @param [Integer] value the decrement
  #
  # @example
  #   ssdb.decr("foo") # => -1
  def decr(key, value = 1)
    mon_synchronize do
      perform ["decr", key, value], proc: T_INT
    end
  end

  # Checks existence of `key`.
  #
  # @param [String] key the key
  # @return [Boolean] true if exists
  #
  # @example
  #   ssdb.exists("foo") # => true
  def exists(key)
    mon_synchronize do
      perform ["exists", key], proc: T_BOOL
    end
  end
  alias_method :exists?, :exists

  # Delete `key`.
  #
  # @param [String] key the key
  #
  # @example
  #   ssdb.del("foo") # => nil
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
  #
  # @example
  #   ssdb.keys("a", "z", limit: 2) # => ["bar", "foo"]
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
  #
  # @example
  #   ssdb.scan("a", "z", limit: 2)
  #   # => [["bar", "val1"], ["foo", "val2"]]
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
  #
  # @example
  #   ssdb.rscan("z", "a", limit: 2)
  #   # => [["foo", "val2"], ["bar", "val1"]]
  def rscan(start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["rscan", start, stop, limit], multi: true, proc: T_STRSTR
    end
  end

  # Sets multiple keys
  #
  # @param [Hash] pairs key/value pairs
  #
  # @example
  #   ssdb.multi_set("bar" => "val1", "foo" => "val2")
  #   # => 4
  def multi_set(pairs)
    mon_synchronize do
      perform ["multi_set", *pairs.to_a].flatten, proc: T_INT
    end
  end

  # Retrieves multiple keys
  #
  # @param [Array<String>] keys
  # @return [Array<String>] values
  #
  # @example
  #   ssdb.multi_get(["bar", "foo"])
  #   # => ["val1", "val2"]
  def multi_get(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_get", *keys], multi: true, proc: T_MAPSTR, args: [keys]
    end
  end

  # Retrieves multiple keys
  #
  # @param [Array<String>] keys
  # @return [Array<String>] values
  #
  # @example
  #   ssdb.mapped_multi_get(["bar", "foo"])
  #   # => {"bar" => "val1", "foo" => val2"}
  def mapped_multi_get(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_get", *keys], multi: true, proc: T_HASHSTR
    end
  end

  # Deletes multiple keys
  #
  # @param [Array<String>] keys
  #
  # @example
  #   ssdb.multi_del(["bar", "foo"])
  #   # => 2
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
  #
  # @example
  #   ssdb.multi_exists(["bar", "foo", "baz"])
  #   # => [true, true, false]
  def multi_exists(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_exists", *keys], multi: true, proc: T_VBOOL
    end
  end
  alias_method :multi_exists?, :multi_exists

  # Returns the score of `member` at `key`.
  #
  # @param [String] key the key
  # @param [String] member the member
  # @return [Integer] the score
  #
  # @example
  #   ssdb.zget("visits", "u1")
  #   # => 101
  def zget(key, member)
    mon_synchronize do
      perform ["zget", key, member], proc: T_CINT
    end
  end

  # Sets the `score` of `member` at `key`.
  #
  # @param [String] key the key
  # @param [String] member the member
  # @param [Integer] score the score
  #
  # @example
  #   ssdb.zset("visits", "u1", 202)
  #   # => true
  def zset(key, member, score)
    mon_synchronize do
      perform ["zset", key, member, score], proc: T_BOOL
    end
  end

  # Redis 'compatibility'.
  #
  # @param [String] key the key
  # @param [Integer] score the score
  # @param [String] member the member
  #
  # @example
  #   ssdb.zadd("visits", 202, "u1")
  #   # => true
  def zadd(key, score, member)
    zset(key, member, score)
  end

  # Increments the `member` in `key` by `score`
  #
  # @param [String] key the key
  # @param [String] member the member
  # @param [Integer] score the increment
  #
  # @example
  #   ssdb.zincr("visits", "u1")
  #   # => 102
  #   ssdb.zincr("visits", "u1", 100)
  #   # => 202
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
  #
  # @example
  #   ssdb.zdecr("visits", "u1")
  #   # => 100
  #   ssdb.zdecr("visits", "u1", 5)
  #   # => 95
  def zdecr(key, member, score = 1)
    mon_synchronize do
      perform ["zdecr", key, member, score], proc: T_INT
    end
  end

  # Checks existence of a zset at `key`.
  #
  # @param [String] key the key
  # @return [Boolean] true if exists
  #
  # @example
  #   ssdb.zexists("visits")
  #   # => true
  def zexists(key)
    mon_synchronize do
      perform ["zexists", key], proc: T_BOOL
    end
  end
  alias_method :zexists?, :zexists

  # Returns the cardinality of a set `key`.
  #
  # @param [String] key the key
  #
  # @example
  #   ssdb.zsize("visits")
  #   # => 2
  def zsize(key)
    mon_synchronize do
      perform ["zsize", key], proc: T_INT
    end
  end

  # Delete an `member` from a zset `key`.
  #
  # @param [String] key the key
  # @param [String] member the member
  #
  # @example
  #   ssdb.zdel("visits", "u1")
  #   # => true
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
  #
  # @example
  #   ssdb.zlist("a", "z", limit: 2)
  #   # => ["visits", "page_views"]
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
  # @param [Integer] start start at this score
  # @param [Integer] stop stop at this score
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<String>] matching members
  #
  # @example
  #   ssdb.zkeys("visits", 0, 300, limit: 2)
  #   # => ["u1", "u2"]
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
  # @param [Integer] start start at this score
  # @param [Integer] stop stop at this score
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<Array<String,Integer>>] member/score pairs
  #
  # @example
  #   ssdb.zscan("visits", 0, 300, limit: 2)
  #   # => [["u1", 101], ["u2", 202]]
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
  # @param [Integer] start start at this score
  # @param [Integer] stop stop at this score
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<Array<String,Integer>>] member/score pairs
  #
  # @example
  #   ssdb.zrscan("visits", 300, 0, limit: 2)
  #   # => [["u2", 202], ["u1", 101]]
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
  #
  # @example
  #   ssdb.multi_zexists("visits", "page_views", "baz")
  #   # => [true, true, false]
  def multi_zexists(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_zexists", *keys], multi: true, proc: T_VBOOL
    end
  end
  alias_method :multi_zexists?, :multi_zexists

  # Returns cardinalities of multiple sets
  #
  # @param [Array<String>] keys
  # @return [Array<Boolean>] results
  #
  # @example
  #   ssdb.multi_zsize("visits", "page_views", "baz")
  #   # => [2, 1, 0]
  def multi_zsize(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_zsize", *keys], multi: true, proc: T_VINT
    end
  end

  # Sets multiple members of `key`
  #
  # @param [String] key the zset
  # @param [Hash<String,Integer>] pairs key/value pairs
  #
  # @example
  #   ssdb.multi_zset("visits", "u1" => 102, "u3" => 303)
  #   # => 2
  def multi_zset(key, pairs)
    mon_synchronize do
      perform ["multi_zset", key, *pairs.to_a].flatten, proc: T_INT
    end
  end

  # Retrieves multiple scores from `key`
  #
  # @param [String] key the zset
  # @param [Array<String>] members
  # @return [Array<Integer>] scores
  #
  # @example
  #   ssdb.multi_zget("visits", ["u1", "u2"])
  #   # => [101, 202]
  def multi_zget(key, members)
    members = Array(members) unless members.is_a?(Array)
    mon_synchronize do
      perform ["multi_zget", key, *members], multi: true, proc: T_MAPINT, args: [members]
    end
  end

  # Retrieves multiple scores from `key`
  #
  # @param [String] key the zset
  # @param [Array<String>] members
  # @return [Hash<String,Integer>] members with scores
  #
  # @example
  #   ssdb.mapped_multi_zget("visits", ["u1", "u2"])
  #   # => {"u1" => 101, "u2" => 202}
  def mapped_multi_zget(key, members)
    members = Array(members) unless members.is_a?(Array)
    mon_synchronize do
      perform ["multi_zget", key, *members], multi: true, proc: T_HASHINT
    end
  end

  # Deletes multiple members from `key`
  #
  # @param [String] key the zset
  # @param [Array<String>] members
  #
  # @example
  #   ssdb.multi_zdel("visits", ["u1", "u2"])
  #   # => 2
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

%w|version errors constants client batch future|.each do |name|
  require "ssdb/#{name}"
end
