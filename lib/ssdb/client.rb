require "socket"
require "uri"

class SSDB
  class Client
    NL = "\n".freeze
    OK = "ok".freeze
    NOT_FOUND = "not_found".freeze

    attr_reader :url, :timeout
    attr_accessor :reconnect

    # @param [Hash] opts
    # @option opts [String|URI] :url the URL to connect to,required
    # @option opts [Numeric] :timeout socket timeout, defaults to 10s
    def initialize(opts = {})
      @timeout   = opts[:timeout] || 10.0
      @sock      = nil
      @url       = parse_url(opts[:url] || ENV["SSDB_URL"] || "ssdb://127.0.0.1:8888/")
      @reconnect = opts[:reconnect] != false
    end

    # @return [String] URL string
    def id
      url.to_s
    end

    # @return [Integer] port
    def port
      @port ||= url.port || 8888
    end

    # @return [Boolean] true if connected
    def connected?
      !!@sock
    end

    # Disconnects the client
    def disconnect
      @sock.close if connected?
    rescue
    ensure
      @sock = nil
    end

    # Calls a single command
    # @param [Hash] opts options
    # @option opts [Array] :cmd command parts
    # @option opts [Boolean] :multi true if multi-response is expected
    # @option opts [Proc] :proc a proc to apply to the result
    # @option opts [Array] :args arguments to pass to the :proc
    def call(opts)
      perform([opts])[0]
    end

    # Performs multiple commands
    # @param [Array<Hash>] commands array of command options
    # @see SSDB::Client#call for command format
    def perform(commands)
      message = ""

      commands.each do |hash|
        hash[:cmd].each do |c|
          message << c.bytesize.to_s << NL << c << NL
        end
        message << NL
      end

      results = []
      ensure_connected do
        io(:write, message)

        commands.each do |hash|
          part = read_part(hash[:multi])
          if hash[:proc]
            args = [part]
            args.concat(hash[:args]) if hash[:args]
            part = hash[:proc].call(*args)
          end
          results << part
        end
      end

      results
    end

    protected

      # @return [TcpSocket] socket connection
      def socket
        @sock ||= connect
      end

      # Safely perform IO operation
      def io(op, *args)
        socket.__send__(op, *args)
      rescue Errno::EAGAIN
        raise SSDB::TimeoutError, "Connection timed out"
      rescue Errno::ECONNRESET, Errno::EPIPE, Errno::ECONNABORTED, Errno::EBADF, Errno::EINVAL => e
        raise SSDB::ConnectionError, "Connection lost (%s)" % [e.class.name.split("::").last]
      end

      def ensure_connected
        attempts = 0
        begin
          yield
        rescue SSDB::ConnectionError
          disconnect
          retry if (attempts += 1) < 2
          raise
        rescue Exception
          disconnect
          raise
        end
      end

    private

      # "Inspired" by http://www.mikeperham.com/2009/03/15/socket-timeouts-in-ruby/
      def connect
        addr = Socket.getaddrinfo(url.host, nil)
        sock = Socket.new(Socket.const_get(addr[0][0]), Socket::SOCK_STREAM, 0)
        sock.setsockopt Socket::SOL_SOCKET, Socket::SO_RCVTIMEO, sock_timeout
        sock.setsockopt Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, sock_timeout
        sock.connect(Socket.pack_sockaddr_in(port, addr[0][3]))
        sock
      end

      # Converts numeric `timeout` into a packed socket option value
      def sock_timeout
        @sock_timeout ||= begin
          secs  = Integer(timeout)
          usecs = Integer((timeout - secs) * 1_000_000)
          [secs, usecs].pack("l_2")
        end
      end

      def read_len
        len = io(:gets).chomp
        len unless len.empty?
      end

      def read_part(multi)
        read_len || return
        status = io(:gets).chomp

        case status
        when OK
          part = []
          part << io(:gets).chomp while read_len
          part.size > 1 || multi ? part : part[0]
        when NOT_FOUND
          multi ? [] : nil
        else
          raise SSDB::CommandError, "Server responded with '#{status}'"
        end
      end

      # Parses `url`
      def parse_url(url)
        url = URI(url) if url.is_a?(String)

        # Validate URL
        unless url.host
          raise ArgumentError, "Invalid :url option, unable to determine 'host'."
        end

        url
      end

  end

end
