#!/usr/bin/env ruby

# ODBA::ConnectionPool -- odba -- 08.12.2011 -- mhatakeyama@ywesee.com
# ODBA::ConnectionPool -- odba -- 08.03.2005 -- hwyss@ywesee.com

require "dbi"

module ODBA
  class ConnectionPool
    POOL_SIZE = 5
    SETUP_RETRIES = 3
    # ydbd-pg maps every PG::Error - including connection level failures -
    # onto DBI::ProgrammingError, so for that class the message is the only
    # thing that tells a dead connection apart from a malformed statement.
    # Without this list a restart of the database server leaves every pooled
    # connection permanently broken, because the reconnect below never runs.
    CONNECTION_LOST_MESSAGES = /
      no\ connection\ to\ the\ server
      | PQsocket
      | server\ closed\ the\ connection
      | connection\ not\ open
      | terminating\ connection
      | SSL\ connection\ has\ been\ closed
      | could\ not\ connect\ to\ server
    /xi
    # attr_reader :connections
    attr_reader :connections, :dbi_args
    # All connections are delegated to DBI. The constructor simply records
    # the DBI-arguments and reuses them to setup connections when needed.
    def initialize(*dbi_args)
      @dbi_args = dbi_args
      @opts = @dbi_args.last.is_a?(Hash) ? @dbi_args.pop : {}
      @connections = []
      @mutex = Mutex.new
      connect
    end

    def next_connection # :nodoc:
      conn = nil
      @mutex.synchronize {
        conn = @connections.shift
      }
      yield(conn)
    ensure
      @mutex.synchronize {
        @connections.push(conn)
      }
    end

    def method_missing(method, *args, &block) # :nodoc:
      tries = SETUP_RETRIES
      begin
        next_connection { |conn|
          conn.send(method, *args, &block)
        }
      rescue NoMethodError, DBI::Error => e
        warn e
        if tries > 0 && retryable?(e)
          sleep(SETUP_RETRIES - tries)
          tries -= 1
          reconnect
          retry
        else
          raise
        end
      end
    end

    # A statement that failed because the connection is gone is worth
    # retrying on a fresh one. A statement that failed on its own merits is
    # not - retrying it would only repeat the same error three times.
    def retryable?(error) # :nodoc:
      !error.is_a?(DBI::ProgrammingError) ||
        CONNECTION_LOST_MESSAGES.match?(error.message.to_s)
    end

    def size
      @connections.size
    end
    alias_method :pool_size, :size
    def connect # :nodoc:
      @mutex.synchronize { _connect }
    end

    def _connect # :nodoc:
      POOL_SIZE.times {
        conn = DBI.connect(*@dbi_args)
        if encoding = @opts[:client_encoding]
          conn.execute "SET CLIENT_ENCODING TO '#{encoding}'"
        end
        @connections.push(conn)
      }
    end

    def disconnect # :nodoc:
      @mutex.synchronize { _disconnect }
    end

    def _disconnect # :nodoc:
      while (conn = @connections.shift)
        begin
          conn.disconnect
        rescue DBI::InterfaceError, Exception
          ## we're not interested, since we are disconnecting anyway
          nil
        end
      end
    end

    def reconnect # :nodoc:
      @mutex.synchronize {
        _disconnect
        _connect
      }
    end
  end
end
