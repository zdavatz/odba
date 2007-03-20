#!/usr/bin/env ruby
#-- ConnectionPool -- ODBA -- 08.03.2005 -- hwyss@ywesee.com

require 'dbi'
require 'thread'

module ODBA
	class ConnectionPool
		POOL_SIZE = 5
		SETUP_RETRIES = 3
		attr_reader :connections
		# All connections are delegated to DBI. The constructor simply records
		# the DBI-arguments and reuses them to setup connections when needed.
		def initialize(*dbi_args)
			@dbi_args = dbi_args
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
        if(e.message == 'no connection to the server')
          raise
        end
        warn e
				if(tries > 0)
					sleep(SETUP_RETRIES - tries)
					tries -= 1
					reconnect
					retry
				else
					raise
				end
			end
		end
		def size 
			@connections.size
		end
    alias :pool_size :size
		def connect # :nodoc:
			@mutex.synchronize { _connect }
		end
		def _connect # :nodoc:
			POOL_SIZE.times { 
				@connections.push(DBI.connect(*@dbi_args))
			}
		end
		def disconnect # :nodoc:
			@mutex.synchronize { _disconnect }
		end
		def _disconnect # :nodoc:
			while(conn = @connections.shift)
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
