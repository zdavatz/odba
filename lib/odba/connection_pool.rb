#!/usr/bin/env ruby
# ConnectionPool -- ODBA -- 08.03.2005 -- hwyss@ywesee.com

require 'dbi'
require 'thread'

module ODBA
	class ConnectionPool
		POOL_SIZE = 5
		SETUP_RETRIES = 3
		attr_reader :connections
		def initialize(*dbi_args)
			@dbi_args = dbi_args
			@connections = []
			@mutex = Mutex.new
			connect
		end
		def next_connection
			conn = nil
			@mutex.synchronize {
				conn = @connections.at(@pos)
				@pos = (@pos + 1) % POOL_SIZE
			}
			conn
		end
		def method_missing(method, *args, &block)
			tries = SETUP_RETRIES
			begin
				next_connection.send(method, *args, &block)
			rescue DBI::DatabaseError
				if(tries > 0)
					sleep(SETUP_RETRIES - tries)
					reconnect
					retry
				else
					raise
				end
			end
		end
		def pool_size
			@connections.size
		end
		def connect
			@pos = 0
			@mutex.synchronize { 
				POOL_SIZE.times { 
					@connections.push(DBI.connect(*@dbi_args))
				}
			}
		end
		def disconnect
			@mutex.synchronize {
				while(conn = @connections.shift)
					begin 
						conn.disconnect
					rescue DBI::InterfaceError, Exception
						## we're not interested.
					end
				end
			}
		end
		def reconnect
			disconnect
			connect
		end
	end
end
