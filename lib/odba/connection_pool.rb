#!/usr/bin/env ruby
# ConnectionPool -- ODBA -- 08.03.2005 -- hwyss@ywesee.com

require 'dbi'

module ODBA
	class ConnectionPool
		POOL_SIZE = 5
		def initialize(*dbi_args)
			@dbi_args = dbi_args
			@connections = []
			POOL_SIZE.times { 
				@connections.push(DBI.connect(*dbi_args))
			}
			@pos = 0
		end
		def next_connection
			conn = @connections.at(@pos)
			@pos = (@pos + 1) % POOL_SIZE
			conn
		end
		def method_missing(method, *args, &block)
			next_connection.send(method, *args, &block)
		end
		def pool_size
			@connections.size
		end
	end
end
