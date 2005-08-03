#!/usr/bin/env ruby
# TestConnectionPool -- odba -- 03.08.2005 -- hwyss@ywesee.com


$: << File.expand_path('../lib', File.dirname(__FILE__))

require 'test/unit'
require 'odba/connection_pool'

module ODBA
	class ConnectionStub
		attr_accessor :response
		def initialize
			@response = Proc.new { }
		end
		def method_missing(*args)
			@response.call
		end
	end
end
module DBI
	def DBI.connect(*args)
		ODBA::ConnectionStub.new
	end
end
module ODBA
	class TestConnectionPool < Test::Unit::TestCase
		def setup
			@pool = ConnectionPool.new()
		end
		def test_survive_restart
			@pool.connections.each { |conn|
				conn.response = Proc.new { raise DBI::ProgrammingError.new }
			}
			assert_nothing_raised { @pool.execute('statement') }
		end
	end
end
