#!/usr/bin/env ruby

$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'odba'
require 'test/unit'
require 'odba/scalar_cache'
require 'mock'

module ODBA
	class TestScalarCache < Test::Unit::TestCase
		class ScalarCache < ScalarCache
			attr_accessor :hash
		end
		def setup
			@scalar_cache = ScalarCache.new
			ODBA.cache_server = Mock.new("cache_server")
		end
		def test_update
			cache_values = [
				[1,:bar, "foo"],
				[1,:bak, "goo"],
				[1,:faz, "gaa"],
			]
			@scalar_cache.update(cache_values)
			assert_equal(3, @scalar_cache.size)
			cache_values = [
				[1,:faz, "bar"],
			]
			@scalar_cache.update(cache_values)
			assert_equal(3, @scalar_cache.size)
		end
		def test_fetch
			cache_values = [
				[1,:faz, "gaa"]
			]
			@scalar_cache.update(cache_values)
			result = @scalar_cache.fetch(1, :faz)
			assert_equal("gaa", result)
		end
		def test_delete
			@scalar_cache.hash = {
				[1,:faz] => "gaa"
			}
			@scalar_cache.delete(1)
			assert_equal(0, @scalar_cache.size)
		end
	end
end
