
#!/usr/bin/env ruby

$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'odba'
require 'test/unit'
require 'odba/scalar_cache'
require 'mock'

module ODBA
	class TestScalarCache < Test::Unit::TestCase
		class ScalarCache < ScalarCache
			attr_accessor :scalar_cache
		end
		def setup
			@scalar_cache = ScalarCache.new
			ODBA.cache_server = Mock.new("cache_server")
		end
=begin
		def test_foobar
			baz = Hash.new
			foo = [[1],[:bar]]
			goo = [[2],[:bak]]
			gaa = [[1],[:faz]]
			baz[foo] = "hello World"
			baz[goo] = "ywesee"
			baz[gaa] = "HUHU"
			puts baz.inspect
			baz.keys.each { |key|
				if (key.first.first == 1)
					puts key.inspect
					baz.delete(key)
				end
			}
			puts "hash:"
			puts baz.inspect
		end
=end
		def test_udpate
			cache_values = Array.new
			baz = Hash.new
			cache_values.push([1,:bar, "foo"])
			cache_values.push([1,:bak, "goo"])
			cache_values.push([1,:faz, "gaa"])
			@scalar_cache.update(cache_values)
			assert_equal(3, @scalar_cache.scalar_cache.size)
			cache_values = []
			cache_values.push([1,:faz, "gaa"])
			@scalar_cache.update(cache_values)
			assert_equal(1, @scalar_cache.scalar_cache.size)
		end
		def test_fetch
			cache_values = []
			cache_values.push([1,:faz, "gaa"])
			@scalar_cache.update(cache_values)
			result = @scalar_cache.fetch(1, :faz)
			assert_equal("gaa", result)
		end
	end
end
