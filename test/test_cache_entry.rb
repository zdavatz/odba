#!/usr/bin/env ruby
# -- odba-- 29.04.2004 -- mwalder@ywesee.com

$: << File.dirname(__FILE__)
$: << File.expand_path("../src", File.dirname(__FILE__))

require 'test/unit'
require 'mock'
require 'odba'
module ODBA
	class CacheEntry
		attr_accessor :accessed_by
		RETIRE_TIME = 0.9
		DESTROY_TIME = 0.9 
	end
	class TestCacheEntry < Test::Unit::TestCase
	class TestMockCacheEntry < Mock
		def is_a?(arg)
			true
		end
	end
		def setup
			@mock = Mock.new
			@cache_entry = ODBA::CacheEntry.new(@mock)
			ODBA.cache_server = Mock.new("cache_server")
		end
		def test_retire_check
			assert_equal(false, @cache_entry.odba_old?)
			sleep(1.5)
			assert_equal(true, @cache_entry.odba_old?)
		end
		def test_ready_to_destroy_true
			sleep(1)
			@mock.__next(:odba_prefetch?){||false}
			assert_equal(true, @cache_entry.ready_to_destroy?)
		end
		def test_ready_to_destroy_false
			@mock.__next(:odba_prefetch?){||true}
			@cache_entry.accessed_by = ["foo"]
			assert_equal(false, @cache_entry.ready_to_destroy?)
		end
		def test_retire
			obj_1 = TestMockCacheEntry.new
			obj_2 = TestMockCacheEntry.new
			hash = TestMockCacheEntry.new
			obj_1.__next(:is_a?) { |arg| false}
			ODBA.cache_server.__next(:include?){|id| false}
			obj_1.__next(:is_a?) { |arg| true}
			obj_1.__next(:odba_replace_persistable) {}
			obj_2.__next(:is_a?) { |arg| false}
			obj_2.__next(:is_a?) { |arg| true}
			obj_2.__next(:odba_replace_persistable) {}
			hash.__next(:is_a?) { |arg| true }
			hash.__next(:odba_id) { 1}
			hash.__next(:is_a?) { |arg| false }
			@cache_entry.accessed_by = [obj_1, obj_2, hash]
			@cache_entry.odba_retire
			assert_equal([hash], @cache_entry.accessed_by)
			hash.__verify
			obj_1.__verify
			obj_2.__verify
		end
		def test_odba_add_reference
			mock = Mock.new
			@cache_entry.odba_add_reference(mock)
			assert_equal(mock, @cache_entry.accessed_by[0])
		end
	end
end