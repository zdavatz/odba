#!/usr/bin/env ruby

$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'odba'
require 'test/unit'
require 'mock'

module ODBA
	class TestHashReplaceStubs < Test::Unit::TestCase
		class StubMock < Mock
			def is_a?(arg)
				true
			end
		end
		class ODBAContainer
			include Persistable
			attr_accessor	:non_replaceable, :replaceable
		end
		class TestStub
			attr_accessor :receiver, :odba_replace
			def is_a?(arg)
				true
			end
		end
		def setup
			@hash = Hash.new
			#@hash.extend(ODBA::PersistableHash)
			ODBA.cache_server = Mock.new("cache_server")
			ODBA.storage = Mock.new("storage")
			@hash.clear
		end
		def test_odba_unsaved_neighbors_hash
			repkey1 = ODBAContainer.new
			repkey2 = ODBAContainer.new
			repvalue1 = ODBAContainer.new
			repvalue2 = ODBAContainer.new
			@hash.store(repkey1, repvalue1)
			@hash.store(repkey2, repvalue2)
			result = @hash.odba_unsaved_neighbors(1)
			assert_equal(true, result.include?(repkey1))
			assert_equal(true, result.include?(repkey2))
			assert_equal(true, result.include?(repvalue1))
			assert_equal(true, result.include?(repvalue2))
		end
		def test_restore_keys
			repkey1 = ODBAContainer.new
			repkey2 = ODBAContainer.new
			stubkey1 = StubMock.new
			stubkey2 = StubMock.new
			foo = Mock.new("foo")

			stubkey1.__next(:is_a?) { |arg| true }
			stubkey1.__next(:odba_id) { 2 }
			stubkey1.__next(:is_a?) { |arg| true }
			stubkey1.__next(:odba_replace) { ||}
			stubkey1.__next(:receiver) { || repkey1 }
			
			stubkey2.__next(:is_a?) { |arg| true }
			stubkey2.__next(:odba_id) { 3 }
			stubkey2.__next(:is_a?) { |arg| true }
			stubkey2.__next(:odba_replace) { || }
			stubkey2.__next(:receiver) { || repkey2}
			
			ODBA.cache_server.__next(:bulk_fetch) { |ids, obj| }
			
			@hash.store(stubkey1, "foo")
			@hash.store(stubkey2, "bar")
			@hash.odba_restore
			
			stubkey1.__verify
			stubkey2.__verify
			foo.__verify
			ODBA.cache_server.__verify
			assert_equal("foo", @hash[repkey1])
			assert_equal("bar", @hash[repkey2])
		end
		def test_hash_replacement
			repvalue1 = ODBAContainer.new
			repvalue2 = ODBAContainer.new

			stubvalue2 = StubMock.new("stubvalue2")
			stubvalue1 = StubMock.new("stubvalue1")
			bar = Mock.new("bar")

			stubvalue1.__next(:is_a?) { |arg| true}
			stubvalue1.__next(:odba_id) { 2 }
			stubvalue1.__next(:is_a?) { |arg| true}
			stubvalue1.__next(:odba_replace) { || }
			stubvalue1.__next(:receiver) { || repvalue1}

			stubvalue2.__next(:is_a?) { |arg| true}
			stubvalue2.__next(:odba_id) { 3 }
			stubvalue2.__next(:is_a?) { |arg| true}
			stubvalue2.__next(:odba_replace) { ||}
			stubvalue2.__next(:receiver) { || repvalue2}
			
			ODBA.cache_server.__next(:bulk_fetch){ |id, ojb|}
			@hash.store("foo", stubvalue1)
			@hash.store("bar", stubvalue2)
			@hash.odba_restore
			ODBA.cache_server.__verify
			stubvalue1.__verify
			stubvalue2.__verify
			bar.__verify
			assert_equal(repvalue1, @hash["foo"])
			assert_equal(repvalue2, @hash["bar"])
		end
		def test_odba_restore
			repvalue1 = ODBAContainer.new
			repvalue2 = ODBAContainer.new
			repkey1 = ODBAContainer.new
			repkey2 = ODBAContainer.new
			stubvalue2 = StubMock.new("stubvalue2")
			stubvalue1 = StubMock.new("stubvalue1")
			stubkey1 = StubMock.new("stubkey1")
			stubkey2 = StubMock.new("stubkey2")
			ODBA.cache_server = Mock.new("cache_server")
			foo = Mock.new("foo")

			stubvalue1.__next(:is_a?) { |arg| true}
			stubvalue1.__next(:odba_id) { 2 }
			stubvalue1.__next(:is_a?) { |arg| true}
			stubvalue1.__next(:odba_replace) { ||}
			stubvalue1.__next(:receiver) { || repvalue1}
			
			stubkey1.__next(:is_a?) { |arg| true}
			stubkey1.__next(:odba_id) { 2 }
			stubkey1.__next(:is_a?) { |arg| true}
			stubkey1.__next(:odba_replace) { || }
			stubkey1.__next(:receiver) { || repkey1}

			stubvalue2.__next(:is_a?) { |arg| true}
			stubvalue2.__next(:odba_id) { 2 } 
			stubvalue2.__next(:is_a?) { |arg| true}
			stubvalue2.__next(:odba_replace) { ||}
			stubvalue2.__next(:receiver) { || repvalue2}

			stubkey2.__next(:is_a?) { |arg| true}
			stubkey2.__next(:odba_id) { 2 }
			stubkey2.__next(:is_a?) { |arg| true}
			stubkey2.__next(:odba_replace) { ||}
			stubkey2.__next(:receiver) { || repkey2}

			ODBA.cache_server.__next(:bulk_fetch) { |ids, obj|}
			@hash.store(stubkey1, stubvalue1)
			@hash.store(stubkey2, stubvalue2)
			@hash.odba_restore
			stubvalue1.__verify
			stubvalue2.__verify
			stubkey1.__verify
			stubkey2.__verify
			foo.__verify
			ODBA.cache_server.__verify
			assert_equal(true, @hash.has_key?(repkey1))
			assert_equal(repvalue1, @hash[repkey1])
			assert_equal(true, @hash.has_key?(repkey2))
			assert_equal(repvalue2, @hash[repkey2])
		end
		def test_odba_replace_persistables_hash
			key1 = StubMock.new
			value1 = StubMock.new
			@hash.store(key1, value1)
			
			key1.__next(:is_a?) { |arg| true }
			key1.__next(:odba_id) { || 1}
			key1.__next(:odba_id) { || 1}
			
			value1.__next(:is_a?) { |arg| true }
			value1.__next(:odba_id) { || 2}
			value1.__next(:odba_id) { || 2}

			@hash.odba_replace_persistables
			@hash.each { |key, value|
				assert_equal(true, key.is_a?(Stub))
				assert_equal(true, value.is_a?(Stub))
			}
			key1.__verify
			value1.__verify
			ODBA.storage.__verify
			ODBA.cache_server.__verify
		end
		def test_odba_prefetch
			key1  = StubMock.new("key")
			val1 = StubMock.new("val1")
			@hash.store(key1, val1)
			assert_equal(false, @hash.odba_prefetch?)
			val1.__verify
			key1.__verify
		end
		def test_odba_unsaved_true
			key = StubMock.new("key")
			val = StubMock.new("val")
			@hash.instance_variable_set("@odba_persistent", true)
			@hash.store(key, val)
			val.__next(:is_a?) { |klass| true }
			val.__next(:odba_unsaved?) { true }

			assert_equal(true, @hash.odba_unsaved?)
			val.__verify
			key.__verify
		end
	end
end
