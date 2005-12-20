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
			def odba_id
				1
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
			ODBA.cache = Mock.new("cache")
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
		def test_odba_replace_persistables_hash
			key1 = StubMock.new("key")
			value1 = StubMock.new("value")
			@hash.store(key1, value1)
			@hash.odba_replace_persistables
			assert_equal(@hash, {})
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
