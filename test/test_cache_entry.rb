#!/usr/bin/env ruby
# TestCacheEntry -- odba-- 29.04.2004 -- hwyss@ywesee.com mwalder@ywesee.com

$: << File.dirname(__FILE__)
$: << File.expand_path("../lib", File.dirname(__FILE__))

require 'test/unit'
require 'flexmock'
require 'odba/cache_entry'
require 'odba/odba'
require 'odba/persistable'
module ODBA
	class CacheEntry
		attr_accessor :accessed_by
	end
	class TestCacheEntry < Test::Unit::TestCase
    include FlexMock::TestCase
		def setup
			@mock = flexmock
			@cache_entry = ODBA::CacheEntry.new(@mock)
			ODBA.cache = flexmock("cache")
      ODBA.cache.should_receive(:retire_age).and_return(0.9)
      ODBA.cache.should_receive(:destroy_age).and_return(0.9)
		end
		def test_retire_check
			@mock.mock_handle(:odba_unsaved?) { false }
			@mock.mock_handle(:odba_unsaved?) { false }
			assert_equal(false, @cache_entry.odba_old?)
			sleep(1.5)
			assert_equal(true, @cache_entry.odba_old?)
		end
		def test_ready_to_destroy_true
			@mock.mock_handle(:odba_unsaved?) { false }
			sleep(1)
			assert_equal(true, @cache_entry.ready_to_destroy?)
		end
		def test_ready_to_destroy_false__age
			@mock.mock_handle(:odba_unsaved?) { false }
			assert_equal(false, @cache_entry.ready_to_destroy?)
		end
		def test_ready_to_destroy_false__accessed_by
			sleep(1)
			@mock.mock_handle(:odba_unsaved?) { false }
			@cache_entry.accessed_by = ['foo']
			assert_equal(false, @cache_entry.ready_to_destroy?)
		end
		def test_ready_to_destroy_false__combined
			@mock.mock_handle(:odba_unsaved?) { false }
			@cache_entry.accessed_by = ['foo']
			assert_equal(false, @cache_entry.ready_to_destroy?)
		end
		def test_retire
			obj_1 = flexmock
			obj_2 = flexmock
			hash = {}
			obj_1.should_receive(:is_a?).with(Enumerable).and_return(false)
			obj_1.should_receive(:is_a?).with(Persistable).and_return(true)
			obj_1.should_receive(:odba_replace_persistable)\
        .times(1).and_return(['@name'])
      ODBA.cache.mock_handle(:include?) { |odba_id|
        odba_id == 35 }
			obj_2.should_receive(:is_a?).with(Enumerable).and_return(true)
			obj_2.should_receive(:is_a?).with(Persistable).and_return(true)
			obj_2.mock_handle(:odba_id) { 34 }
			obj_2.should_receive(:odba_replace_persistable)\
        .times(1).and_return(['@name'])
      ODBA.cache.mock_handle(:include?) { |odba_id|
        odba_id == 35 }
			hash.instance_variable_set('@odba_id', 35)
			@cache_entry.accessed_by = {obj_1 => true, obj_2 => true, 
        hash => true}
			@cache_entry.odba_retire
			assert_equal({hash => true}, @cache_entry.accessed_by)
			obj_1.mock_verify
			obj_2.mock_verify
		end
		def test_odba_add_reference
			mock = flexmock
			@cache_entry.odba_add_reference(mock)
			assert_equal({mock => true}, @cache_entry.accessed_by)
		end
		def test_odba_id
			@mock.mock_handle(:odba_id) { 123 }
			assert_equal(123, @cache_entry.odba_id)
		end
    def test_odba_cut_connections
      ## transaction-rollback for unsaved items
      item1 = flexmock('Item1')
      item2 = flexmock('Item2')
      @cache_entry.accessed_by.store(item1, true)
      @cache_entry.accessed_by.store(item2, true)
      item1.should_receive(:is_a?).with(Persistable).and_return(false)
      item2.should_receive(:is_a?).with(Persistable).and_return(true)
      item2.should_receive(:odba_cut_connection).with(@mock)\
        .times(1).and_return { assert(true) }
      @cache_entry.odba_cut_connections!
    end
    def test_odba_replace
      ## transaction-rollback for saved items
      reloaded = flexmock('Reloaded')
      item1 = flexmock('Item1')
      item2 = flexmock('Item2')
      @cache_entry.accessed_by.store(item1, true)
      @cache_entry.accessed_by.store(item2, true)
      item1.should_receive(:odba_replace).with(reloaded)\
        .times(1).and_return { assert(true) }
      item2.should_receive(:odba_replace).with(reloaded)\
        .times(1).and_return { assert(true) }
      @cache_entry.odba_replace!(reloaded)
      assert_equal(reloaded, @cache_entry.odba_object)
    end
    def test_odba_notify_observers
      @mock.should_receive(:odba_notify_observers).with(:foo, 2, 'bar')\
        .and_return { assert(true) }
      @cache_entry.odba_notify_observers(:foo, 2, 'bar')
    end
	end
end
