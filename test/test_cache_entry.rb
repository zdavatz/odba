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
      @mock.should_receive(:odba_add_observer)
			@cache_entry = ODBA::CacheEntry.new(@mock)
			ODBA.cache = flexmock("cache")
      ODBA.cache.should_receive(:retire_age).and_return(0.9)
      ODBA.cache.should_receive(:destroy_age).and_return(0.9)
		end
		def test_retire_check
			@mock.mock_handle(:odba_unsaved?) { false }
			@mock.mock_handle(:odba_unsaved?) { false }
			assert_equal(false, @cache_entry.odba_old?(Time.now - 1))
			assert_equal(true, @cache_entry.odba_old?(Time.now + 1))
		end
		def test_ready_to_destroy_true
			@mock.mock_handle(:odba_unsaved?) { false }
			assert_equal(true, @cache_entry.ready_to_destroy?(Time.now + 1))
		end
		def test_ready_to_destroy_false__age
			@mock.mock_handle(:odba_unsaved?) { false }
			assert_equal(false, @cache_entry.ready_to_destroy?)
		end
		def test_ready_to_destroy_false__accessed_by
			@mock.mock_handle(:odba_unsaved?) { false }
			@cache_entry.accessed_by = ['foo']
			assert_equal(false, @cache_entry.ready_to_destroy?(Time.now + 1))
		end
		def test_ready_to_destroy_false__combined
			@mock.mock_handle(:odba_unsaved?) { false }
			@cache_entry.accessed_by = ['foo']
			assert_equal(false, @cache_entry.ready_to_destroy?)
		end
		def test_retire
			obj_1 = Object.new
      obj_1.extend(Persistable)
			obj_1.instance_variable_set('@odba_id', 36)
			obj_2 = Object.new
      obj_2.extend(Persistable)
			obj_2.instance_variable_set('@odba_id', 34)
      ODBA.cache.should_receive(:include?).with(34).and_return(false)
      ODBA.cache.should_receive(:include?).with(35).and_return(false)
      ODBA.cache.should_receive(:include?).with(36).and_return(true)
      ODBA.cache.should_receive(:fetch).with(36).and_return(obj_1)
			hash = {}
			hash.instance_variable_set('@odba_id', 35)
			@cache_entry.accessed_by = {
        obj_1.object_id => 36, 
        obj_2.object_id => 34, 
        hash.object_id  => 35,
      }
			@cache_entry.odba_retire
			#assert_equal({hash => true}, @cache_entry.accessed_by)
			assert_equal({}, @cache_entry.accessed_by)
		end
		def test_odba_add_reference
			mock = flexmock
			@cache_entry.odba_add_reference(mock)
      id = mock.object_id
      assert_equal({id => nil}, @cache_entry.accessed_by)
      mock2 = flexmock
      mock2.should_receive(:odba_id).and_return(123)
			@cache_entry.odba_add_reference(mock2)
      assert_equal({mock2.object_id => 123, id => nil}, @cache_entry.accessed_by)
		end
		def test_odba_id
			@mock.mock_handle(:odba_id) { 123 }
			assert_equal(123, @cache_entry.odba_id)
		end
    def test_odba_cut_connections
      ## transaction-rollback for unsaved items
      item1 = flexmock('Item1')
      item2 = flexmock('Item2')
      @cache_entry.accessed_by.store(item1.object_id, nil)
      @cache_entry.accessed_by.store(item2.object_id, nil)
      item1.should_receive(:is_a?).with(Persistable).and_return(false)
      item2.should_receive(:is_a?).with(Persistable).and_return(true)
      item2.should_receive(:odba_cut_connection).with(@mock)\
        .times(1).and_return { assert(true) }
      @cache_entry.odba_cut_connections!
    end
    def test_odba_replace
      ## transaction-rollback for saved items
      modified = Object.new
      modified.extend(ODBA::Persistable)
      modified.instance_variable_set('@data', 'foo')
      cache_entry = CacheEntry.new(modified)

      reloaded = modified.dup

      modified.instance_variable_set('@data', 'bar')
      assert_equal('bar', modified.instance_variable_get('@data'))
      
      cache_entry.odba_replace!(reloaded)
      assert_equal('foo', modified.instance_variable_get('@data'))
    end
    def test_odba_notify_observers
      @mock.should_receive(:odba_notify_observers).with(:foo, 2, 'bar')\
        .and_return { assert(true) }
      @cache_entry.odba_notify_observers(:foo, 2, 'bar')
    end
	end
end
