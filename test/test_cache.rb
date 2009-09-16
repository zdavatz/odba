#!/usr/bin/env ruby

$: << File.dirname(__FILE__)
$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'test/unit'
require 'flexmock'
require 'odba/cache'
require 'odba/cache_entry'
require 'odba/persistable'
require 'odba/index'
require 'odba/index_definition'
require 'odba/odba_error'
require 'odba/odba'

module ODBA
	class Cache 
		CLEANING_INTERVAL = 0
		MAIL_RECIPIENTS = []
		MAIL_FROM = "test@testfirst.local"
		attr_accessor :cleaner, :fetched, :cache_entry_factory, :prefetched
		attr_writer :indices
		public :load_object
	end
	class TestCache < Test::Unit::TestCase
    include FlexMock::TestCase
		class ODBAContainer
		 include ODBA::Persistable
		 attr_accessor	:odba_connection, :odba_id
		end
		def setup
			@storage = ODBA.storage = flexmock("storage")
			@marshal = ODBA.marshaller = flexmock("marshaller")
			ODBA.cache = @cache = ODBA::Cache.instance
			@cache.fetched = {}
			@cache.prefetched = {}
			@cache.indices = {}
		end
		def teardown
      super
			ODBA.storage = nil
			ODBA.marshaller = nil
			@cache.fetched.clear
			@cache.prefetched.clear
			@cache.indices.clear
		end
		def test_fetch_named_ok
			obj = flexmock
			obj.instance_variable_set("@odba_name", 'the_name')
			obj.instance_variable_set("@odba_id", 2)
      @marshal.should_receive(:load).with('dump2').and_return(obj)
			@storage.should_receive(:restore_named).and_return('dump2')
			@storage.should_receive(:restore_collection).and_return([])
			load_1 = @cache.fetch_named('the_name', nil)
			assert_instance_of(FlexMock, load_1)
			assert_equal('the_name', load_1.odba_name)
			assert_equal(2, load_1.odba_id)
			load_2 = @cache.fetch_named('the_name', nil)
			assert_equal(load_1, load_2)
			load_3 = @cache.fetch(2, nil)
			assert_equal(load_1, load_3)
		end
		def test_bulk_fetch_load_all
			old_marshal = ODBA.marshaller
			ODBA.marshaller = marshal = flexmock('Marshal')
			array = [2, 3]
			obj1 = Object.new
			obj1.instance_variable_set("@odba_id", 2)
			obj1.instance_variable_set("@odba_prefetch", false)
			obj1.instance_variable_set("@odba_name", nil)
			obj2 = Object.new
			obj2.instance_variable_set("@odba_id", 3)
			obj2.instance_variable_set("@odba_prefetch", true)
			obj2.instance_variable_set("@odba_name", nil)
			dump_1 = Marshal.dump(obj1)
			dump_2 = Marshal.dump(obj2)
			@storage.should_receive(:bulk_restore)\
        .and_return([[2, dump_1],[3, dump_2]])
      dumps = [dump_1, dump_2]
      objs = [obj1, obj2]
			marshal.mock_handle(:load) { |dump|
				assert_equal(dumps.shift, dump)
				objs.shift
			}
			@storage.should_receive(:restore_collection).and_return([])
			loaded = @cache.bulk_fetch(array, nil)
			marshal.mock_verify
			assert_equal([obj1, obj2], loaded)
			assert_equal([2], @cache.fetched.keys)
			assert_equal([3], @cache.prefetched.keys)
			assert_instance_of(CacheEntry, @cache.fetched[2])
			assert_instance_of(CacheEntry, @cache.prefetched[3])
		ensure
			ODBA.marshaller = old_marshal
		end
		def test_bulk_fetch
			old_marshal = ODBA.marshaller
			ODBA.marshaller = marshal = flexmock('Marshal')
			array = [2, 3, 7]
			baz = flexmock('loaded')
			baz.mock_handle(:odba_add_reference){ |odba_caller| }
			baz.mock_handle(:odba_object){ 'foo' }
			@cache.fetched = {
				7 => baz
			}
			obj1 = Object.new
			obj1.instance_variable_set("@odba_id", 2)
			obj1.instance_variable_set("@odba_prefetch", false)
			obj1.instance_variable_set("@odba_name", nil)
			obj2 = Object.new
			obj2.instance_variable_set("@odba_id", 3)
			obj2.instance_variable_set("@odba_prefetch", false)
			obj2.instance_variable_set("@odba_name", nil)
			dump_1 = Marshal.dump(obj1)
			dump_2 = Marshal.dump(obj2)
      dumps = [dump_1, dump_2]
      objs = [obj1, obj2]
			marshal.mock_handle(:load) { |dump|
				assert_equal(dumps.shift, dump)
				objs.shift
			}
			@storage.should_receive(:bulk_restore).and_return([[2, dump_1],[3, dump_2]])
			@storage.should_receive(:restore_collection).and_return([])
			@cache.bulk_fetch(array, nil)
			assert_equal(true, @cache.fetched.has_key?(2))
			assert_equal(3, @cache.fetched.size)
			assert_equal(true, @cache.fetched.has_key?(3))
			baz.mock_verify
			ODBA.marshaller = old_marshal
		end
		def test_bulk_restore_in_fetchedadd_caller
			obj = Object.new
			cache_entry = flexmock('cache_entry')
			cache_entry.mock_handle(:odba_object){
				obj
			}
			cache_entry.mock_handle(:odba_add_reference){|caller|
				#this assert_equal is interesting
				assert_equal('Reference', caller)
			}
			@cache.fetched.store(1, cache_entry)
			rows = [[1, '']]
			retrieved = @cache.bulk_restore(rows, 'Reference')
		end
		def test_clean
			obj1 = flexmock
			obj2 = flexmock
			@cache.fetched.store(2, obj1)
			@cache.fetched.store(3, obj2)
			assert_equal(2, @cache.fetched.size)
			obj1.mock_handle(:odba_old?) { true }
			obj1.mock_handle(:odba_retire) { }
			obj2.mock_handle(:odba_old?) { false }
			@cache.clean
			assert_equal(2, @cache.fetched.size)
			obj1.mock_verify
			obj2.mock_verify
		end
		def test_clean__prefetched
			obj1 = flexmock
			obj2 = flexmock
			@cache.prefetched.store(2, obj1)
			@cache.prefetched.store(3, obj2)
			assert_equal(2, @cache.prefetched.size)
			obj1.mock_handle(:ready_to_destroy?) { false }
			obj1.mock_handle(:odba_old?) { true }
			obj1.mock_handle(:odba_retire) { }
			obj2.mock_handle(:ready_to_destroy?) { false }
			obj2.mock_handle(:odba_old?) { false }
			@cache.clean_prefetched
			assert_equal(2, @cache.prefetched.size)
			obj1.mock_verify
			obj2.mock_verify
		end
		def test_clear
			value = flexmock("value")
			obj = flexmock('object')
			prefetched = flexmock('prefetched')
			@cache.fetched.store(12, value)
			@cache.prefetched.store(13, prefetched)
			assert_equal(2, @cache.size)
      @cache.clear
			value.mock_verify
			obj.mock_verify
			assert_equal(0, @cache.size)
		end
		def test_fetch_named_block
			restore = flexmock("restore")
			caller = flexmock("caller")
			caller2 = flexmock("caller2")
			@storage.mock_handle(:restore_named) { |name| }
			restore.mock_handle(:odba_name=) { |name| }
			restore.mock_handle(:odba_store){ |obj| }
			result = @cache.fetch_named("foo", caller2) {
				restore
			}
			assert_equal(restore, result)
			restore.mock_verify
		end
		def prepare_fetch(id, receiver)
			@storage.mock_handle(:restore){ |odba_id|
				assert_equal(id, odba_id)
				odba_id
			}
			@marshal.mock_handle(:load) { receiver }
			if(receiver.is_a?(FlexMock))
				receiver.mock_handle(:odba_restore) {}
				receiver.mock_handle(:odba_name) {}
			end
		end
		def test_fetch_has_name
			caller1 = flexmock('Caller1')
			caller2 = flexmock('Caller2')
			caller3 = flexmock('Caller3')
			receiver = flexmock('Receiver')
			@storage.mock_handle(:restore){ |odba_id|
				assert_equal(23, odba_id)
				odba_id
			}
			@marshal.mock_handle(:load){|dump|
				receiver
			}
			@storage.mock_handle(:restore_collection){|*args| 
				[]
			}
			receiver.instance_variable_set("@odba_id", 23)
			receiver.instance_variable_set("@odba_name", 'name')
			first_fetch = @cache.fetch(23, caller1)
			assert_equal(receiver, first_fetch)
			assert_equal(2, @cache.fetched.size)
			assert(@cache.fetched.include?('name'))
			second_fetch = @cache.fetch(23, caller2)
			assert_equal(receiver, second_fetch)
			named_fetch = @cache.fetch_named('name', caller3)
			assert_equal(receiver, named_fetch)
			receiver.mock_verify
		end
		def test_fetch_error
			receiver = flexmock
			@storage.mock_handle(:restore) { |odba_id|
				nil	
			}
			assert_raises(OdbaError) {
				@cache.load_object(23, receiver)
			}
		end
		def test_fetch__adds_reference
			obj = flexmock
			obj.instance_variable_set("@odba_id", 23)
			obj.instance_variable_set("@odba_prefetch", false)
			@storage.mock_handle(:restore) { |id|
				assert_equal(23, id)
				'dump'
			}
			@storage.mock_handle(:restore_collection) { |id|
				[]
			}
			@marshal.mock_handle(:load) { |dump|
				assert_equal('dump', dump)
				obj
			}
			callr = flexmock
			res = @cache.fetch(23, callr)
			cache_entry = @cache.fetched[23]
			assert_instance_of(CacheEntry, cache_entry)
      assert_equal([callr.object_id], cache_entry.accessed_by.keys)
			assert_equal(obj, res)
      ## test for duplicates
			@cache.fetch(23, callr)
      assert_equal([callr.object_id], cache_entry.accessed_by.keys)
		end
		def test_store
      cont = flexmock('CacheEntry')
      @cache.fetched.store(3, cont)
			save_obj = flexmock("save_obj")
      save_obj.should_receive(:odba_target_ids).and_return([3])
      save_obj.should_receive(:odba_observers).and_return { [] }
			prepare_store([save_obj])
      save_obj.should_receive(:odba_add_observer)
      cont.should_receive(:odba_add_reference).with(save_obj)\
        .times(1).and_return { assert(true) }
			@cache.store(save_obj)
		end
		def test_store_collection_elements
			old_mar = ODBA.marshaller
			ODBA.marshaller = Marshal

			old_collection = [['key1', 'val1'], ['key2', 'val2']]
			new_collection = [['key2', 'val2'], ['key3', 'val3']]

			cache_entry = flexmock('cache_entry')
			cache_entry.mock_handle(:collection) { old_collection }
			cache_entry.mock_handle(:collection=) { |col|
				assert_equal(new_collection, col)
			}

      @storage.mock_handle(:restore_collection) { 
        old_collection.collect { |key, val|
          [Marshal.dump(key.odba_isolated_stub), 
            Marshal.dump(val.odba_isolated_stub)]
        }
      }
			@storage.mock_handle(:collection_remove) { |odba_id, key| 
				assert_equal(54, odba_id)
				assert_equal(Marshal.dump('key1'.odba_isolated_stub), key)
			}
			@storage.mock_handle(:collection_store) { |odba_id, key, value| 
				assert_equal(54, odba_id)
				assert_equal(Marshal.dump('key3'.odba_isolated_stub), key)
				assert_equal(Marshal.dump('val3'.odba_isolated_stub), value)
			}

			@cache.fetched = {
				54 => cache_entry
			}
			
			obj = flexmock('Obj')
			obj.mock_handle(:odba_id) { 54 }
			obj.mock_handle(:odba_collection) { new_collection }
			@cache.store_collection_elements(obj)
    ensure
			ODBA.marshaller = old_mar
		end
		def test_store_object_connections
			@storage.mock_handle(:ensure_object_connections) { |id,target_ids|
				assert_equal(4, id)
				assert_equal([1,2], target_ids)
			}
			@cache.store_object_connections(4, [1,2])
		end
		def test_load__and_restore_object
			caller1 = flexmock
			loaded = flexmock
			@storage.mock_handle(:restore) { |odba_id|
				assert_equal(23, odba_id)
				'dump'
			}
			@marshal.mock_handle(:load) { |dump|
				assert_equal('dump', dump)
				loaded
			}
			@storage.mock_handle(:restore_collection) { [] }

      loaded.instance_variable_set('@odba_id', 23)
			@cache.load_object(23, caller1)
			loaded.mock_verify
		end
		def test_prefetch
			foo = flexmock("foo")
			@storage.mock_handle(:restore_prefetchable) {
				[[2, foo]]
			}
			prepare_bulk_restore([foo])
			assert_nothing_raised {
				@cache.prefetch
			}
			foo.mock_verify
		end
		def test_fill_index
			foo = flexmock("foo")
			foo.mock_handle(:fill) { |target| 
				assert_equal("baz", target)
			}
			@cache.indices = { 
				"foo" => foo
			}
			@cache.fill_index("foo", "baz")
			foo.mock_verify
		end
		def test_create_index
      df = IndexDefinition.new
      df.index_name = 'index'
			indices = flexmock('indices')
			indices.should_receive(:store).and_return { |key, val|
				assert_instance_of(Index, val)
			}
			indices.should_receive(:odba_store_unsaved).times(1)
			@cache.instance_variable_set('@indices', indices)
			@storage.mock_handle(:transaction) { |block| block.call }
			@storage.should_receive(:create_index).times(1)
			index = @cache.create_index(df, flexmock)
			assert_instance_of(Index, index)
			@cache.instance_variable_set('@indices', {})
		end
		def test_create_index__fulltext
      df = IndexDefinition.new
      df.index_name = 'index'
      df.fulltext = true
			indices = flexmock('indices')
			indices.should_receive(:store).and_return { |key, val|
				assert_instance_of(FulltextIndex, val)
			}
			indices.should_receive(:odba_store_unsaved).times(1)
			@cache.instance_variable_set('@indices', indices)
			@storage.mock_handle(:transaction) { |block| block.call }
			@storage.should_receive(:create_fulltext_index).times(1)
			index = @cache.create_index(df, flexmock)
			assert_instance_of(FulltextIndex, index)
			@cache.instance_variable_set('@indices', {})
		end
		def test_create_index__condition
      df = IndexDefinition.new
      df.index_name = 'index'
      df.resolve_search_term = { }
			indices = flexmock('indices')
			indices.should_receive(:store).and_return { |key, val|
				assert_instance_of(ConditionIndex, val)
			}
			indices.should_receive(:odba_store_unsaved).times(1)
			@cache.instance_variable_set('@indices', indices)
			@storage.mock_handle(:transaction) { |block| block.call }
			@storage.should_receive(:create_condition_index).times(1)
			index = @cache.create_index(df, flexmock)
			assert_instance_of(ConditionIndex, index)
			@cache.instance_variable_set('@indices', {})
		end
		def prepare_fetch_collection(obj)
		end
		def prepare_store(store_array, &block)
			store_array.each { |mock|
				# store
				mock.mock_handle(:odba_id){ || }
				mock.mock_handle(:odba_name){ || }
        mock.mock_handle(:odba_notify_observers) { |key, id1, id2|
          assert_equal(:store, key)
        }
				mock.mock_handle(:odba_isolated_dump){ || }

				# store_collection_elements
				mock.mock_handle(:odba_id){ || }
				mock.mock_handle(:odba_collection){|| []}
				mock.mock_handle(:odba_id){ || }

				# store
				mock.mock_handle(:odba_prefetch?){ || }

				# store_object_connections
				mock.mock_handle(:odba_target_ids){ || [] }

				# update_indices
				mock.mock_handle(:odba_indexable?){}

				# store_cache_entry
				mock.mock_handle(:odba_prefetch?){ || }
				mock.mock_handle(:odba_collection){ || [] }

				@storage.mock_handle(:restore_collection) { [] }
				if(block)
					@storage.mock_handle(:store, &block) 
				else
					@storage.mock_handle(:store) { 
						assert(true)
					}
				end
				@storage.mock_handle(:ensure_object_connections){|*args|}
			}
		end
		def test_delete
			delete_item = ODBAContainer.new
			delete_item.odba_id = 1
			origin_obj = ODBAContainer.new
			origin_obj.odba_id = 2
			origin_obj.odba_connection = delete_item
			@cache.fetched.store(1, delete_item)
			@storage.mock_handle(:retrieve_connected_objects) { |id|
				[[2]] 
			}
			prepare_fetch(2, origin_obj)
			@storage.mock_handle(:restore_collection) { |*args| 
				[]
			}
			@storage.mock_handle(:store) { |id, dump, name, prefetch, klass| }
			@storage.mock_handle(:ensure_object_connections) { } 
			@storage.mock_handle(:delete_persistable) { |id| } 
			@marshal.mock_handle(:dump) { |ob| "foo"}
			@cache.delete(delete_item)
			assert_equal(1, @cache.fetched.size)
			assert_equal(nil, origin_obj.odba_connection)
		end
		def prepare_delete(mock, name, id)
			mock.mock_handle(:odba_id) { id }
			mock.mock_handle(:odba_name) { name }
			mock.mock_handle(:odba_notify_observers) { |key, id1, id2|
        assert_equal(:delete, key) 
      }
			@storage.mock_handle(:retrieve_connected_objects) { |id|
				[]
			}
			mock.mock_handle(:origin_class?) { true }
			mock.mock_handle(:odba_id) { id }
			@storage.mock_handle(:delete_persistable) { |id_arg| 
				assert_equal(id, id_arg)
			}
			@storage.mock_handle(:delete_index_element) { }
		end
		def prepare_bulk_restore(rows)
			rows.each { |odba_mock|
        ## according to recent changes, objects are extended with 
        #  ODBA::Persistable after loading - this enables ad-hoc storing
        #  but messes up loads of tests
				@marshal.mock_handle(:load) { |dump|
					odba_mock.instance_variable_set('@odba_id', 2)
					odba_mock.instance_variable_set('@odba_prefetch', true)
          odba_mock
				}
				@storage.mock_handle(:restore_collection){|*args|
					[]
				}
			}
		end
		def test_retrieve_from_index
			foo = flexmock
			index = flexmock("bar_index")
			@cache.indices["index_name"] = index
			index.mock_handle(:fetch_ids) { |search_term, meta|
				assert_equal('search term', search_term)
				assert_nil(meta)
				[2]
			}
			@storage.mock_handle(:bulk_restore) {
				[[2, 'dump']]
			}
			prepare_bulk_restore([foo])
			@cache.retrieve_from_index("index_name", "search term")
			foo.mock_verify
			index.mock_verify
		end
		def test_update_indices
			index = flexmock("index")
			bar = flexmock("bar")
			bar.mock_handle(:odba_indexable?){ true }
			@cache.indices = {
				"foo" => index
			}
			index.mock_handle(:update){ |obj|
				assert_equal(bar, obj)
			}
			@cache.update_indices(bar)
			bar.mock_verify
			index.mock_verify
		end
		def test_delete_index_element
			foo = flexmock("foo")
			bar = flexmock("bar")
			@cache.indices = {
				"foo" => foo
			}
			foo.should_receive(:delete).with(bar).times(1).and_return {
        assert(true)
      }
			@cache.delete_index_element(bar)
		end
		def test_drop_index
			@storage.mock_handle(:transaction) { |block| block.call }
			@storage.mock_handle(:drop_index) { |index_name|
				assert_equal("foo_index", index_name)
			}
			index = flexmock("index")
      index.should_receive(:delete).with(index)
			prepare_delete(index, "foo", 2)
			@cache.indices.store("foo_index", index)
			@cache.drop_index("foo_index")
			index.mock_verify
		end
		def test_drop_indices
			@storage.mock_handle(:transaction) { |block| block.call}
			@storage.mock_handle(:drop_index){|index_name|
				assert_equal("foo_index", index_name)
			}
			index = flexmock("index")
      index.should_receive(:delete).with(index)
			prepare_delete(index, "foo", 2)
			@cache.indices.store("foo_index", index)
			@cache.drop_indices
			index.mock_verify
		end
		def test_fetch_collection_element
			key_dump = Marshal.dump('foo')
			@storage.mock_handle(:collection_fetch) { |odba_id, key|
				assert_equal(12, odba_id)
				assert_equal(key_dump, key)
				'val_dump'
			}
			@marshal.mock_handle(:dump) { |key|
				assert_equal('foo', key)
				key_dump
			}
			@marshal.mock_handle(:load) { |dump|
				assert_equal('val_dump', dump)
				'val'
			}
			res = @cache.fetch_collection_element(12, 'foo')
			assert_equal('val', res)
		end
    def test_transaction
      o1 = Object.new
      o1.instance_variable_set('@odba_id', 1)
      o2 = Object.new
      o3 = Object.new
      o1.extend(ODBA::Persistable)
      o2.extend(ODBA::Persistable)
      o2.odba_name = 'name2'
      o3.extend(ODBA::Persistable)
      o4 = o1.odba_dup
      @storage.mock_handle(:transaction) { |block| block.call }

      ## store o1
      @marshal.should_receive(:dump).times(3).and_return { |obj|
        "dump%i" % obj.odba_id
      } 
      next_id = 1
      @storage.should_receive(:next_id).and_return { next_id += 1 }
      @storage.should_receive(:store).with(1,'dump1',nil,nil,Object)\
        .times(1).and_return { assert(true) }
      @storage.should_receive(:ensure_object_connections)\
        .with(1,[2]).times(1).and_return { assert(true) }

      ## store o2
      @storage.should_receive(:restore_collection).with(2)\
        .times(1).and_return([])
      @storage.should_receive(:store)\
        .with(2,'dump2','name2',nil,Object)\
        .times(1).and_return { assert(true) }
      @storage.should_receive(:ensure_object_connections)\
        .with(2,[3]).times(1).and_return { assert(true) }

      ## store o3 and raise
      @storage.should_receive(:restore_collection).with(3)\
        .times(1).and_return([])
      ## at this stage 1 and 2 (and 'name2') are stored:
      @storage.should_receive(:store)\
        .with(3,'dump3',nil,nil,Object)\
        .times(1).and_return { raise "trigger rollback" }

      ## rollback
      @storage.should_receive(:restore).with(2)\
        .times(1).and_return(nil)
      @storage.should_receive(:restore).with(1)\
        .times(1).and_return('dump1')
      @storage.should_receive(:restore_collection).with(1)\
        .times(2).and_return([])
      @marshal.should_receive(:load).with('dump1')\
        .times(1).and_return(o4)
      @cache.fetched.store(1, ODBA::CacheEntry.new(o1))
      assert_raises(RuntimeError) {
        ODBA.transaction { 
          o2.instance_variable_set('@other', o3)
          o1.instance_variable_set('@other', o2)
          o1.odba_store
        }
      }
      assert_equal(1, @cache.size)
      assert_nil(o1.instance_variable_get('@other'))
    end
    def test_extent
      o1 = flexmock('O1')
      o1.should_receive(:odba_id).and_return(1)
      o2 = flexmock('O2')
      o2.should_receive(:odba_id).and_return(2)
      clr = flexmock('Caller')
      @storage.should_receive(:extent_ids).and_return([1,2])
      @storage.should_receive(:restore_collection).and_return([])
      @storage.should_receive(:bulk_restore).with([1,2])\
        .and_return([[1, 'dump1'],[2,'dump2']])
      @marshal.should_receive(:load).with('dump1')\
        .times(1).and_return(o1)
      @marshal.should_receive(:load).with('dump2')\
        .times(1).and_return(o2)
      assert_equal([o1, o2], @cache.extent(ODBAContainer, clr))
    end
    def test_fetch_collection
      obj = flexmock('Object')
      obj.should_receive(:odba_id).and_return(1)
      restored = flexmock('Restored')
      restored.should_receive(:odba_id).and_return(7)
      i1 = flexmock('Item1')
      i2 = flexmock('Item2')
      i1.should_receive(:is_a?).with(Stub).and_return(false)
      i2.should_receive(:is_a?).with(Stub).and_return(true)
      i2.should_receive(:odba_id).and_return(7)
      i2.should_receive(:odba_container=).with(obj).times(1)
      @storage.should_receive(:restore_collection).with(1)\
        .times(1).and_return([['keydump1','dump1'],['keydump2','dump2']])
      @storage.should_receive(:restore_collection).with(7).times(1).and_return([])
      @storage.should_receive(:bulk_restore).with([7]).and_return([[7,'inst']])
      @marshal.should_receive(:load).with('keydump1').and_return(0)
      @marshal.should_receive(:load).with('keydump2').and_return(1)
      @marshal.should_receive(:load).with('dump1').and_return(i1)
      @marshal.should_receive(:load).with('dump2').and_return(i2)
      @marshal.should_receive(:load).with('inst').and_return(restored)
      assert_equal([[0,i1],[1,i2]], @cache.fetch_collection(obj))
    end
    def test_include
      assert(!@cache.include?(1))
      @cache.fetched.store(1, 'foo')
      assert(@cache.include?(1))
      assert(!@cache.include?(2))
      @cache.prefetched.store(2, 'bar')
      assert(@cache.include?(2))
    end
    def test_index_keys
      index = flexmock('Index')
      @cache.indices.store('index', index)
      index.should_receive(:keys).with(nil).times(1).and_return(['ABC'])
      index.should_receive(:keys).with(2).times(1).and_return(['AB'])
      assert_equal(['ABC'], @cache.index_keys('index'))
      assert_equal(['AB'], @cache.index_keys('index', 2))
    end
    def test_setup
      @storage.should_receive(:setup).times(1).and_return { assert(true)}
      @storage.should_receive(:ensure_target_id_index)\
        .with('index').times(1).and_return { assert(true)}
      df1 = IndexDefinition.new
      df1.index_name = 'deferred'
      df2 = IndexDefinition.new
      df2.index_name = 'index'
			indices = flexmock('indices')
      indices.should_receive(:each_key).and_return { |block|
        block.call('index')
      }
      indices.should_receive(:include?).with('index')\
        .times(1).and_return(true)
      indices.should_receive(:include?).with('deferred')\
        .times(1).and_return(false)
			indices.should_receive(:store).and_return { |key, val|
        assert_equal('deferred', key)
				assert_instance_of(Index, val)
			}
			indices.should_receive(:odba_store_unsaved).times(1)
			@cache.instance_variable_set('@indices', indices)
      @storage.should_receive(:transaction).and_return { |block|
        block.call
      }
      @storage.should_receive(:create_index).with('deferred')\
        .times(1).and_return { assert(true) }
      @cache.instance_variable_set('@deferred_indices', [df1, df2])
      @cache.setup
			@cache.instance_variable_set('@indices', {})
    end
	end
end
