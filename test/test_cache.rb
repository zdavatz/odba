#!/usr/bin/env ruby

$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'test/unit'
require 'mock'
require 'odba'

class Mock
	def odba_id
		1
	end
end

module ODBA
	class Cache < SimpleDelegator
		CLEANING_INTERVAL = 0
		MAIL_RECIPIENTS = []
		MAIL_FROM = "test@testfirst.local"
		attr_accessor :cleaner, :hash, :batch_objects, :batch_mode,
									:batch_deletions, :batch_mode, :cache_entry_factory
		attr_writer :indices
		public :load_object
	end
	class TestCache < Test::Unit::TestCase
		class Mock < Mock
			ODBA_PREFETCH = false
		end
		class CountingStub
			def initialize
				@called = {}
			end
			def called?(meth)
				@called[meth]
			end 
			def method_missing(meth, *args, &block)
				@called[meth] = @called[meth].to_i.next
			end
			def __define(meth, result)
				instance_variable_set("@#{meth}", result)
				eval <<-EOS
					def #{meth}(*args)
						@#{meth}
					end
				EOS
			end
		end
		class ODBAContainer
		 include ODBA::Persistable
		 attr_accessor	:odba_connection, :odba_id
		end
		def setup
			@storage = ODBA.storage = Mock.new("storage")
			ODBA.scalar_cache = Mock.new("scalar")
			@marshal = ODBA.marshaller = Mock.new("marshaller")
			ODBA.cache_server = @cache = ODBA::Cache.instance
			@cache.hash = {}
			@cache.indices = {}
			@cache.batch_objects = {}
			@cache.batch_deletions = {}
			@cache.batch_mode = false
		end
		def teardown
			ODBA.storage.__verify
			ODBA.scalar_cache.__verify
			ODBA.storage = nil
			ODBA.marshaller = nil
			ODBA.scalar_cache = nil
			@cache.hash.clear
			@cache.indices.clear
			@cache.batch_objects.clear
			@cache.batch_deletions.clear
			@cache.batch_mode = false
		end
		def test_fetch_named_ok
			old_marshaller = ODBA.marshaller
			ODBA.marshaller = Marshal
			obj = CountingStub.new
			obj.__define(:odba_name, 'the_name')
			obj.__define(:odba_id, 2)
			storage = CountingStub.new
			storage.__define(:restore_named, ODBA.marshaller.dump(obj))
			storage.__define(:restore_collection, [])
			ODBA.storage = storage
			load_1 = ODBA.cache_server.fetch_named('the_name', nil)
			assert_instance_of(CountingStub, load_1)
			assert_equal('the_name', load_1.odba_name)
			assert_equal(2, load_1.odba_id)
			load_2 = ODBA.cache_server.fetch_named('the_name', nil)
			assert_equal(load_1, load_2)
			load_3 = ODBA.cache_server.fetch(2, nil)
			assert_equal(load_1, load_3)
		ensure
			ODBA.marshaller = old_marshaller
		end
		def test_bulk_fetch_load_all
			old_marshal = ODBA.marshaller
			ODBA.marshaller = Marshal
			array = [2, 3]
			storage = CountingStub.new
			obj1 = CountingStub.new
			obj1.__define(:odba_id, 2)
			obj2 = CountingStub.new
			obj2.__define(:odba_id, 3)
			dump_1 = Marshal.dump(obj1)
			dump_2 = Marshal.dump(obj2)
			storage.__define(:bulk_restore, [[2, dump_1],[3, dump_2]])
			storage.__define(:restore_collection, [])
			ODBA.storage = storage
			@cache.bulk_fetch(array, nil)
			assert_equal(true, @cache.hash.has_key?(2))
			assert_equal(2, @cache.hash.size)
			assert_equal(true, @cache.hash.has_key?(3))
			ODBA.marshaller = old_marshal
		end
		def test_bulk_fetch
			old_marshal = ODBA.marshaller
			ODBA.marshaller = Marshal
			array = [2, 3, 7]
			baz = Mock.new('loaded')
			baz.__next(:odba_add_reference){|odba_caller|}
			@cache.hash = {
				7 => baz
			}
			storage = CountingStub.new
			obj1 = CountingStub.new
			obj1.__define(:odba_id, 2)
			obj2 = CountingStub.new
			obj2.__define(:odba_id, 3)
			dump_1 = Marshal.dump(obj1)
			dump_2 = Marshal.dump(obj2)
			storage.__define(:bulk_restore, [[2, dump_1],[3, dump_2]])
			storage.__define(:restore_collection, [])
			ODBA.storage = storage
			@cache.bulk_fetch(array, nil)
			assert_equal(true, @cache.hash.has_key?(2))
			assert_equal(3, @cache.hash.size)
			assert_equal(true, @cache.hash.has_key?(3))
			baz.__verify
			ODBA.marshaller = old_marshal
		end
		def test_bulk_restore_in_hashadd_caller
			obj = Object.new
			cache_entry = Mock.new('cache_entry')
			cache_entry.__next(:odba_object){
				obj
			}
			cache_entry.__next(:odba_add_reference){|caller|
				#this assert_equal is interesting
				assert_equal('Reference', caller)
			}
			@cache.hash.store(1, cache_entry)
			rows = [[1, '']]
			retrieved = @cache.bulk_restore(rows, 'Reference')
		end
		def test_clean
			obj1 = Mock.new
			obj2 = Mock.new
			@cache.hash.store(2, obj1)
			@cache.hash.store(3, obj2)
			assert_equal(2, @cache.hash.size)
			obj1.__next(:ready_to_destroy?) { false }
			obj1.__next(:odba_old?) { true }
			obj1.__next(:odba_retire) { }
			obj2.__next(:ready_to_destroy?) { false }
			obj2.__next(:odba_old?) { false }
			@cache.clean
			assert_equal(2, @cache.hash.size)
			obj1.__verify
			obj2.__verify
		end
		def test_delete_old
			value = Mock.new("value")
			obj = Mock.new('object')
			@cache.hash.store(12, value)
			assert_equal(1, @cache.hash.size)
			value.__next(:ready_to_destroy?) { true }
			value.__next(:odba_object) { obj }
			obj.__next(:odba_id) { 12 }
			obj.__next(:odba_cache_values) { {} }
			@cache.delete_old
			value.__verify
			obj.__verify
			assert_equal(0, @cache.hash.size)
		end
		def test_fetch_named_block
			restore = Mock.new("restore")
			caller = Mock.new("caller")
			caller2 = Mock.new("caller2")
			ODBA.storage.__next(:restore_named) { |name| }
			restore.__next(:odba_name=) { |name| }
			restore.__next(:odba_store){ |obj| }
			result = @cache.fetch_named("foo", caller2) {
				restore
			}
			assert_equal(restore, result)
			restore.__verify
		end
		def prepare_fetch(id, receiver)
			ODBA.storage.__next(:restore){ |odba_id|
				assert_equal(id, odba_id)
				odba_id
			}
			ODBA.marshaller.__next(:load) { receiver }
			if (receiver.is_a?(Mock))
				receiver.__next(:odba_restore) {}
				receiver.__next(:odba_name) {}
			end
		end
		def test_fetch_has_name
			caller = Mock.new('Caller')
			caller2 = Mock.new('Caller2')
			caller3 = Mock.new('Caller3')
			receiver = Mock.new('Receiver')
			ODBA.storage.__next(:restore){ |odba_id|
				assert_equal(23, odba_id)
				odba_id
			}
			ODBA.marshaller.__next(:load){|dump|
				receiver
			}
		
			ODBA.storage.__next(:restore_collection){|*args| 
					[]
			}
			receiver.__next(:odba_id) {23}
			receiver.__next(:odba_restore) {}
			receiver.__next(:odba_id) {23}
			receiver.__next(:odba_name) { 'name' }
			receiver.__next(:odba_name) { 'name' }
			first_fetch = @cache.fetch(23, caller)
			assert_equal(receiver, first_fetch)
			assert_equal(2, @cache.hash.size)
			assert(@cache.hash.include?('name'))
			second_fetch = @cache.fetch(23, caller2)
			assert_equal(receiver, second_fetch)
			named_fetch = @cache.fetch_named('name', caller3)
			assert_equal(receiver, named_fetch)
			receiver.__verify
		end
		def test_fetch_error
			receiver = Mock.new
			ODBA.storage.__next(:restore) { |odba_id|
				nil	
			}
			assert_raises(OdbaError) {
				@cache.load_object(23, receiver)
			}
		end
		def test_fetch__adds_reference
			obj = CountingStub.new
			obj.__define(:odba_id, 23)
			@storage.__next(:restore) { |id|
				assert_equal(23, id)
				'dump'
			}
			@storage.__next(:restore_collection) { |id|
				[]
			}
			@marshal.__next(:load) { |dump|
				assert_equal('dump', dump)
				obj
			}
			caller = CountingStub.new
			res = @cache.fetch(23, caller)
			cache_entry = @cache.hash[23]
			assert_instance_of(CacheEntry, cache_entry)
			assert_equal([caller], cache_entry.accessed_by)
			assert_equal(obj, res)
		end
		def test_store
			save_obj = Mock.new("save_obj")
			prepare_store([save_obj])
			@cache.store(save_obj)
			save_obj.__verify
		end
		def test_store_collection_elements
			old_mar = ODBA.marshaller
			ODBA.marshaller = Marshal
			ODBA.storage.__next(:collection_remove){|id, key|}
			cache_entry = Mock.new('cache_entry')
			key = CountingStub.new
			value = CountingStub.new
			collection_entry = [[key, value]]
			cache_entry.__next(:collection){ collection_entry}
			cache_entry.__next(:collection=){ []}
			@cache.hash = {
				1 => cache_entry
			}
			obj = Mock.new('enumerable')
			
			obj.__next(:odba_id){1}
			obj.__next(:odba_collection){ []}
			
			@cache.store_collection_elements(obj)
			ODBA.marshaller = old_mar
		end
		def test_store_object_connections
			save = Mock.new("to_store")
			save.__next(:odba_name){ nil}
			save.__next(:odba_target_ids){ [1,2]}
			save.__next(:odba_id){ 4}
=begin
			ODBA.storage.__next(:add_object_connection){|id,target_id|
				assert_equal(4, id)
				assert_equal(1, target_id)
			}
			ODBA.storage.__next(:add_object_connection){|id,target_id|
				assert_equal(4, id)
				assert_equal(2, target_id)
			}
=end
			ODBA.storage.__next(:ensure_object_connections) { |id,target_ids|
				assert_equal(4, id)
				assert_equal([1,2], target_ids)
			}
			@cache.store_object_connections(save)
			save.__verify
		end
		def test_store_object_connection_named
			save = Mock.new("to_store")
			save.__next(:odba_name){ "foo" }
			save.__next(:odba_target_ids){ []}
			save.__next(:odba_id){ 4}
			ODBA.storage.__next(:ensure_object_connections) { |id,target_ids|
				assert_equal(4, id)
				assert_equal([4], target_ids)
			}
			@cache.store_object_connections(save)
			save.__verify
		end
		def test_load__and_restore_object
			receiver = Mock.new
			ODBA.storage.__next(:restore){ |odba_id|
				assert_equal(23, odba_id)
				odba_id
			}
			ODBA.storage.__next(:restore_collection){|*args| []}
			ODBA.marshaller.__next(:load){|dump|
				receiver
			}
			receiver.__next(:odba_id){23}
			receiver.__next(:odba_restore){|id|}
			receiver.__next(:odba_id){23}
			receiver.__next(:odba_name){23}
			receiver.__next(:odba_name){23}
			@cache.load_object(23, receiver)
			receiver.__verify
		end
		def test_reap_object_connection
			ODBA.storage.__next(:max_id){ 3 }
			ODBA.storage.__next(:remove_dead_objects) { |min, max| }
			ODBA.storage.__next(:remove_dead_connections) { |min, max| }
			assert_nothing_raised {
				@cache.reap_object_connections
			}
		end
		def test_prefetch
			foo = Mock.new("foo")
			ODBA.storage.__next(:restore_prefetchable) {
				[[2, foo]]
			}
			prepare_bulk_restore([foo])
			assert_nothing_raised {
				@cache.prefetch
			}
			foo.__verify
		end
		def test_fill_index
			foo = Mock.new("foo")
			foo.__next(:fill) { |target| 
				assert_equal("baz", target)
			}
			@cache.indices = { 
				"foo" => foo
			}
			@cache.fill_index("foo", "baz")
			foo.__verify
		end
		def test_create_index
			index_def_mock = CountingStub.new
			index_def_mock.__define(:index_name, "foo")
			indices = Mock.new('indices')
			indices.__next(:store){|key, val|
				assert_instance_of(FulltextIndex, val)
			}
			indices.__next(:odba_store_unsaved){}
			@cache.instance_variable_set('@indices', indices)
			ODBA.storage.__next(:transaction) { |block| block.call }
			ODBA.storage.__next(:create_fulltext_index) { |*args|}
			index = @cache.create_index(index_def_mock, CountingStub.new)
			assert_instance_of(FulltextIndex, index)
			@cache.instance_variable_set('@indices', {})
		end
		def prepare_fetch_collection(obj)
		end
		def prepare_store(store_array, &block)
			store_array.each{ |mock|
				mock.__next(:odba_id){ || }
				mock.__next(:odba_cache_values){ []}
				mock.__next(:odba_isolated_dump){ || }
				mock.__next(:odba_id){ || }
				mock.__next(:odba_collection){|| []}
				mock.__next(:odba_name){ || }
				mock.__next(:odba_prefetch?){ || }
				mock.__next(:odba_indexable?){}
				mock.__next(:odba_name){ || }
			  mock.__next(:odba_target_ids) { []}
				mock.__next(:odba_id){ || }
				if(block)
					ODBA.storage.__next(:store, &block) 
				else
					ODBA.storage.__next(:store) { 
						assert(true)
					}
				end
				ODBA.storage.__next(:ensure_object_connections){|*args|}
			}
		end
		def test_delete
			delete_item = ODBAContainer.new
			delete_item.odba_id = 1
			origin_obj = ODBAContainer.new
			origin_obj.odba_id = 2
			origin_obj.odba_connection = delete_item
			@cache.hash.store(1, delete_item)
			ODBA.storage.__next(:retrieve_connected_objects) { |id|
				[[2]] 
			}
			prepare_fetch(2, origin_obj)
			ODBA.storage.__next(:restore_collection) { |*args| 
				[]
			}
			ODBA.storage.__next(:store) { |id, dump, name, prefetch| }
			ODBA.storage.__next(:ensure_object_connections) { } 
			ODBA.storage.__next(:delete_persistable) { |id| } 
			ODBA.marshaller.__next(:dump) { |ob| "foo"}
			@cache.delete(delete_item)
			assert_equal(1, @cache.hash.size)
			assert_equal(nil, origin_obj.odba_connection)
		end
		def prepare_delete(mock, name, id)
			mock.__next(:odba_id) { id }
			ODBA.storage.__next(:retrieve_connected_objects) { |id|
				[]
			}
			mock.__next(:odba_name) { name }
			ODBA.storage.__next(:delete_persistable) { |id_arg| 
				assert_equal(id, id_arg)
			}
			mock.__next(:odba_id) { id }
			mock.__next(:origin_class?) { true }
			mock.__next(:odba_id) { id }
			ODBA.storage.__next(:delete_index_element) { }
		end
		def prepare_bulk_restore(rows)
			rows.each { |odba_mock|
				odba_mock.__next(:odba_id) { 2 }
				odba_mock.__next(:odba_restore) { }
				odba_mock.__next(:odba_id) { 2 }
				odba_mock.__next(:odba_name) { nil }
				ODBA.marshaller.__next(:load) { |dump|
					odba_mock
				}
				ODBA.storage.__next(:restore_collection){|*args|
					[]
				}
			}
		end
		def test_retrieve_from_index
				foo = Mock.new
				index = Mock.new("bar_index")
				@cache.indices["index_name"] = index
				index.__next(:retrieve_data) { |search_term, meta|
					assert_equal('search term', search_term)
					assert_nil(meta)
					[[2, foo]]
				}
				prepare_bulk_restore([foo])
				@cache.retrieve_from_index("index_name", "search term")
				foo.__verify
				index.__verify
		end
		def test_update_indices
			index = Mock.new("index")
			bar = Mock.new("bar")
			bar.__next(:odba_indexable?){ true }
			@cache.indices = {
				"foo" => index
			}
			index.__next(:update){ |obj|
				assert_equal(bar, obj)
			}
			@cache.update_indices(bar)
			bar.__verify
			index.__verify
		end
		def test_delete_index_element
			foo = Mock.new("foo")
			bar = Mock.new("bar")
			@cache.indices = {
				"foo" => foo
			}
			foo.__next(:origin_class?) { |klass| true }
			bar.__next(:odba_id) { 1 }
			ODBA.storage.__next(:delete_index_element) { |name, id|
				assert_equal(name, "foo")
				assert_equal(id, 1)
			}
			@cache.delete_index_element(bar)
		end
		def test_drop_index
			#ODBA.scalar_cache.__next(:odba_isolated_store){}
			ODBA.storage.__next(:transaction) { |block| block.call }
			#ODBA.scalar_cache.__next(:odba_isolated_store) { }
			ODBA.storage.__next(:drop_index) { |index_name|
				assert_equal("foo_index", index_name)
			}
			index = Mock.new("index")
			prepare_delete(index, "foo", 2)
			@cache.indices.store("foo_index", index)
			@cache.drop_index("foo_index")
			index.__verify
		end
		def test_drop_indices
			#ODBA.scalar_cache.__next(:odba_isolated_store){}
			ODBA.storage.__next(:transaction) { |block| block.call}
			#ODBA.scalar_cache.__next(:odba_isolated_store) { }
			ODBA.storage.__next(:drop_index){|index_name|
				assert_equal("foo_index", index_name)
			}
			index = Mock.new("index")
			prepare_delete(index, "foo", 2)
			@cache.indices.store("foo_index", index)
			@cache.drop_indices
			index.__verify
		end
		def test_fetch_collection_element
			key_dump = Marshal.dump('foo')
			ODBA.storage.__next(:collection_fetch) { |odba_id, key|
				assert_equal(12, odba_id)
				assert_equal(key_dump, key)
				'val_dump'
			}
			ODBA.marshaller.__next(:dump) { |key|
				assert_equal('foo', key)
				key_dump
			}
			ODBA.marshaller.__next(:load) { |dump|
				assert_equal('val_dump', dump)
				'val'
			}
			res = @cache.fetch_collection_element(12, 'foo')
			assert_equal('val', res)
		end
	end
end
