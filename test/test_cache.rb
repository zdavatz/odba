#!/usr/bin/env ruby

$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'test/unit'
require 'mock'
require 'odba'

class Mock
	ODBA_PREFETCH = false
end

module ODBA
	class Cache < SimpleDelegator
		attr_accessor :cleaner, :hash
		attr_writer :indices
		public :load_object
		def initialize
			@hash = Hash.new
			super(@hash)
		end
	end
	class TestCache < Test::Unit::TestCase
		class ODBAContainer
		 include ODBA::Persistable
		 attr_accessor	:odba_connection, :odba_id
		end
		def setup
			ODBA.storage = Mock.new("storage")
			@cache = ODBA::Cache.instance
			@cache.hash.clear
			ODBA.marshaller = Mock.new("marshaller")
			#@cache.cleaner.kill
			@cache.indices = {}
			#set to nil because of test suite
			ODBA.cache_server = nil
		end
		def teardown
			ODBA.storage.__verify
		end
		def test_fetch_named_ok
			storage = Mock.new("storage")
			marshaller = Mock.new
			restore = Mock.new
			caller = Mock.new
			caller2 = Mock.new
			ODBA.marshaller = marshaller
			ODBA.storage = storage
			storage.__next(:restore_named){ |name|
				restore
			}
			restore.__next(:odba_restore) {}
			restore.__next(:odba_id) { 1 }
			marshaller.__next(:load){|dump|
				restore
			}
			result1 = @cache.fetch_named('foo', caller)
			assert_equal(restore, result1)
			result2 = @cache.fetch_named('foo', caller2)
			assert_equal(restore, result1)
			result_fetch_by_id = @cache.fetch(1, caller)
			assert_equal(restore, result_fetch_by_id)
			caller.__verify
			caller2.__verify
			storage.__verify
			marshaller.__verify
			restore.__verify
		end
		def test_bulk_fetch_load_all
			array = [2, 3]
			storage = Mock.new
			caller = Mock.new
			foo = Mock.new
			bar = Mock.new
			ODBA.marshaller = Mock.new
			ODBA.storage = storage
			storage.__next(:bulk_restore) { |ids|
				[foo, bar]
			}
			ODBA.marshaller.__next(:load) { |dump|
				foo
			}
			foo.__next(:first) { || foo}
			foo.__next(:odba_restore) { }
			foo.__next(:odba_id) { 2 }
			foo.__next(:odba_id) { 2 }
			foo.__next(:odba_name) { nil }
			ODBA.marshaller.__next(:load) { |dump|
				bar
			}
			bar.__next(:first) {|| bar }
			bar.__next(:odba_restore) { }
			bar.__next(:odba_id) { 3 }
			bar.__next(:odba_id) { 3 }
			bar.__next(:odba_name) { nil }
			@cache.bulk_fetch(array, caller)
			assert_equal(true, @cache.hash.has_key?(2))
			assert_equal(2, @cache.hash.size)
			assert_equal(true, @cache.hash.has_key?(3))
			foo.__verify
			bar.__verify
			storage.__verify
			ODBA.marshaller.__verify
		end
		def test_bulk_fetch
			array = [2, 3, 7]
			storage = Mock.new
			caller = Mock.new
			foo = Mock.new("foo")
			bar = Mock.new("bar")
			baz = Mock.new("baz")
			ODBA.marshaller = Mock.new
			ODBA.storage = storage
			@cache.hash = {
				7 => baz
			}
			baz.__next(:odba_add_reference) { |caller| }
			storage.__next(:bulk_restore) { |ids|
				[foo, bar]
			}
			ODBA.marshaller.__next(:load) { |dump|
				foo
			}
			foo.__next(:first) {|| foo }
			foo.__next(:odba_restore) { }
			foo.__next(:odba_id) { 2 }
			foo.__next(:odba_id) { 2 }
			foo.__next(:odba_name) { nil }
			ODBA.marshaller.__next(:load) { |dump|
				bar
			}
			bar.__next(:first) {|| bar }
			bar.__next(:odba_restore) { }
			bar.__next(:odba_id) { 3 }
			bar.__next(:odba_id) { 3 }
			bar.__next(:odba_name) { nil }
			@cache.bulk_fetch(array, caller)
			assert_equal(true, @cache.hash.has_key?(2))
			assert_equal(3, @cache.hash.size)
			assert_equal(true, @cache.hash.has_key?(3))
			foo.__verify
			bar.__verify
			storage.__verify
			ODBA.marshaller.__verify
		end
		def test_bulk_restore
			foo = Mock.new("foo")
			rows = [foo]
			foo.__next(:first) {|| foo }
			foo.__next(:odba_restore) { }
			foo.__next(:odba_id) { 2 }
			foo.__next(:odba_id) { 2 }
			foo.__next(:odba_name) { nil }
			ODBA.marshaller.__next(:load) { |dump|
				foo
			}
			@cache.bulk_restore(rows, "foo")
			foo.__verify
		end
		def test_bulk_restore_in_hash
			foo = Mock.new("foo")
			rows = [foo]
			@cache.hash.store(1, foo)
			foo.__next(:first) {|| foo }
			foo.__next(:odba_restore) { }
			ODBA.marshaller.__next(:load) { |dump|
				foo
			}
			foo.__next(:odba_id){|| 1}
			foo.__next(:odba_add_reference){|caller|}
			@cache.bulk_restore(rows, "foo")
			foo.__verify
			ODBA.marshaller.__verify
		end
		def test_clean
			obj1 = Mock.new
			obj2 = Mock.new
			@cache.hash.store(2, obj1)
			@cache.hash.store(3, obj2)
			obj1.__next(:ready_to_destroy?) { false }
			obj1.__next(:odba_old?) { true }
			obj1.__next(:odba_retire) { }
			obj2.__next(:ready_to_destroy?) { false }
			obj2.__next(:odba_old?) { false }
			@cache.clean
			obj1.__verify
			obj2.__verify
		end
		def test_delete_old
			value = Mock.new("value")
			ODBA.storage = Mock.new("storage")
			@cache.hash.store(12, value)
			value.__next(:ready_to_destroy?) { || true}
			@cache.delete_old
			value.__verify
			assert_equal(0, @cache.size)
		end
		def test_fetch_named_block
			restore = Mock.new("restore")
			marshaller = Mock.new("marshaller")
			caller = Mock.new("caller")
			caller2 = Mock.new("caller2")
			ODBA.marshaller = marshaller
			ODBA.storage.__next(:restore_named) { |name| }
			restore.__next(:odba_name=) { |name| }
			restore.__next(:odba_store){ |obj| }
			#restore.__next(:odba_isolated_dump){ || }
			restore.__next(:odba_id) { 2 }
			#restore.__next(:odba_prefetch?){ || }
			#ODBA.storage.__next(:store) { |id, index, name, pref|
				#assert_equal('foo', name)
				#nil
		#	}
			#restore.__next(:odba_target_ids) { []}
			#ODBA.storage.__next(:add_object_connection){|id,id2|}
			#restore.__next(:odba_id) { 2 }
			#restore.__next(:odba_id) { 2 }
			result = @cache.fetch_named("foo", caller2) {
				restore
			}
			assert_equal(restore, result)
			result_fetch_by_id = @cache.fetch(2, caller)
			assert_equal(restore, result_fetch_by_id)
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
		def test_fetch
			caller = Mock.new
			caller2 = Mock.new
			receiver = Mock.new
			prepare_fetch(5, receiver)
			first_fetch = @cache.fetch(5, caller)
			assert_equal(receiver, first_fetch)
			second_fetch = @cache.fetch(5, caller2)
			assert_equal(receiver, second_fetch)
			receiver.__verify
			ODBA.storage.__verify
			ODBA.marshaller.__verify
		end
		def test_fetch_has_name
			storage = Mock.new
			marshaller = Mock.new
			caller = Mock.new
			caller2 = Mock.new
			caller3 = Mock.new
			ODBA.marshaller = marshaller
			ODBA.storage = storage
			receiver = Mock.new
			storage.__next(:restore){ |odba_id|
				assert_equal(23, odba_id)
				odba_id
			}
			marshaller.__next(:load){|dump|
				receiver
			}
			receiver.__next(:odba_restore) {}
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
			storage.__verify
			marshaller.__verify
		end
		def test_fetch_error
			storage = Mock.new
			ODBA.storage = storage
			receiver = Mock.new
			storage.__next(:restore) { |odba_id|
				nil	
			}
			assert_raises(OdbaError) {
				@cache.load_object(23)
			}
		end
		def test_store						
			storage = Mock.new("storage")
			save_obj = Mock.new("save_obj")
			prepare_store([save_obj])
			@cache.store(save_obj)
			verify_store	
			save_obj.__verify
		end
		def test_store_object_connections
			save = Mock.new("to_store")
			save.__next(:odba_name){ nil}
			save.__next(:odba_target_ids){ [1,2]}
			save.__next(:odba_id){ 4}
			ODBA.storage.__next(:add_object_connection){|id,target_id|
				assert_equal(4, id)
				assert_equal(1, target_id)
			}
			ODBA.storage.__next(:add_object_connection){|id,target_id|
				assert_equal(4, id)
				assert_equal(2, target_id)
			}
			@cache.store_object_connections(save)
			save.__verify
			ODBA.storage.__verify
		end
		def test_store_object_connection_named
			save = Mock.new("to_store")
			save.__next(:odba_name){ "foo" }
			save.__next(:odba_target_ids){ []}
			save.__next(:odba_id){ 4}
			ODBA.storage.__next(:add_object_connection){|id,target_id|
				assert_equal(4, id)
				assert_equal(4, target_id)
			}
			@cache.store_object_connections(save)
			save.__verify
			ODBA.storage.__verify
		end
		def test_load_object
			storage = Mock.new
			ODBA.storage = storage
			marshaller = Mock.new
			receiver = Mock.new
			ODBA.marshaller = marshaller
			storage.__next(:restore){ |odba_id|
				assert_equal(23, odba_id)
				odba_id
			}
			marshaller.__next(:load){|dump|
				receiver
			}
			receiver.__next(:odba_restore){||}
			@cache.load_object(23)
			storage.__verify
			marshaller.__verify
			receiver.__verify
		end
		def test_clean_object_connection
			ODBA.storage.__next(:remove_dead_objects) { }
			ODBA.storage.__next(:remove_dead_connections) { }
			@cache.clean_object_connections
		end
		def test_prefetch
			foo = Mock.new("foo")
			ODBA.storage = Mock.new("storage")
			ODBA.storage.__next(:restore_prefetchable){||
				[foo]
			
			}
			prepare_bulk_restore([foo])
			@cache.prefetch
			foo.__verify
			ODBA.storage.__verify
		end
		def test_fill_index
			foo = Mock.new("foo")
			foo.__next(:fill) { |target| 
				assert_equal("baz", target)
			}
			ODBA.storage.__next(:fill_index) { |index_name, row| }
			@cache.indices = { 
				"foo" => foo
			}
			@cache.fill_index("foo", "baz")
			verify_store
			foo.__verify
		end
		def test_create_index
			ODBA.storage.__next(:create_index) { |index_name|  }
			ODBA.storage.__next(:next_id) {  }
			ODBA.storage.__next(:next_id) {  }
			ODBA.storage.__next(:next_id) {  }
			ODBA.storage.__next(:next_id) {  }
			ODBA.storage.__next(:store) { |name, index, mp, pref|  
			}
			ODBA.storage.__next(:next_id) {  }
			ODBA.storage.__next(:add_object_connection) { |id, targedid|  }
			ODBA.storage.__next(:add_object_connection) { |id, targedid|  }
			ODBA.storage.__next(:next_id) {  }
			ODBA.storage.__next(:next_id) {  }
			ODBA.storage.__next(:store) { |name, index, mp, pref| }
			ODBA.storage.__next(:next_id) {  }
			ODBA.marshaller.__next(:dump) { |dump| }
			ODBA.marshaller.__next(:dump) { |dump| }
			@cache.create_index("foo", "bar", "proc_code", "baz")
			assert_instance_of(Index, @cache.indices['foo'])
			verify_store
		end
		def prepare_store(store_array)
			store_array.each{ |mock|
				mock.__next(:odba_id){ || }
				mock.__next(:odba_isolated_dump){ || }
				mock.__next(:odba_name){ || }
				mock.__next(:odba_prefetch?){ || }
				mock.__next(:odba_name){ || }
			  mock.__next(:odba_target_ids) { []}
				mock.__next(:odba_id){ || }
				ODBA.storage.__next(:store) { |id, dump, name, pref| }
			}
		end
		def verify_store
			ODBA.storage.__verify
			ODBA.marshaller.__verify
		end
		def test_delete
			delete_item = ODBAContainer.new
			delete_item.odba_id = 1
			origin_obj = ODBAContainer.new
			origin_obj.odba_id = 2
			origin_obj.odba_connection = delete_item
			@cache.hash.store(1, delete_item)
			ODBA.storage.__next(:retrieve_connected_objects) { |id|					[[2]] 
			}
			prepare_fetch(2, origin_obj)
			ODBA.storage.__next(:store) { |id, dump, name, prefetch|}
		  ODBA.storage.__next(:delete_persistable) { |id| } 
			ODBA.marshaller.__next(:dump) { |ob| "foo"}
			@cache.delete(delete_item)
			assert_equal(1, @cache.hash.size)
			assert_equal(nil, origin_obj.odba_connection)
			ODBA.storage.__verify
			ODBA.marshaller.__verify
		end
		def test_delete_index
			index = Mock.new("index")
			@cache.hash.store(1, index)
			@cache.hash.store("foobar", index)
			@cache.indices.store("foobar", index)
			ODBA.storage.__next(:next_id) { 1}
			ODBA.storage.__next(:store) { |id, dump, name, pref| }
			#ODBA.storage.__next(:add_object_connection) { |id, target_id| }
			index.__next(:odba_object) { }
			prepare_delete(index, "foobar", 1)
			ODBA.storage.__next(:drop_index_table) { |name| }
			ODBA.marshaller.__next(:dump) {	|dum| "foo" } 
			@cache.delete_index("foobar")
			assert_equal(0, @cache.indices.size)
			assert_equal(0, @cache.size)
			index.__verify
			ODBA.storage.__verify
		end
		def prepare_delete(mock, name, id)
			mock.__next(:odba_id) { id }
			ODBA.storage.__next(:retrieve_connected_objects) { |id|
				[]
			}
			ODBA.storage.__next(:delete_persistable) { |id| }
			mock.__next(:odba_id) { id }
			mock.__next(:odba_name) { name }
			mock.__next(:odba_id) { id }
		end
		def prepare_bulk_restore(rows)
			rows.each { |foo|
				foo.__next(:first) {|| foo }
				foo.__next(:odba_restore) { }
				foo.__next(:odba_id) { 2 }
				foo.__next(:odba_id) { 2 }
				foo.__next(:odba_name) { nil }
				ODBA.marshaller.__next(:load) { |dump|
					foo
				}
			}
		end
		def test_retrieve_from_index
				foo = Mock.new
				ODBA.storage.__next(:retrieve_from_index){|name, term|
					[foo]
				}
				prepare_bulk_restore([foo])
				@cache.retrieve_from_index("bar", "search bar")
				ODBA.storage.__verify
				foo.__verify
		end
=begin
		def test_update_index
			ODBA.storage.__next(:update_index){ |name, id, search|
				assert_equal(name, "foo")
				assert_equal(id, 1)
				assert_equal(search, "foobar")
			}
			@cache.update_index("foo", 1, "foobar")
			ODBA.storage.__verify
		end
=end
		def test_update_indices
			foo = Mock.new("foo")
			bar = Mock.new("bar")
			@cache.indices = {
				"foo" => foo
			}
			foo.__next(:origin_class?) { |klass| true }
			foo.__next(:search_term) { |obj| "foobar" }
			foo.__next(:resolve_target_id) { |obj| "foobar" }
			bar.__next(:odba_id) { 1 }
			ODBA.storage.__next(:update_index) { |name, id, search, target_id|
				assert_equal(name, "foo")
				assert_equal(id, 1)
				assert_equal(search, "foobar")
			}
			@cache.update_indices(bar)
			ODBA.storage.__verify
			foo.__verify
			bar.__verify
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
			ODBA.storage.__verify
		end
	end
end
