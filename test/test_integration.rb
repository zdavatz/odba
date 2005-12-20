#!/usr/bin/env ruby
# TestIntegration -- oddb -- 28.04.2004 -- mwalder@ywesee.com
=begin
$: << File.expand_path('../lib/odba/', File.dirname(__FILE__))

require 'test/unit'
require 'persistable'
module ODBA
	class CacheEntry
		RETIRE_TIME = 0.5
		DESTROY_TIME = 1
	end	
	class Cache < Hash
		attr_accessor :cleaner
		CLEANING_INTERVAL = 0.1
	end
	class TestIintegration < Test::Unit::TestCase
			class ODBAContainer
			 include ODBA::Persistable
			 attr_accessor :link
			 attr_accessor :odba_snapshot_level
			 attr_accessor :data
			 attr_accessor :hash_obj
			 def initialize
				 @odba_name = "foo"
			 end
			end
		class Storage
			attr_accessor :store_obj, :next_id_int
			def initialize
				@store_obj = Hash.new
				@next_id_int = 0
			end
			def store(id, dump, name)
				@store_obj.store(id, dump)
			end
			def next_id
				@next_id_int += 1
			end
			def restore(id)
				@store_obj[id]
			end
		end
		def setup
			#	Stub.delegate_object_methods
			ODBA.storage  = Storage.new
			ODBA.cache.hash.clear
		end
		def test_integration_simple
		 root_odba = ODBAContainer.new
		 root_odba.data = "rootObject"
		 level_1 = ODBAContainer.new
		 level_1.data = "level_1"
		 level_2 = ODBAContainer.new
		 level_2.data = "level_2"
		 root_odba.link = level_1
		 level_1.link = level_2
		 root_odba.odba_take_snapshot
		 root_obda = nil
		 root_odba = ODBA.cache.fetch(1, self)
		 assert_equal("rootObject", root_odba.data)
		 assert_equal("level_1", root_odba.link.data)
		 assert_equal("level_2", root_odba.link.link.data)
		end
		def test_integration_enumerable
			root_odba = ODBAContainer.new
			root_odba.data = "rootObject"
			level_1 = ODBAContainer.new
			level_1.data = "level_1"
			key_1 = ODBAContainer.new
			key_1.data = "key_1"
			key_2 = ODBAContainer.new
			key_2.data = "key_2"
			val_1 = ODBAContainer.new
			val_1.data = "val_1"
			val_2  = ODBAContainer.new
			sub_val_1 = ODBAContainer.new
			sub_val_1.data = "sub_val_1"
			obj_hash = { 
				"key_1" => val_1,	
				key_2 => val_2,
			}
			sub_obj_hash = {
				"key_1" => sub_val_1,
			}
			val_1.hash_obj = sub_obj_hash
			root_odba.hash_obj = obj_hash
			root_odba.odba_take_snapshot
			assert_not_nil(val_1.odba_snapshot_level)
			assert_not_nil(sub_val_1.odba_snapshot_level)
			key_2_id = key_2.odba_id
			val_2_id = val_2.odba_id
			root_odba = nil
			root_odba = ODBA.cache.fetch(1, self)
			assert_equal("rootObject", root_odba.data)
			assert_equal(2, root_odba.hash_obj.size)
			new_key_2 = ODBA.cache.fetch(key_2_id, root_odba)
			assert_equal(key_2, new_key_2)
			new_val_2 = ODBA.cache.fetch(val_2_id, root_odba)
			assert_equal(val_2, new_val_2)
			assert_equal(new_val_2, root_odba.hash_obj[new_key_2])
			assert_equal("val_1", root_odba.hash_obj["key_1"].data)
			assert_equal(1, root_odba.hash_obj["key_1"].hash_obj.size)
			assert_equal("sub_val_1", root_odba.hash_obj["key_1"].hash_obj["key_1"].data)
			assert_equal(7, ODBA.storage.next_id_int)
			
			sleep(4)
			root_odba = ODBA.cache.fetch(1, self)
			assert_equal("rootObject", root_odba.data)
			assert_equal(2, root_odba.hash_obj.size)
			Thread.critical {
				new_key_2 = ODBA.cache.fetch(key_2_id, self)
				assert_instance_of(ODBAContainer, new_key_2)
				assert_not_equal(key_2, new_key_2)
				new_val_2 = ODBA.cache.fetch(val_2_id, self)
				assert_instance_of(ODBAContainer, new_val_2)
				assert_not_equal(val_2, new_val_2)
				control_val = root_odba.hash_obj[new_key_2]
				assert_equal(new_val_2, control_val)
			}
		end
		def test_save_same_array
			odba = ODBAContainer.new
			odba_sub = ODBAContainer.new
			arr1 = Array.new
			arr1.push(odba_sub)
			arr2 = Array.new
			arr2.push(odba_sub)
			odba.link = arr1
			odba.hash_obj = arr2
			result = odba.odba_take_snapshot
			assert_equal(4, ODBA.storage.store_obj.size)
		end
		def test_cache_stub
			odba = ODBAContainer.new
			odba_sub = ODBAContainer.new
			odba_sub.data = "odba_sub"
			odba.link = odba_sub
			odba.odba_take_snapshot
			sleep(1.5)
			loaded_odba = ODBA.cache.fetch(1, self)
			assert_equal(true, loaded_odba.link.is_a?(Stub))
			assert_equal("odba_sub", loaded_odba.link.data)
			assert_equal(2, ODBA.storage.store_obj.size)
			assert_equal(true, loaded_odba.link.is_a?(Stub))
			assert_equal(2, ODBA.storage.store_obj.size)
		end
		def test_cache_delete
			odba = ODBAContainer.new
			odba_sub = ODBAContainer.new
			ODBA.cache.super_store(1, CacheEntry.new(odba))
			ODBA.cache.super_store(2, CacheEntry.new(odba_sub))
			loaded_odba = ODBA.cache.fetch(1, self)
			assert_equal(2, ODBA.cache.size)
			sleep(1)
			ODBA.cache.delete_old
			assert_equal(1, ODBA.cache.size)
		end
		def test_delete_hash_elements
			root_element = ODBAContainer.new
			root_element.odba_name = nil
			val_1 = ODBAContainer.new
			val_1.odba_name = nil
			val_2 = ODBAContainer.new
			val_2.odba_name = nil
			hash = {
				"key1" => val_1,
				"key2" => val_2
			}
			hash.odba_name = nil
			root_element.hash_obj = hash	
			root_element.odba_take_snapshot
			root_element = nil
			sleep(1.5)
			root_element = ODBA.cache.fetch(1, self)
			root_element.hash_obj["key1"]
			cache_entry = ODBA.cache[val_1.odba_id]
			accessed_by = cache_entry.instance_variable_get("@accessed_by")
			assert_equal(hash.odba_id, accessed_by[0].odba_id)
			sleep(1.5)
			assert_equal(true, ODBA.cache.has_key?(1))
			assert_equal(false, ODBA.cache.has_key?(hash.odba_id))
			assert_equal(1, ODBA.cache.size)
		end
	end
end
=end
