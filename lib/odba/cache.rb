# rwaltert@ywesee.com mwalder@ywesee.com
#!/usr/bin/env ruby

require 'singleton'
require 'delegate'

module ODBA
	class Cache < SimpleDelegator
		attr_reader :indices
		include Singleton
		CLEANING_INTERVAL = 900
		CLEANER_ID_STEP = 100
		def initialize
			#=begin
			@cleaner = Thread.new {
				loop {
					sleep(self::class::CLEANING_INTERVAL)
					begin
						puts "cleaning up DB"
						clean
						#		clean_object_connections
					rescue StandardError => e
						puts e
						puts e.backtrace
					end
				}
			}
			@cleaner.priority = -5
			#=end
			@hash = Hash.new
			@cleaner_min_id = 0
			@cleaner_max_id = 0
			super(@hash)
		end
		def bulk_fetch(bulk_fetch_ids, odba_caller)
			dumps = []
			bulk_fetch_ids.each { |id|
				if(entry = @hash[id])
					entry.odba_add_reference(odba_caller)
					bulk_fetch_ids.delete(id)
				end
			}
			unless(bulk_fetch_ids.empty?)
				rows = ODBA.storage.bulk_restore(bulk_fetch_ids)
				bulk_restore(rows, odba_caller)
			end
		end
		def bulk_restore(rows, odba_caller = nil)
			retrieved_objects= []
			rows.each { |row|
				obj_id = row.at(0)
				dump = row.at(1)
				#dump = row.first
				#obj = restore_object(dump)
				#puts " object class"
				#puts obj.class
				if(cache_entry = @hash.fetch(obj_id.to_i, false))
					obj = cache_entry.odba_object
					cache_entry.odba_add_reference(odba_caller)
				else
					obj = restore_object(dump)
					cache_entry = CacheEntry.new(obj)
					cache_entry.odba_add_reference(odba_caller)
					@hash.store(obj.odba_id, cache_entry)
					unless(obj.odba_name.nil?)
						@hash.store(obj.odba_name, cache_entry)
					end
				end
				#puts "bulk_restore"
				#puts obj.class
				retrieved_objects.push(obj)
			}
			#puts "found:"
			#puts retrieved_objects.size
			retrieved_objects
		end
		def clean
			delete_old
			@hash.each { |key, value|
				if(value.odba_old?)
					value.odba_retire
					#puts "retiring #{key}"
				end
			}
		end
		def clean_object_connections
			@cleaner_min_id += CLEANER_ID_STEP
			@cleaner_max_id = @cleaner_min_id + CLEANER_ID_STEP
			if(@cleaner_min_id > ODBA.storage.max_id)
				@cleaner_min_id = 0
				@cleaner_max_id = CLEANER_ID_STEP
			end
			ODBA.storage.remove_dead_objects(@cleaner_min_id, @cleaner_max_id)
			ODBA.storage.remove_dead_connections(@cleaner_min_id, @cleaner_max_id)
		end
		def create_index(index_definition, origin_module)
			index_name = index_definition.index_name
			ODBA.transaction{
				index = ODBA.index_factory(index_definition, origin_module)
				puts "******created index***"
				self.indices.store(index_name, index)
				puts "store self.indices"
				self.indices.odba_store_unsaved
				puts "store index"
				index
			}
		end
		def delete(object)
			rows = nil
			puts "delteting"
			rows = ODBA.storage.retrieve_connected_objects(object.odba_id)
			@hash.delete(object.odba_id)
			@hash.delete(object.odba_name)
			#small transaction  because of odba_store call later on 
			ODBA.storage.delete_persistable(object.odba_id)
			delete_index_element(object)
			unless (rows.empty?)
				rows.each{ |row|
					id = row.first
					connected_object = fetch(id, nil)
					connected_object.odba_cut_connection(object)
					puts "saving connected_object"
					puts "object is a : #{connected_object.class}"
					connected_object.odba_store_unsaved
				}
			end
		end
		def delete_index_element(odba_object)
			klass = odba_object.class
			indices.each { |index_name, index|
				if(index.origin_class?(klass))
					puts "deleting from index #{index_name}"
					# no transaction needed, because method call is
					# already in a transaction (see delete)
					ODBA.storage.delete_index_element(index_name, odba_object.odba_id)
				end
			}
		end
		def delete_old
			@hash.each { |key, value|
				if(value.ready_to_destroy?)
					@hash.delete(key)
				end
			}
		end
		def drop_index(index_name)
			puts "before transaction"
			ODBA.transaction {
				puts "in transaction"
				ODBA.storage.drop_index(index_name)
				self.delete(self.indices[index_name]) #.odba_delete
				puts "index #{index_name} deleted"
			}
		end
		def drop_indices
				keys = self.indices.keys
				keys.each{ |key|
					puts "before drop_index"
					drop_index(key)
				}
		end
		def fetch(odba_id, odba_caller)
			cache_entry = @hash.fetch(odba_id) {
				obj = load_object(odba_id)
					puts "fetch"
				  puts obj.class
				#puts obj.to_s
				cache_entry = CacheEntry.new(obj)
				if(name = obj.odba_name)
					@hash.store(name, cache_entry)
				end
				@hash.store(odba_id, cache_entry)
			}
			cache_entry.odba_add_reference(odba_caller)
			cache_entry.odba_object
		end
		def fetch_named(name, caller, &block)
			cache_entry = @hash[name]
			if(cache_entry.nil?)
				dump = ODBA.storage.restore_named(name)
				obj = nil
				if(dump.nil?)
					puts "#{name} dump is nil"
					obj = block.call
					puts "after block call"
					obj.odba_name = name
					#			store(obj, name) 
					obj.odba_store(name)
					puts "fetch named odba_store completed"
				else
					obj = ODBA.marshaller.load(dump)
					obj.odba_restore
				end	
				cache_entry = CacheEntry.new(obj)
				@hash.store(obj.odba_id, cache_entry)
				@hash.store(name, cache_entry)
				"added to hash"
			end
			cache_entry.odba_add_reference(caller)
			cache_entry.odba_object
		end
		def fill_index(index_name, targets)
			puts "in cache fill index"
				self.indices[index_name].fill(targets)
				puts "index filled"
		end
		def indices
			@indices ||= fetch_named('__cache_server_indices__',self){
				{}
			}
		end
		def prefetch
			rows = ODBA.storage.restore_prefetchable
			bulk_restore(rows)
		end
		def retrieve_from_index(index_name, search_term, meta=nil)
			rows = self.indices.fetch(index_name).retrieve_data(search_term, meta)
			bulk_restore(rows)
		end
		#it is a test method
		def search_indication(index_name, search)
			rows = ODBA.storage.search_indication(index_name, search)
			bulk_restore(rows)
		end
		def store(object)
				odba_id = object.odba_id
				cache_values = object.odba_cache_values
				unless(cache_values.empty?)
					puts "call from cache"
					ODBA.scalar_cache.update(cache_values)
					#			ODBA.scalar_cache.odba_isolated_store
				end
				dump = object.odba_isolated_dump
				name = object.odba_name
				prefetchable = object.odba_prefetch?
				ODBA.storage.store(odba_id, dump, name, prefetchable)
				update_indices(object)
				store_object_connections(object)
				cache_entry = @hash[odba_id]
				if(cache_entry.nil?)
					cache_entry = CacheEntry.new(object)
					@hash.store(odba_id, cache_entry)
					unless(name.nil?)
						@hash.store(name, cache_entry)
					end
				end
				cache_entry.odba_object
		end
		def store_object_connections(object)
			name = object.odba_name
			target_ids = object.odba_target_ids
			origin_id = object.odba_id
			target_ids.each { |target_id|
				ODBA.storage.add_object_connection(origin_id, target_id)
			}
			unless(name.nil?)
				ODBA.storage.add_object_connection(origin_id, origin_id)
			end
		end
		def update_indices(odba_object)
			puts "UPDATING INDEX:"
			klass = odba_object.class
			puts "klass #{klass}"
			if(odba_object.odba_indexable?)
				indices.each { |index_name, index|
					index.update(odba_object)
				}
			end
		end
		private
		def load_object(odba_id)
			receiver_dump = ODBA.storage.restore(odba_id)
			restore_object(receiver_dump)
		end
		def restore_object(dump)
			if(dump.nil?)
				raise OdbaError, "Unknown odba_id"
			end
			obj = ODBA.marshaller.load(dump)
			obj.odba_restore
			obj
		end
	end
end
