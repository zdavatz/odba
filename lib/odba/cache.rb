# rwaltert@ywesee.com mwalder@ywesee.com
#!/usr/bin/env ruby

require 'singleton'
require 'delegate'

module ODBA
	class Cache < SimpleDelegator
		include Singleton
		CLEANING_INTERVAL = 900
		REAPER_ID_STEP = 1000
		REAPER_INTERVAL = 60
		def initialize
			@cleaner = Thread.new {
				Thread.current.priority = -5
				loop {
					sleep(self::class::CLEANING_INTERVAL)
					begin
						clean unless(@batch_mode)
					rescue StandardError => e
						puts e
						puts e.backtrace
					end
				}
			}
			@grim_reaper = Thread.new {
				Thread.current.priority = -6
				loop {
					sleep(self::class::REAPER_INTERVAL)
					begin
						reap_object_connections unless(@batch_mode)
					rescue StandardError => e
						puts e
						puts e.backtrace
					end
				}
			}
			@hash = Hash.new
			@reaper_min_id = 0
			@reaper_max_id = 0
			super(@hash)
		end
		def batch(&block)
			@batch_mutex ||= Mutex.new
			@batch_mutex.synchronize {
				begin
					@batch_objects = {}
					@batch_deletions = {}
					@batch_mode = true
					block.call
					@batch_deletions.each_value { |object|
						delete_direct(object)
					}
					@batch_objects.each_value { |object|
						store_direct(object)
					}
				ensure
					@batch_mode = false
				end
			}
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
				if(cache_entry = @hash.fetch(obj_id.to_i, nil))
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
				retrieved_objects.push(obj)
			}
			retrieved_objects
		end
		def clean
			delete_old
			@hash.each { |key, value|
				if(value.odba_old? \
					&& !(@batch_mode && @batch_objects.has_key?(value.odba_id)))
					value.odba_retire
				end
			}
		end
		def reap_object_connections
			@reaper_min_id += REAPER_ID_STEP
			@reaper_max_id = @reaper_min_id + REAPER_ID_STEP
			if(@reaper_min_id > ODBA.storage.max_id)
				@reaper_min_id = 0
				@reaper_max_id = REAPER_ID_STEP
			end
			ODBA.storage.remove_dead_objects(@reaper_min_id, @reaper_max_id)
			ODBA.storage.remove_dead_connections(@reaper_min_id, @reaper_max_id)
		end
		def create_index(index_definition, origin_module)
			ODBA.transaction {
				index = ODBA.index_factory(index_definition, origin_module)
				self.indices.store(index_definition.index_name, index)
				self.indices.odba_store_unsaved
				index
			}
		end
		def delete(object)
			odba_id = object.odba_id
			#require 'debug'
			rows = ODBA.storage.retrieve_connected_objects(odba_id)
			rows.each { |row|
				id = row.first
				# Self-Referencing objects don't have to be resaved
				begin
					if(connected_object = fetch(id, nil))
						connected_object.odba_cut_connection(object)
						connected_object.odba_isolated_store
					end
				rescue OdbaError
					puts "OdbaError ### deleting #{object.class}:#{odba_id}"
					puts "          ### while looking for connected object #{id}"
				end
			}
			@hash.delete(odba_id)
			@hash.delete(object.odba_name)
			if(@batch_mode)
				delete_batched(object)
			else
				delete_direct(object)
			end
		end
		def delete_batched(object)
			odba_id = object.odba_id
			#puts "ODBA ### queuing object for deletion #{object.class}:#{odba_id}"
			@batch_objects.delete(odba_id)
			@batch_deletions.store(odba_id, object)
		end
		def delete_direct(object)
			odba_id = object.odba_id
			#puts "ODBA ### deleting object #{object.class}:#{odba_id}"
			ODBA.storage.delete_persistable(odba_id)
			delete_index_element(object)
			object
		end
		def delete_index_element(odba_object)
			klass = odba_object.class
			indices.each { |index_name, index|
				if(index.origin_class?(klass))
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
			ODBA.transaction {
				ODBA.storage.drop_index(index_name)
				self.delete(self.indices[index_name]) #.odba_delete
			}
		end
		def drop_indices
				keys = self.indices.keys
				keys.each{ |key|
					drop_index(key)
				}
		end
		def fetch(odba_id, odba_caller)
			cache_entry = @hash.fetch(odba_id) {
				obj = load_object(odba_id)
				#update_scalar_cache(odba_id, obj.odba_cache_values)
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
					obj = block.call
					obj.odba_name = name
					obj.odba_store(name)
				else
					obj = ODBA.marshaller.load(dump)
					obj.odba_restore
				end	
				cache_entry = CacheEntry.new(obj)
				#update_scalar_cache(obj.odba_id, obj.odba_cache_values)
				@hash.store(obj.odba_id, cache_entry)
				@hash.store(name, cache_entry)
			end
			cache_entry.odba_add_reference(caller)
			cache_entry.odba_object
		end
		def fill_index(index_name, targets)
				self.indices[index_name].fill(targets)
		end
		def indices
			@indices ||= fetch_named('__cache_server_indices__',self) {
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
		def store(object)
			if(@batch_mode)
				store_batched(object)
			else
				store_direct(object)
			end
		end
		def store_direct(object)
			odba_id = object.odba_id
			update_scalar_cache(odba_id, object.odba_cache_values)
			dump = object.odba_isolated_dump
			name = object.odba_name
			prefetchable = object.odba_prefetch?
			ODBA.storage.store(odba_id, dump, name, prefetchable)
			update_indices(object)
			store_object_connections(object)
			store_cache_entry(odba_id, object, name)
		end
		def store_batched(object)
			odba_id = object.odba_id
			unless(@batch_deletions.include?(odba_id))
				@batch_objects.store(odba_id, object)
			end
			store_cache_entry(odba_id, object, object.odba_name)
		end
		def store_cache_entry(odba_id, object, name=nil)
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
			klass = odba_object.class
			if(odba_object.odba_indexable?)
				indices.each { |index_name, index|
					index.update(odba_object)
				}
			end
		end
		def update_scalar_cache(odba_id, cache_values)
			unless(cache_values.empty?)
				ODBA.scalar_cache.delete(odba_id)
				ODBA.scalar_cache.update(cache_values)
			end
		end
		private
		def load_object(odba_id)
			dump = ODBA.storage.restore(odba_id)
			restore_object(dump)
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
