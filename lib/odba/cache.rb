#!/usr/bin/env ruby
#-- Cache -- odba -- 29.04.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

require 'singleton'
require 'date'

module ODBA
	class Cache
		include Singleton
		CLEANER_PRIORITY = 0   # :nodoc: 
		CLEANING_INTERVAL = 120# :nodoc: 
		REAPER_ID_STEP = 1000  # :nodoc: 
		REAPER_INTERVAL = 900  # :nodoc: 
		def initialize # :nodoc: 
			if(self::class::CLEANING_INTERVAL > 0)
				start_cleaner
				#start_reaper # TODO Database Garbage-Collection.
			end
			@cache_mutex = Mutex.new
      @deferred_indices = []
			@fetched = Hash.new
			@prefetched = Hash.new
			@clean_prefetched = false
			@reaper_min_id = 0
			@reaper_max_id = 0
		end
		# Returns all objects designated by _bulk_fetch_ids_ and registers 
		# _odba_caller_ for each of them. Objects which are not yet loaded are loaded
		# from ODBA#storage.
		def bulk_fetch(bulk_fetch_ids, odba_caller)
			instances = []
			loaded_ids = []
			bulk_fetch_ids.each { |id|
				if(entry = fetch_cache_entry(id))
					entry.odba_add_reference(odba_caller)
					instances.push(entry.odba_object)
					loaded_ids.push(id)
				end
			}
			bulk_fetch_ids -= loaded_ids
			unless(bulk_fetch_ids.empty?)
				rows = ODBA.storage.bulk_restore(bulk_fetch_ids)
				instances += bulk_restore(rows, odba_caller)
			end
			instances
		end
		def bulk_restore(rows, odba_caller = nil) # :nodoc:
			retrieved_objects= []
			rows.each { |row|
				obj_id = row.at(0)
				dump = row.at(1)
				if(cache_entry = fetch_cache_entry(obj_id))
					obj = cache_entry.odba_object
					cache_entry.odba_add_reference(odba_caller)
				else
					obj = restore_object(obj_id, dump, odba_caller)
				end
				retrieved_objects.push(obj)
			}
			retrieved_objects
		end
		def clean # :nodoc:
			delete_old
			#cleaned = 0
			#puts "starting cleaning cycle"
			#$stdout.flush
			#start = Time.now
			@fetched.each_value { |value|
				if(value.odba_old?)
					#cleaned += 1
					value.odba_retire
				end
			}
			if(@clean_prefetched)
				@prefetched.each_value { |value|
					if(value.odba_old?)
						#cleaned += 1
						value.odba_retire
					end
				}
			end
			#puts "cleaned: #{cleaned} objects in #{Time.now - start} seconds"
			#puts "remaining objects in @fetched:    #{@fetched.size}"
			#puts "remaining objects in @prefetched: #{@prefetched.size}"
			#$stdout.flush
		end
		# overrides the ODBA_PREFETCH constant and @odba_prefetch instance variable
		# in Persistable. Use this if a secondary client is more memory-bound than 
		# performance-bound.
		def clean_prefetched(flag=true)
			if(@clean_prefetched = flag)
				clean
			end
		end
		def clear # :nodoc:
			@fetched.clear
			@prefetched.clear
		end
		# Creates a new index according to IndexDefinition
		def create_index(index_definition, origin_module=Object)
			transaction {
				klass = if(index_definition.fulltext)
									FulltextIndex
								elsif(index_definition.resolve_search_term.is_a?(Hash))
									ConditionIndex
								else
									Index
								end
				index = klass.new(index_definition, origin_module)
				indices.store(index_definition.index_name, index)
				indices.odba_store_unsaved
				index
			}
		end
		# Permanently deletes _object_ from the database and deconnects all connected
		# Persistables 
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
					warn "OdbaError ### deleting #{object.class}:#{odba_id}"
					warn "          ### while looking for connected object #{id}"
				end
			}
			@fetched.delete(odba_id)
			@fetched.delete(object.odba_name)
			@prefetched.delete(odba_id)
			@prefetched.delete(object.odba_name)
			ODBA.storage.delete_persistable(odba_id)
			delete_index_element(object)
			object
		end
		def delete_index_element(odba_object) # :nodoc:
			klass = odba_object.class
			indices.each { |index_name, index|
				if(index.origin_class?(klass))
					ODBA.storage.delete_index_element(index_name, odba_object.odba_id)
				end
			}
		end
		def delete_old # :nodoc:
			#deleted = 0
			#start = Time.now
			@fetched.delete_if { |key, value|
				value.ready_to_destroy?
			}
			if(@clean_prefetched)
				@prefetched.delete_if { |key, value|
					value.ready_to_destroy?
				}
			end
			#puts "deleted: #{deleted} objects in #{Time.now - start} seconds"
			#$stdout.flush
		end
		# Permanently deletes the index named _index_name_
		def drop_index(index_name)
			transaction {
				ODBA.storage.drop_index(index_name)
				self.delete(self.indices[index_name]) 
			}
		end
		def drop_indices # :nodoc:
				keys = self.indices.keys
				keys.each{ |key|
					drop_index(key)
				}
		end
    # Queue an index for creation by #setup
    def ensure_index_deferred(index_definition)
      @deferred_indices.push(index_definition)
		end
    # Get all instances of a class (- a limited extent)
    def extent(klass, odba_caller=nil)
			bulk_fetch(ODBA.storage.extent_ids(klass), odba_caller)
    end
		# Fetch a Persistable identified by _odba_id_. Registers _odba_caller_ with
		# the CacheEntry. Loads the Persistable if it is not already loaded.
		def fetch(odba_id, odba_caller=nil)
			if(cache_entry = fetch_cache_entry(odba_id))
				cache_entry.odba_add_reference(odba_caller)
				cache_entry.odba_object
			else
				load_object(odba_id, odba_caller)
			end
		end
		def fetch_cache_entry(odba_id_or_name) # :nodoc:
			@prefetched[odba_id_or_name] || @fetched[odba_id_or_name]
		end
		def fetch_collection(obj) # :nodoc:
			collection = []
			bulk_fetch_ids = [] 
			rows = ODBA.storage.restore_collection(obj.odba_id)
			rows.each { |row|
				key = ODBA.marshaller.load(row[0])
				value = ODBA.marshaller.load(row[1])
				bulk_fetch_ids.push(key.odba_id)
				bulk_fetch_ids.push(value.odba_id)
				collection.push([key, value])
			}
			bulk_fetch_ids.compact!
			bulk_fetch(bulk_fetch_ids, obj)
			collection.each { |pair| 
				pair.collect! { |item| 
					if(item.is_a?(Stub))
						## replace any stubized instance_variables in obj with item
						## independent of odba_restore
						item.odba_container = obj
						item.odba_instance
					else
						item 
					end
				}
			}
			collection
		end
		def fetch_collection_element(odba_id, key) # :nodoc:
			key_dump = ODBA.marshaller.dump(key.odba_isolated_stub)
			## for backward-compatibility and robustness we only attempt
			## to load if there was a dump stored in the collection table
			if(dump = ODBA.storage.collection_fetch(odba_id, key_dump))
				ODBA.marshaller.load(dump)
			end
		end
		def fetch_named(name, odba_caller, &block) # :nodoc:
			fetch_or_do(name, odba_caller) { 
				dump = ODBA.storage.restore_named(name)
				if(dump.nil?)
					obj = block.call
					obj.odba_name = name
					obj.odba_store(name)
					obj
				else
					fetch_or_restore(name, dump, odba_caller)
				end	
			}
		end
		def fetch_or_do(obj_id, odba_caller, &block) # :nodoc:
			if(cache_entry = fetch_cache_entry(obj_id))
				cache_entry.odba_add_reference(odba_caller)
				cache_entry.odba_object
			else
				block.call
			end
		end
		def fetch_or_restore(obj_id, dump, odba_caller) # :nodoc:
			fetch_or_do(obj_id, odba_caller) { 
				obj, collection = restore(dump)
				cache_entry = CacheEntry.new(obj)
				cache_entry.odba_add_reference(odba_caller)
				## only add collection elements that exist in the collection
				## table
				cache_entry.collection = collection
				obj = cache_entry.odba_object
				hash = obj.odba_prefetch? ? @prefetched : @fetched
				name = obj.odba_name
				odba_id = obj.odba_id
				@cache_mutex.synchronize {
					fetch_or_do(odba_id, odba_caller) {
						hash.store(odba_id, cache_entry)
						unless(name.nil?)
							hash.store(name, cache_entry)
						end
						## set access time to now
						cache_entry.odba_object
					}
				}
			}
		end
		def fill_index(index_name, targets) 
			self.indices[index_name].fill(targets)
		end
		# Checks wether the object identified by _odba_id_ has been loaded.
		def include?(odba_id)
			@fetched.include?(odba_id) || @prefetched.include?(odba_id)
		end
		def index_keys(index_name, length=nil)
			index = indices.fetch(index_name)
			index.keys(length)
		end
		# Returns a Hash-table containing all stored indices. 
		def indices
			@indices ||= fetch_named('__cache_server_indices__', self) {
				{}
			}
		end
		# Returns the next valid odba_id
		def next_id
			ODBA.storage.next_id
		end
		# Use this to load all prefetchable Persistables from the db at once
		def prefetch
			bulk_restore(ODBA.storage.restore_prefetchable)
		end
		def reap_object_connections # :nodoc:
			@reaper_min_id += REAPER_ID_STEP
			@reaper_max_id = @reaper_min_id + REAPER_ID_STEP
			if(@reaper_min_id > ODBA.storage.max_id)
				@reaper_min_id = 0
				@reaper_max_id = REAPER_ID_STEP
			end
			#puts "removing objects between #{@reaper_min_id} and #{@reaper_max_id}"
			ODBA.storage.remove_dead_objects(@reaper_min_id, @reaper_max_id)
			ODBA.storage.remove_dead_connections(@reaper_min_id, @reaper_max_id)
		end
		# Find objects in an index
		def retrieve_from_index(index_name, search_term, meta=nil)
			index = indices.fetch(index_name)
			ids = index.fetch_ids(search_term, meta)
			bulk_fetch(ids, nil)
		end
		# Create necessary DB-Structure / other storage-setup
		def setup
			ODBA.storage.setup
      @deferred_indices.each { |definition|
        unless(self.indices.include?(definition.index_name))
          create_index(definition)
        end
      }
      nil
		end
		def start_cleaner # :nodoc: 
			@cleaner = Thread.new {
				Thread.current.priority = self::class::CLEANER_PRIORITY
				loop {
					sleep(self::class::CLEANING_INTERVAL)
					begin
						clean 
					rescue StandardError => e
						puts e
						puts e.backtrace
					end
				}
			}
		end
		def start_reaper # :nodoc: 
			@grim_reaper = Thread.new {
				Thread.current.priority = -6
				loop {
					sleep(self::class::REAPER_INTERVAL)
					begin
						reap_object_connections 
					rescue StandardError => e
						puts e
						puts e.backtrace
					end
				}
			}
		end
		# Store a Persistable _object_ in the database
		def store(object)
			odba_id = object.odba_id
			if(ids = Thread.current[:txids])
				ids.unshift(odba_id)
			end
			dump = object.odba_isolated_dump
			store_collection_elements(object)
			name = object.odba_name
			prefetchable = object.odba_prefetch?
			ODBA.storage.store(odba_id, dump, name, prefetchable, object.class)
			target_ids = object.odba_target_ids
			store_object_connections(odba_id, target_ids)
			update_references(target_ids, object)
			update_indices(object)
			store_cache_entry(odba_id, object, name)
		end
		def store_cache_entry(odba_id, object, name=nil) # :nodoc:
			@cache_mutex.synchronize {
				cache_entry = fetch_cache_entry(odba_id)
				if(cache_entry.nil?)
					hash = object.odba_prefetch? ? @prefetched : @fetched
					cache_entry = CacheEntry.new(object)
					hash.store(odba_id, cache_entry)
					unless(name.nil?)
						hash.store(name, cache_entry)
					end
				end
				cache_entry.collection = object.odba_collection
				cache_entry.odba_object
			}
		end
		def store_collection_elements(obj) # :nodoc:
			odba_id = obj.odba_id
			collection = obj.odba_collection
			old_collection = []
			if(cache_entry = fetch_cache_entry(odba_id))
				old_collection = cache_entry.collection
			else
				old_collection = fetch_collection(obj)
			end
			(old_collection - collection).each { |key, value|
				key_dump = ODBA.marshaller.dump(key.odba_isolated_stub)
				ODBA.storage.collection_remove(odba_id, key_dump)
			}
			(collection - old_collection).each { |key, value|
				key_dump = ODBA.marshaller.dump(key.odba_isolated_stub)
				value_dump = ODBA.marshaller.dump(value.odba_isolated_stub)
				ODBA.storage.collection_store(odba_id, key_dump, value_dump)	
			}
		end
		def store_object_connections(odba_id, target_ids) # :nodoc:
			ODBA.storage.ensure_object_connections(odba_id, target_ids)
		end
		# Executes the block in a transaction. If the transaction fails, all 
		# affected Persistable objects are reloaded from the db (which by then has 
		# also performed a rollback). Rollback is quite inefficient at this time.
		def transaction(&block)
			Thread.current[:txids] = []
			ODBA.storage.transaction(&block)
		rescue Exception => excp
			transaction_rollback
			raise excp
		ensure
			Thread.current[:txids] = nil
		end
		def transaction_rollback # :nodoc:
			if(ids = Thread.current[:txids])
				ids.each { |id|
					if(entry = fetch_cache_entry(id))
						if(dump = ODBA.storage.restore(id))
							obj, collection = restore(dump)
							entry.collection = collection
							entry.odba_replace!(obj)
						else
							entry.odba_cut_connections!
						end
					end
				}
			end
		end
		def update_indices(odba_object) # :nodoc:
			if(odba_object.odba_indexable?)
				indices.each { |index_name, index|
					index.update(odba_object)
				}
			end
		end
		def update_references(target_ids, object) # :nodoc:
			target_ids.each { |odba_id|
				if(entry = fetch_cache_entry(odba_id))
					entry.odba_add_reference(object)
				end
			}
		end
		private
		def load_object(odba_id, odba_caller)
			dump = ODBA.storage.restore(odba_id)
			restore_object(odba_id, dump, odba_caller)
		end
		def restore(dump)
			obj = ODBA.marshaller.load(dump)
			collection = fetch_collection(obj)
      unless(obj.is_a?(Persistable))
        obj.extend(Persistable)
      end
			obj.odba_restore(collection)
			[obj, collection]
		end
		def restore_object(odba_id, dump, odba_caller)
			if(dump.nil?)
				raise OdbaError, "Unknown odba_id #{odba_id}"
			end
			fetch_or_restore(odba_id, dump, odba_caller)
		end
	end
end
