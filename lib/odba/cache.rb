#!/usr/bin/env ruby
# Cache -- odba -- 29.04.2004 -- rwaltert@ywesee.com mwalder@ywesee.com

require 'singleton'
require 'tmail'
require 'net/smtp'
require 'date'

module ODBA
	class Cache
		include Singleton
		CLEANER_PRIORITY = 0
		CLEANING_INTERVAL = 300
		REAPER_ID_STEP = 1000
		REAPER_INTERVAL = 900
		def initialize
			if(self::class::CLEANING_INTERVAL > 0)
				start_cleaner
				#start_reaper
			end
			@fetched = Hash.new
			@prefetched = Hash.new
			@clean_prefetched = false
			@reaper_min_id = 0
			@reaper_max_id = 0
		end
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
		def bulk_restore(rows, odba_caller = nil)
			retrieved_objects= []
			rows.each { |row|
				obj_id = row.at(0)
				dump = row.at(1)
				if(cache_entry = fetch_cache_entry(obj_id.to_i))
					obj = cache_entry.odba_object
					cache_entry.odba_add_reference(odba_caller)
				else
					obj = restore_object(dump, odba_caller)
				end
				retrieved_objects.push(obj)
			}
			retrieved_objects
		end
		def clean
			delete_old
			#cleaned = 0
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
			#$stdout.flush
		end
		def clean_prefetched(flag=true)
			if(@clean_prefetched = flag)
				clean
			end
		end
		def clear
			@fetched.clear
			@prefetched.clear
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
			@fetched.delete(odba_id)
			@fetched.delete(object.odba_name)
			@prefetched.delete(odba_id)
			@prefetched.delete(object.odba_name)
			ODBA.storage.delete_persistable(odba_id)
			delete_index_element(object)
			object
		end
		def delete_index_element(odba_object)
			klass = odba_object.class
			indices.each { |index_name, index|
				if(index.origin_class?(klass))
					ODBA.storage.delete_index_element(index_name, odba_object.odba_id)
				end
			}
		end
		def delete_old
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
		def fetch(odba_id, odba_caller=nil)
			if(cache_entry = fetch_cache_entry(odba_id))
				cache_entry.odba_add_reference(odba_caller)
				cache_entry.odba_object
			else
				load_object(odba_id, odba_caller)
			end
		end
		def fetch_cache_entry(odba_id)
			@prefetched[odba_id] || @fetched[odba_id]
		end
		def fetch_collection(obj)
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
		def fetch_collection_element(odba_id, key)
			key_dump = ODBA.marshaller.dump(key.odba_isolated_stub)
			## for backward-compatibility and robustness we only attempt
			## to load if there was a dump stored in the collection table
			if(dump = ODBA.storage.collection_fetch(odba_id, key_dump))
				ODBA.marshaller.load(dump)
			end
		end
		def fetch_named(name, caller, &block)
			cache_entry = fetch_cache_entry(name)
			obj = nil
			if(cache_entry.nil?)
				dump = ODBA.storage.restore_named(name)
				if(dump.nil?)
					obj = block.call
					obj.odba_name = name
					obj.odba_store(name)
				else
					obj = restore_object(dump, caller)
				end	
			else
				#add reference only in this case  
				cache_entry.odba_add_reference(caller)
				obj = cache_entry.odba_object
			end
			obj
		end
		def fill_index(index_name, targets)
			self.indices[index_name].fill(targets)
		end
		def include?(key)
			@fetched.include?(key) || @prefetched.include?(key)
		end
		def indices
			@indices ||= fetch_named('__cache_server_indices__', self) {
				{}
			}
		end
		def prefetch
			rows = ODBA.storage.restore_prefetchable
			bulk_restore(rows)
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
		def retrieve_from_index(index_name, search_term, meta=nil)
			index = indices.fetch(index_name)
			ids = index.fetch_ids(search_term, meta)
			bulk_fetch(ids, nil)
		end
		def start_cleaner
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
		def start_reaper
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
		def store(object)
			odba_id = object.odba_id
			dump = object.odba_isolated_dump
			store_collection_elements(odba_id, object.odba_collection)
			name = object.odba_name
			prefetchable = object.odba_prefetch?
			ODBA.storage.store(odba_id, dump, name, prefetchable)
			store_object_connections(odba_id, object.odba_target_ids)
			update_indices(object)
			store_cache_entry(odba_id, object, name)
		end
		def store_cache_entry(odba_id, object, name=nil)
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
		end
		def store_collection_elements(odba_id, collection)
			#odba_id = object.odba_id
			#collection = object.odba_collection
			old_collection = []
			if(cache_entry = fetch_cache_entry(odba_id))
				old_collection = cache_entry.collection
				(old_collection - collection).each { |key, value|
					key_dump = ODBA.marshaller.dump(key.odba_isolated_stub)
					ODBA.storage.collection_remove(odba_id, key_dump)
				}
			end
			(collection - old_collection).each { |key, value|
				key_dump = ODBA.marshaller.dump(key.odba_isolated_stub)
				value_dump = ODBA.marshaller.dump(value.odba_isolated_stub)
				ODBA.storage.collection_store(odba_id, key_dump, value_dump)	
			}
		end
		def store_object_connections(odba_id, target_ids)
			ODBA.storage.ensure_object_connections(odba_id, target_ids)
		end
		def update_indices(odba_object)
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
		def load_object(odba_id, caller)
			dump = ODBA.storage.restore(odba_id)
			begin
				restore_object(dump, caller)
			rescue OdbaError => odba_error
				if(@last_timeout.nil? || (Time.now - @last_timeout) > 300)
					text = TMail::Mail.new
					recipients = self::class::MAIL_RECIPIENTS
					text.set_content_type('text', 'plain', 'charset'=>'ISO-8859-1')
					text.body = <<-EOM
Error loading object unknown odba_id #{odba_id}"
#{::Kernel.caller.join("\n")}
					EOM
					text.from = self::class::MAIL_FROM
					text.to = recipients
					text.subject = "ODBA ID ERROR"
					text.date = Time.now
					text['User-Agent'] = 'ODBA Framework'
					if(recipients.size > 0)
						begin
							Net::SMTP.start(self::class::SMTP_SERVER) { |smtp|
								smtp.sendmail(text.encoded, self::class::MAIL_FROM, recipients.uniq)
							}
						rescue Timeout::Error
							@last_timeout = Time.now
						end
					end
				end
				raise odba_error
			end
		end
		def restore_object(dump, odba_caller)
			if(dump.nil?)
				raise OdbaError, "Unknown odba_id"
			end
			obj = ODBA.marshaller.load(dump)
			collection = fetch_collection(obj)
			obj.odba_restore(collection)
			cache_entry = CacheEntry.new(obj)
			cache_entry.odba_add_reference(odba_caller)
			#only add collection elements that exist in the collection
			#table
			cache_entry.collection = collection
			obj = cache_entry.odba_object
 ## Thread-Critical ##
 ## if(@hash.include?(obj.odba_id))
 ##		@hash[obj.odba_id].odba_object
 ## else
			hash = obj.odba_prefetch? ? @prefetched : @fetched
 			hash.store(obj.odba_id, cache_entry)
			name = obj.odba_name
 			unless(name.nil?)
				hash.store(name, cache_entry)
			end
 ## bis da. ##
			obj
=begin
			obj = ODBA.marshaller.load(dump)
			collection = fetch_collection(obj)
			obj.odba_restore(collection)
			cache_entry = CacheEntry.new(obj)
			cache_entry.odba_add_reference(odba_caller)
			#only add collection elements that exist in the collection
			#table
			cache_entry.collection = collection
			cache_entry.odba_object
=end
		end
	end
end
