# rwaltert@ywesee.com mwalder@ywesee.com
#!/usr/bin/env ruby

require 'singleton'
require 'delegate'

module ODBA
	class Cache < SimpleDelegator
		attr_reader :indices
		include Singleton
		CLEANING_INTERVAL = 40
		def initialize
			#=begin
			@cleaner = Thread.new {
				loop {
					sleep(self::class::CLEANING_INTERVAL)
					begin
						puts "cleaning up DB"
						#clean
						#clean_object_connections
					rescue StandardError => e
						puts e
						puts e.backtrace
					end
				}
			}
			@cleaner.priority = -5
			#=end
			@hash = Hash.new
			super(@hash)
		end
		def indices
			@indices ||= fetch_named('__cache_server_indices__',self){
				#indices_hash = Hash.new
				#indices_hash.odba_name = '__cache_server_indices__'
				#indices_hash
				{}
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
			puts "in bulk_restore"
			retrieved_objects= []
			rows.each { |row|
				obj_id, dump = row
				#dump = row.first
				#obj = restore_object(dump)
				#puts " object class"
				#puts obj.class
				puts "********3"
				puts obj_id
				puts dump
				if(cache_entry = @hash.fetch(obj_id.to_i, false))
					puts "already loaded"
					obj = cache_entry.odba_object
					cache_entry.odba_add_reference(odba_caller)
				else
					puts "loading from DB"
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
			puts "clean object conne"
			ODBA.storage.remove_dead_objects
			ODBA.storage.remove_dead_connections
		end
		def delete(object)
			puts "in delete method"
			puts @hash.size
			rows = ODBA.storage.retrieve_connected_objects(object.odba_id)
			unless (rows.empty?)
			rows.each{ |row|
puts "rows each"
				id = row.first
				connected_object = fetch(id, nil)
				connected_object.odba_cut_connection(object)
=begin
				if(connected_object.is_a?(Hash))
					connected_object.delete_if{|key, val|
						key == object || val == object
					}
				elsif(connected_object.is_a?(Array))
					connected_object.delete_if{|val| val == object}
				else
					connected_object.instance_variables.each { |name|
						var = connected_object.instance_variable_get(name)
						if(var.equal?(object))
							connected_object.instance_variable_set(name, nil)
						end
					}
				end
=end
				puts "saving connected_object"
				puts "object is a : #{connected_object.class}"
				#store(connected_object, connected_object.odba_name)
				connected_object.odba_store
			}
			end
			@hash.delete(object.odba_id)
			@hash.delete(object.odba_name)
			ODBA.storage.delete_persistable(object.odba_id)
			delete_index_element(object)
		end
		def delete_index_element(odba_object)
			klass = odba_object.class
			indices.each { |index_name, index|
				if(index.origin_class?(klass))
					puts "deleting from index #{index_name}"
					ODBA.storage.delete_index_element(index_name, odba_object.odba_id)
				end
			}
		end
		def store(object)
			odba_id = object.odba_id
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
		def fetch(odba_id, odba_caller)
			cache_entry = @hash.fetch(odba_id) {
				obj = load_object(odba_id)
				cache_entry = CacheEntry.new(obj)
				if(name = obj.odba_name)
					@hash.store(name, cache_entry)
				end
				@hash.store(odba_id, cache_entry)
			}
			cache_entry.odba_add_reference(odba_caller)
			cache_entry.odba_object
		end
		def prefetch
			rows = ODBA.storage.restore_prefetchable
			bulk_restore(rows)
		end
		def delete_old
			@hash.each { |key, value|
				#				puts "checking #{key} for deleting"
				if(value.ready_to_destroy?)
					#puts "deleting #{key} from cache"
					@hash.delete(key)
				end
			}
		end
		def drop_index(index_name)
			ODBA.storage.drop_index(index_name)
			self.indices[index_name].odba_delete
			puts "index #{index_name} deleted"
		end
		def drop_indices
			keys = self.indices.keys
			keys.each{ |key|
				drop_index(key)
			}	
		end
		def fetch_named(name, caller, &block)
			cache_entry = @hash[name]
			if(cache_entry.nil?)
				dump = ODBA.storage.restore_named(name)
				obj = nil
				if(dump.nil?)
					puts "#{name} dump is nil"
					obj = block.call
					puts "ater block call"
					obj.odba_name = name
					#			store(obj, name) 
					obj.odba_store(name)
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
		def create_index(index_name, origin_klass, target_klass, mthd, resolve_target, resolve_origin = '')
		index = Index.new(index_name, origin_klass, target_klass, mthd, resolve_target, resolve_origin)
		#ODBA.storage.create_index(index_name)
		self.indices.store(index_name, index)
		puts "store self.indices"
		#store(self.indices, '__cache_server_indices__')
		#index.odba_store
		self.indices.odba_store('__cache_server_indices__')
		puts "store index"
		index
		end
		def fill_index(index_name, targets)
			puts "in cache fill index"
			self.indices[index_name].fill(targets)
			puts "index filled"
		end
		def retrieve_from_index(index_name, search_term)
			bulks = ODBA.storage.retrieve_from_index(index_name, search_term)
			bulk_restore(bulks)

		end
		def update_indices(odba_object)
			klass = odba_object.class
			puts "klass #{klass}"
			indices.each { |index_name, index|
				index.update(odba_object)
=begin
				if(index.origin_class?(klass))
					puts "**********"
					#update index
					search_term = index.search_term(odba_object)
					puts "search_term"
					puts search_term
					if(target_id = index.resolve_target_id(odba_object))
						ODBA.storage.update_index(index_name, 
							odba_object.odba_id, search_term, target_id)
					end
				end
=end
			}
		end
		def delete_index(index_name)
			index = indices[index_name]
			indices.delete(index_name)
			#store(self.indices, '__cache_server_indices__')
			self.indices.odba_store
			delete(index)
			ODBA.storage.drop_index_table(index_name)
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
