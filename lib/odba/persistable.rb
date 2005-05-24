#!/usr/bin/env ruby
# Persistable -- odba -- 29.04.2004 -- rwaltert@ywesee.com mwalder@ywesee.com

class Object
	def odba_instance
		self
	end
	#defined here for fetch_collection fetch in cache_server
	def odba_id
	end
	def odba_isolated_stub
		self
	end
end
module ODBA
	class Stub; end
	module Persistable
		attr_accessor :odba_name, :odba_prefetch
		ODBA_CACHE_METHODS = []
		ODBA_EXCLUDE_VARS = []
		ODBA_INDEXABLE = true
		ODBA_PREFETCH = false
		ODBA_PREDEFINE_SERIALIZABLE = ['@odba_target_ids']
		ODBA_SERIALIZABLE = []
		def dup
			twin = super
			odba_potentials.each { |name|
				var = twin.instance_variable_get(name)
				if(var.is_a?(ODBA::Stub))
					stub = var.dup
					stub.odba_container = twin
					twin.instance_variable_set(name, stub)
				end
			}
			twin
		end
		def odba_cache_methods
			self::class::ODBA_CACHE_METHODS
		end
		def odba_cache_values
			odba_cache_methods.collect { |symbol|
				if(self.respond_to?(symbol))
					[self.odba_id, symbol, self.send(symbol)]
				end
			}.compact
		end
		def odba_collection
			[]
		end
		def odba_cut_connection(remove_object)
			odba_potentials.each { |name|
				var = instance_variable_get(name)
				if(var.eql?(remove_object))
					instance_variable_set(name, nil)
				end
			}
		end
		def odba_delete
			#ODBA.transaction {
				ODBA.cache_server.delete(self)
			#}
		end
		def odba_id
			@odba_id ||= ODBA.storage.next_id
		end
		def odba_isolated_dump
			ODBA.marshaller.dump(odba_isolated_twin)
		end
		def odba_isolated_store
			@odba_persistent = true
			ODBA.cache_server.store(self)
		end		
		def odba_isolated_stub
			Stub.new(self.odba_id, nil, self)
		end
		def odba_isolated_twin
			# ensure a valid odba_id
			self.odba_id
			twin = self.dup
			twin.odba_replace_persistables
			twin.odba_replace_excluded!
			twin
		end
		def odba_prefetch?
			@odba_prefetch || self::class::ODBA_PREFETCH
		end
		def odba_indexable?
			@odba_indexable || self::class::ODBA_INDEXABLE
		end
		def odba_potentials
			instance_variables - odba_serializables - self::class::ODBA_EXCLUDE_VARS
		end
		def odba_replace_persistable(obj)
			odba_potentials.each { |name|
				var = instance_variable_get(name)
				# must not be synchronized because of the following if
				# statement (if an object has already been replaced by
				# a	stub, it will have the correct id and it
				# will be ignored) 
				if(var.is_a?(Persistable) \
					&& var.odba_id == obj.odba_id) 
					stub = ODBA::Stub.new(obj.odba_id, self, obj)
					instance_variable_set(name, stub) 
				end
			}
		end
		def odba_replaceable?(var, name)
			var.is_a?(ODBA::Persistable) && (!var.is_a?(ODBA::Stub)) #\
			#&& (!odba_serializables.include?(name))
		end
		def odba_replace_persistables
			odba_potentials.each { |name|
				var = instance_variable_get(name)
				if(odba_replaceable?(var, name))
					odba_id = var.odba_id
					stub = ODBA::Stub.new(odba_id, self, var)
					instance_variable_set(name, stub)
				elsif(var.is_a?(ODBA::Stub))
					var.odba_clear_receiver
				end
			}
		end
		def odba_replace_stubs(stub, substitution, name = nil)
			if(name)
				instance_variable_set(name, substitution)
			else
				odba_potentials.each { |name|
					var = instance_variable_get(name)
					if(stub.eql?(var))
						instance_variable_set(name, substitution)
					end
				}
			end
		end
		def odba_restore(collection=[])
		end
		def odba_serializables
			self::class::ODBA_PREDEFINE_SERIALIZABLE \
				+ self::class::ODBA_SERIALIZABLE
		end
		def odba_store_unsaved
			@odba_persistent = false
			current_level = [self]
			while(!current_level.empty?)
				next_level = []
				current_level.each { |item|
					if(item.odba_unsaved?)
						next_level += item.odba_unsaved_neighbors
						item.odba_isolated_store
					end
				}
				current_level = next_level #.uniq
			end
		end
		def odba_snapshot(snapshot_level)
			if(snapshot_level > @odba_snapshot_level.to_i)
				@odba_snapshot_level = snapshot_level
				odba_isolated_store
			end
		end
		def odba_store(name = nil)
			begin
				unless (name.nil?)
					old_name = @odba_name
					@odba_name = name
				end
				odba_store_unsaved
			rescue DBI::ProgrammingError => e
				@odba_name = old_name
				raise
			end
		end
		def odba_take_snapshot
			@odba_snapshot_level ||= 0
			snapshot_level = @odba_snapshot_level.next
			current_level = [self]
			tree_level = 0
			while(!current_level.empty?)
				tree_level += 1
				obj_count = 0
				next_level = []
				current_level.each { |item|
					if(item.odba_unsaved?(snapshot_level))
						obj_count += 1
						next_level += item.odba_unsaved_neighbors(snapshot_level)
						item.odba_snapshot(snapshot_level)
					end
				}
				current_level = next_level #.uniq
			end
		end
		def odba_target_ids
			odba_potentials.collect { |name|
				var = instance_variable_get(name)
				if(var.is_a?(ODBA::Persistable))
					var.odba_id
				end
			}.compact.uniq
		end
		def odba_unsaved_neighbors(snapshot_level = nil)
			unsaved = []
			odba_potentials.each { |name|
=begin
				unless(self::class::ODBA_EXCLUDE_VARS.include?(name))
					item = instance_variable_get(name)
					#odba_extend_enumerable(item)
					if(item.is_a?(ODBA::Persistable) \
						&& item.odba_unsaved?(snapshot_level))
						unsaved.push(item)
					end
=end
				item = instance_variable_get(name)
					if(item.is_a?(ODBA::Persistable) \
						&& item.odba_unsaved?(snapshot_level))
						unsaved.push(item)
					end
				}
			unsaved
		end
		def odba_unsaved?(snapshot_level = nil)
			if(snapshot_level.nil?)
				!@odba_persistent
				#true
			else
				@odba_snapshot_level.to_i < snapshot_level
			end
		end
		protected
		def odba_replace_excluded!
			self::class::ODBA_EXCLUDE_VARS.each { |name|
				instance_variable_set(name, nil)
			}
		end
	end
end
class Array
	include ODBA::Persistable
	ODBA_CACHE_METHODS = [:length, :size, :empty?]
	def odba_collection
		coll = []
		each_with_index { |item, index|
			coll.push([index, item])	
		}
		coll
	end
	def odba_cut_connection(remove_object)
		super(remove_object)
		delete_if { |val| val.eql?(remove_object) }
	end
	def odba_prefetch?
		super || any? { |item| 
			item.respond_to?(:odba_prefetch?) \
				&& item.odba_prefetch? 
		}
	end
	def odba_replace_persistables
		clear
		super
	end
	def odba_restore(collection=[])
		collection.each { |key, val| 
			self[key] = val
		}
		## can be phased out...
		bulk_fetch_ids = []
		each { |item|
			if(item.is_a?(ODBA::Stub))
				bulk_fetch_ids.push(item.odba_id)
			end
		}
		unless(bulk_fetch_ids.empty?)
			ODBA.cache_server.bulk_fetch(bulk_fetch_ids, self)
			each_with_index { |item, idx|
				if(item.is_a? ODBA::Stub)
					self[idx] = item.odba_instance
				end
			}
		end
		##
	end
	def odba_unsaved_neighbors(snapshot_level = nil)
		unsaved = super
		each { |item|
			#odba_extend_enumerable(item)
			if(item.is_a?(ODBA::Persistable) \
				&& item.odba_unsaved?(snapshot_level))
				unsaved.push(item)
			end
		}
		unsaved
	end
	def odba_unsaved?(snapshot_level = nil)
		super || (snapshot_level.nil? && any? { |val|
			val.is_a?(ODBA::Persistable) && val.odba_unsaved?
		} )
	end
	unless(instance_methods.include?('odba_flatten!'))
		alias :odba_flatten! :flatten!
		def flatten!
			odba_restore
			odba_flatten!
		end
	end
	def odba_target_ids
		ids = super
		self.each { |value|
			if(value.is_a?(ODBA::Persistable))
				ids.push(value.odba_id)
			end
		}
		ids.uniq
	end
end
class Hash
	include ODBA::Persistable
	ODBA_CACHE_METHODS = [:length, :size, :empty?]
	def odba_cut_connection(remove_object)
		super(remove_object)
		delete_if { |key, val|
			key.eql?(remove_object) || val.eql?(remove_object)
		}
	end
	def odba_collection
		self.to_a
	end
	def odba_prefetch?
		super || any? { |item|
			item.respond_to?(:odba_prefetch?) \
				&& item.odba_prefetch?
		}
	end
	def odba_replace_persistables
		clear
		super
	end
	def odba_restore(collection=[])
		bulk_fetch_ids = []
		self.each { |key, value|
			if(value.is_a?(ODBA::Stub))
				bulk_fetch_ids.push(value.odba_id)
			end
			if(key.is_a?(ODBA::Stub))
				bulk_fetch_ids.push(key.odba_id)
			end
		}
		ODBA.cache_server.bulk_fetch(bulk_fetch_ids, self)
		self.each { |key, value|
			if(value.is_a?(ODBA::Stub))
				value = value.odba_instance
				self[key] = value
			end
			if(key.is_a?(ODBA::Stub))
				delete(key)
				store(key.odba_instance, value)
			end
		}
		collection.each { |key, val| 
			self[key] = val
		}
	end
	def odba_unsaved?(snapshot_level = nil)
		super || (snapshot_level.nil? && any? { |key, val|
			val.is_a?(ODBA::Persistable) && val.odba_unsaved? \
				|| key.is_a?(ODBA::Persistable) && key.odba_unsaved?
		})
	end
	def odba_unsaved_neighbors(snapshot_level = nil)
		unsaved = super
		each { |pair|
			pair.each { |item|
				#odba_extend_enumerable(item)
				if(item.is_a?(ODBA::Persistable)\
					&& item.odba_unsaved?(snapshot_level))
					unsaved.push(item)
				end
			}
		}
		unsaved
	end
	def odba_target_ids
		ids = super
		self.each { |key, value|
			if(value.is_a?(ODBA::Persistable))
				ids.push(value.odba_id)
			end
			if(key.is_a?(ODBA::Persistable))
				ids.push(key.odba_id)
			end
		}
		ids.uniq
	end
end
