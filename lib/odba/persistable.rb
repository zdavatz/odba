#!/usr/bin/env ruby

module ODBA
	module Persistable
		attr_accessor :odba_name
		attr_reader :odba_target_ids
		ODBA_PREFETCH = false
		ODBA_PREDEFINE_SERIALIZABLE = ['@odba_target_ids']
		def dup
			twin = super
			twin.instance_variables.each { |name|
				var = twin.instance_variable_get(name)
				if(var.is_a?(ODBA::Stub))
					stub = ODBA::Stub.new(var.odba_id, twin)
					twin.instance_variable_set(name, stub)
				end
			}
			twin
		end
=begin
		def odba_extend_enumerable(item)
			if(item.is_a?(Hash))
				item.extend(PersistableHash)
			elsif(item.is_a?(Array))
				item.extend(PersistableArray)
			end
		end
=end
		def odba_id
			@odba_id ||= ODBA.storage.next_id
		end
		def odba_delete
			ODBA.cache_server.delete(self)
		end
		def odba_isolated_dump
			Thread.critical = true
			begin
				# ensure a valid odba_id
				self.odba_id
				twin = self.dup
				#odba_extend_enumerable(twin)
				twin.odba_replace_persistables
				twin.odba_replace_excluded!
				@odba_target_ids = twin.odba_target_ids
				dump = ODBA.marshaller.dump(twin)
			ensure
				Thread.critical = false
			end
			dump
		end
		def odba_prefetch?
			self::class::ODBA_PREFETCH
		end
		def odba_replace_persistable(obj)
			instance_variables.each { |name|
				var = instance_variable_get(name)
				if(var.is_a?(Persistable) \
					&& var.odba_id == obj.odba_id)
					stub = ODBA::Stub.new(obj.odba_id, self)
					instance_variable_set(name, stub)
				end
			}
		end
		def odba_replaceable?(var, name)
			(var.is_a?(ODBA::Persistable) \
				&& !ODBA_PREDEFINE_SERIALIZABLE.include?(name))
		end
		def odba_replace_persistables
			@odba_target_ids = []
			puts "odba_replace_persistables"
			puts self.class
			instance_variables.each { |name|
				var = instance_variable_get(name)
				#odba_extend_enumerable(var)
				if(odba_replaceable?(var, name))
					@odba_target_ids.push(var.odba_id)
					stub = ODBA::Stub.new(var.odba_id, self)
					instance_variable_set(name, stub)
				end
			}
		end
		def odba_replace_stubs(stub, substitution, name = nil)
			if(name)
				instance_variable_set(name, substitution)
			else
				instance_variables.each { |name|
					var = instance_variable_get(name)
					if(var.equal?(stub))
						puts "#{self.class} replacing #{name}"
						instance_variable_set(name, substitution)
						puts "#{self.class} finished replacing #{name}"
					end
				}
			end
		end
		def odba_restore
		end
		def odba_cut_connection(remove_object)
			instance_variables.each { |name|
				var = instance_variable_get(name)
				if(var.equal?(remove_object))
					instance_variable_set(name, nil)
				end
			}
		end
		def odba_store_unsaved
			@odba_persistent = false
			current_level = [self]
			tree_level = 0
			while(!current_level.empty?)
				puts "tree_level: #{tree_level}"
				puts "checking #{current_level.size} objects"
				tree_level += 1
				obj_count = 0
				next_level = []
				current_level.each { |item|
					if(item.odba_unsaved?)
						obj_count += 1
						next_level += item.odba_unsaved_neighbors
						item.odba_isolated_store
					end
				}
				current_level = next_level #.uniq
				#puts "saved #{obj_count} objects"
			end
		end
		def odba_snapshot(snapshot_level)
			if(snapshot_level > @odba_snapshot_level.to_i)
				@odba_snapshot_level = snapshot_level
				odba_isolated_store
			end
		end
		def odba_isolated_store
			@odba_persistent = true
			ODBA.cache_server.store(self)
		end
		def odba_store(name = nil)
			begin
				unless (name.nil?)
					old_name = @odba_name
					@odba_name = name
				end
				puts "#{name} odba store call"
				puts self.class
				#ODBA.cache_server.store(self)
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
				puts "tree_level: #{tree_level}"
				puts "checking #{current_level.size} objects"
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
				#puts "saved #{obj_count} objects"
			end
		end
		def odba_unsaved_neighbors(snapshot_level = nil)
			unsaved = []
			exclude = ODBA_PREDEFINE_SERIALIZABLE
			if(defined?(self::class::ODBA_EXCLUDE_VARS))
				exclude += self::class::ODBA_EXCLUDE_VARS
			end
			instance_variables.each { |name|
				unless(exclude.include?(name))
					item = instance_variable_get(name)
					#odba_extend_enumerable(item)
					if(item.is_a?(ODBA::Persistable) \
						&& item.odba_unsaved?(snapshot_level))
						unsaved.push(item)
					end
				end
			}
			unsaved
		end
		def odba_unsaved?(snapshot_level = nil)
			if(snapshot_level.nil?)
				!@odba_persistent
			else
				@odba_snapshot_level.to_i < snapshot_level
			end
		end
		protected
		def odba_replace_excluded!
			if(defined?(self::class::ODBA_EXCLUDE_VARS))
				exclude_vars = self::class::ODBA_EXCLUDE_VARS
			else
				return 
			end
			instance_variables.each { |name|
				if(exclude_vars.include?(name))
					instance_variable_set(name, nil)
				end
			}
		end
	end
=begin
	def store_all
		#GC.disable
		ObjectSpace.each_object(Persistable) { |pers|
			pers.odba_replace_persistables
		}
		ObjectSpace.each_object(Persistable) { |pers|
			pers.odba_store
		}
		#GC.enable
	end
=end
	#module_function :store_all
end
class Array
	include ODBA::Persistable
	def odba_prefetch?
		any? { |item| 
			item.respond_to?(:odba_prefetch?) \
				&& item.odba_prefetch? 
		}
	end
	def odba_restore
		bulk_fetch_ids = []
		each { |item|
			if(item.is_a?(ODBA::Stub))
				#ODBA.chache_server.bulk_fetch_add(item)
				bulk_fetch_ids.push(item.odba_id)
				#item.odba_replace
				#self[idx] = item.receiver
			end
		}
		ODBA.cache_server.bulk_fetch(bulk_fetch_ids, self)
		#ODBA.chache_server.bulk_fetch_execute
		each_with_index { |item, idx|
			if(item.is_a? ODBA::Stub)
				item.odba_replace
				self[idx] = item.receiver
			end
		}
	end
	def odba_cut_connection(remove_object)
		super(remove_object)
		delete_if{|val| val == remove_object}
	end
	def odba_replace_persistables
		super
		each_with_index { |item, idx|
			#odba_extend_enumerable(item)
			if(item.is_a?(ODBA::Persistable))
				@odba_target_ids.push(item.odba_id)
				stub = ODBA::Stub.new(item.odba_id, self)
				self[idx] = stub
			end
		}
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
			puts "checking array elements"
			val.is_a?(ODBA::Persistable) && val.odba_unsaved?
		} )
	end
	unless(instance_methods.include?('old_flatten!'))
		alias :old_flatten! :flatten!
		def flatten!
			odba_restore
			old_flatten!
		end
	end
end
class Hash
	include ODBA::Persistable
	def odba_prefetch?
		any? { |item| 
			item.respond_to?(:odba_prefetch?) \
				&& item.odba_prefetch?
		}
	end
	def odba_cut_connection(remove_object)
		super(remove_object)
		delete_if{|key, val|
			key == remove_object || val == remove_object
		}
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
	def odba_restore
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
				value.odba_replace
				value = value.receiver
				self[key] = value
			end
			if(key.is_a?(ODBA::Stub))
				delete(key)
				key.odba_replace
				store(key.receiver, value)
			end
		}
	end
	def odba_replace_persistables
		super
		self.each {|key, value|
			if(value.is_a?(ODBA::Persistable))
				@odba_target_ids.push(value.odba_id)
				stub = ODBA::Stub.new(value.odba_id, self)
				value = stub
				self[key] = stub
			end
			if(key.is_a?(ODBA::Persistable))
				@odba_target_ids.push(key.odba_id)
				stub = ODBA::Stub.new(key.odba_id, self)
				delete(key)
				store(stub, value)
			end
		}
	end
	def odba_unsaved?(snapshot_level = nil)
		super || (snapshot_level.nil? && any? { |key, val|
			puts "checking hash elements"
			val.is_a?(ODBA::Persistable) && val.odba_unsaved? \
				|| key.is_a?(ODBA::Persistable) && key.odba_unsaved?
		})
	end
end