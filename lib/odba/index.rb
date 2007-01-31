#!/usr/bin/env ruby
#-- Index -- odba -- 13.05.2004 -- rwaltert@ywesee.com

require 'odba/persistable'

module ODBA
	# Indices in ODBA are defined by origin and target (which may be identical)
	# Any time a Persistable of class _target_klass_ or _origin_klass_ is stored,
	# all corresponding indices are updated. To make this possible, we have to tell
	# Index, how to navigate from _origin_ to _target_ and vice versa.
	# This entails the Limitation that these paths must not change without
	# _origin_ and/or _target_ being stored.
	# Further, _search_term_ must be resolved in relation to _origin_.
	class IndexCommon # :nodoc: all
		include Persistable
		ODBA_EXCLUDE_VARS = ['@proc_origin']
		attr_accessor :origin_klass, :target_klass, :resolve_origin,
			:resolve_search_term, :index_name
		def initialize(index_definition, origin_module)
			@origin_klass = origin_module.instance_eval(index_definition.origin_klass.to_s)
			@target_klass = origin_module.instance_eval(index_definition.target_klass.to_s)
			@resolve_origin = index_definition.resolve_origin
			@index_name = index_definition.index_name
			@resolve_search_term = index_definition.resolve_search_term
			@dictionary = index_definition.dictionary
		end
		def do_update_index(origin_id, search_term, target_id=nil) # :nodoc:
			if(search_term.is_a?(Array))
				search_term.compact.each { |term|
					do_update_index(origin_id, term, target_id)
				}
			elsif(!search_term.to_s.empty?)
				ODBA.storage.update_index(@index_name, origin_id, 
					search_term, target_id)
			end
		end
		def fill(targets)
			@proc_origin = nil
			rows = []
			targets.flatten.each { |target|
				target_id = target.odba_id
				origins = proc_instance_origin.call(target)
				origins.each { |origin|
					do_update_index( origin.odba_id, 
						self.search_term(origin), target_id)
				}
			}
		end
		def keys(length=nil)
			ODBA.storage.index_fetch_keys(@index_name, length)
		end
		def origin_class?(klass)
			(@origin_klass == klass)
		end
		def proc_instance_origin # :nodoc:
			if(@proc_origin.nil?)
				if(@resolve_origin.to_s.empty?)
					@proc_origin = Proc.new { |odba_item|  [odba_item] }
				else
					src = <<-EOS
						Proc.new { |odba_item| 
							res = [odba_item.#{@resolve_origin}]
							res.flatten!
							res.compact!
							res
						}
					EOS
					@proc_origin = eval(src)
				end
			end
			@proc_origin
		end
		def proc_resolve_search_term # :nodoc:
			if(@proc_resolve_search_term.nil?)
				if(@resolve_search_term.to_s.empty?)
					@proc_resolve_search_term = Proc.new { |origin| origin.to_s }
				else
					src = <<-EOS
						Proc.new { |origin| 
							begin
								origin.#{@resolve_search_term}
							rescue NameError
								nil
							end
						}
					EOS
					@proc_resolve_search_term = eval(src)
				end
			end
			@proc_resolve_search_term
		end
		def search_term(origin) # :nodoc:
			proc_resolve_search_term.call(origin)
		end
		def set_relevance(meta, rows) # :nodoc:
			if(meta.respond_to?(:set_relevance))
				rows.each { |row|
					meta.set_relevance(row.at(0), row.at(1))
				}
			end
		end
		def update(object)
			if(object.is_a?(@target_klass))
				update_target(object)
			elsif(object.is_a?(@origin_klass))
				update_origin(object)
			end
		end
		def update_origin(object) # :nodoc:
			origin_id = object.odba_id
			search_term = search_term(object)
      target_ids = ODBA.storage.index_target_ids(@index_name, origin_id)
			ODBA.storage.delete_index_element(@index_name, origin_id)
			target_ids.each { |target_id|
				do_update_index(origin_id, search_term, target_id)
			}
		end
		def update_target(object) # :nodoc:
			target_id = object.odba_id
			ODBA.storage.index_delete_target(@index_name, target_id)
			fill([object])
		end
	end
	# Currently there are 3 predefined Index-classes
	# For Sample Code see
	# http://dev.ywesee.com/wiki.php/ODBA/SimpleIndex
	# http://dev.ywesee.com/wiki.php/ODBA/ConditionIndex
	# http://dev.ywesee.com/wiki.php/ODBA/FulltextIndex
	class Index < IndexCommon # :nodoc: all
		def initialize(index_definition, origin_module) # :nodoc:
			super(index_definition, origin_module)
			ODBA.storage.create_index(index_definition.index_name)
		end
		def fetch_ids(search_term, meta=nil) # :nodoc:
			exact = meta.respond_to?(:exact) && meta.exact
			rows = ODBA.storage.retrieve_from_index(@index_name, search_term, exact)
			set_relevance(meta, rows)
			rows.collect { |row| row.at(0) }
		end
	end
	class ConditionIndex < IndexCommon # :nodoc: all  
		def initialize(index_definition, origin_module) # :nodoc:
			super(index_definition, origin_module)
			definition = {}
			@resolve_search_term = {}
			index_definition.resolve_search_term.each { |name, info|
				if(info.is_a?(String))
					info = { 'resolve' => info }
				end
				if(info['type'].nil?)
					info['type'] = 'text'
				end
				@resolve_search_term.store(name, info)
				definition.store(name, info['type'])
			}
			ODBA.storage.create_condition_index(@index_name, definition)
		end
		def do_update_index(origin_id, search_term, target_id=nil) # :nodoc:
			ODBA.storage.update_condition_index(@index_name, origin_id, 
				search_term, target_id)
		end
		def fetch_ids(conditions, meta=nil)  # :nodoc:
			rows = ODBA.storage.retrieve_from_condition_index(@index_name, conditions)
			set_relevance(meta, rows)
      rows.collect { |row| row.at(0) }
		end
		def proc_resolve_search_term # :nodoc:
			if(@proc_resolve_search_term.nil?)
				src = <<-EOS
					Proc.new { |origin| 
						values = {}
				EOS
				@resolve_search_term.each { |name, info|
					src << <<-EOS
						begin
							values.store('#{name}', origin.#{info['resolve']})
						rescue NameError
						end
					EOS
				}
				src << <<-EOS 
						values
					}
				EOS
				@proc_resolve_search_term = eval(src)
			end
			@proc_resolve_search_term
		end
	end
	class FulltextIndex < IndexCommon # :nodoc: all
		def initialize(index_definition, origin_module)  # :nodoc:
			super(index_definition, origin_module)
			ODBA.storage.create_fulltext_index(@index_name)
		end
		def fetch_ids(search_term, meta=nil)  # :nodoc:
			rows = ODBA.storage.retrieve_from_fulltext_index(@index_name, 
				search_term, @dictionary)
			set_relevance(meta, rows)
			rows.collect { |row| row.at(0) }
		end
    def do_update_index(origin_id, search_term, target_id=nil) # :nodoc:
      ODBA.storage.update_fulltext_index(@index_name, origin_id, search_term, target_id, @dictionary) 
    end
	end
end
