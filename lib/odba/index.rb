#!/usr/bin/env ruby
# Index -- odba -- 13.05.2004 -- rwaltert@ywesee.com

module ODBA
	class IndexCommon
		include Persistable
		ODBA_EXCLUDE_VARS = ['@proc_target', '@proc_origin']
		def initialize(index_definition, origin_module)
			@origin_klass = origin_module.instance_eval(index_definition.origin_klass.to_s)
			@target_klass = origin_module.instance_eval(index_definition.target_klass.to_s)
			@resolve_origin = index_definition.resolve_origin
			@resolve_target = index_definition.resolve_target
			@index_name = index_definition.index_name
			@resolve_search_term = index_definition.resolve_search_term
			@dictionary = index_definition.dictionary
		end
		def do_update_index(origin_id, search_term, target_id)
			if(search_term.is_a?(Array))
				search_term.compact.each { |term|
					do_update_index(origin_id, term, target_id)
				}
			elsif(search_term && !search_term.empty?)
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
		def origin_class?(klass)
			(@origin_klass == klass)
		end
		def proc_instance_origin
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
		def proc_instance_target
			if(@proc_target.nil?)
				if(@resolve_target.to_s.empty?)
					@proc_target = Proc.new { |odba_item|  [odba_item] }
				else
					src =	 <<-EOS
						Proc.new { |odba_item| 
							res = [odba_item.#{@resolve_target}]
							res.flatten!
							res.compact!
							res
						}
					EOS
					@proc_target = eval(src)
				end
			end
			@proc_target
		end
		def proc_resolve_search_term
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
		def resolve_targets(odba_obj)
			@proc_target = nil
			proc_instance_target.call(odba_obj)
		end
		def search_term(odba_obj)
			proc_resolve_search_term.call(odba_obj)
		end
		def update(object)
			if(object.is_a?(@target_klass))
				update_target(object)
			elsif(object.is_a?(@origin_klass))
				self.update_origin(object)
			end
		end
		def update_origin(object)
			origin_id = object.odba_id
			search_term = search_term(object)
			target_objs = resolve_targets(object)		
			ODBA.storage.delete_index_element(@index_name, origin_id)
			target_objs.each { |target_obj|
				target_id = target_obj.odba_id
				do_update_index(origin_id, search_term, target_id)
			}
		end
		def update_target(object)
			target_id = object.odba_id
			ODBA.storage.index_delete_target(@index_name, target_id)
			fill([object])
		end
	end
	class Index < IndexCommon
		def initialize(index_definition, origin_module)
			super(index_definition, origin_module)
			ODBA.storage.create_index(index_definition.index_name)
		end
		def fetch_ids(search_term, meta=nil)
			exact = meta.respond_to?(:exact) && meta.exact
			rows = ODBA.storage.retrieve_from_index(@index_name, 
				search_term, exact)
			rows.collect { |row| row.at(0) }
		end
	end
	class FulltextIndex < IndexCommon
		def initialize(index_definition, origin_module)
			super(index_definition, origin_module)
			ODBA.storage.create_fulltext_index(index_definition.index_name)
		end
		def fetch_ids(search_term, meta=nil)
			rows = ODBA.storage.retrieve_from_fulltext_index(@index_name, 
				search_term, @dictionary)
			if(meta.respond_to?(:set_relevance))
				rows.each { |row|
					meta.set_relevance(row.at(0), row.at(1))
				}
			end
			rows.collect { |row| row.at(0) }
		end
		def do_update_index(origin_id, search_term, target_id)
				ODBA.storage.update_fulltext_index(@index_name, origin_id, search_term, target_id, @dictionary) 
		end
	end
end
