#!/usr/bin/env ruby
# Index -- odba -- 13.05.2004 -- rwaltert@ywesee.com


module ODBA
	class IndexCommon
		include Persistable
		ODBA_EXCLUDE_VARS = ['@proc_target', '@proc_origin']
		def initialize(index_definition, origin_module)
			@origin_klass = origin_module.const_get(index_definition.origin_klass)
			@target_klass = origin_module.const_get(index_definition.target_klass)
			@resolve_origin = index_definition.resolve_origin
			@resolve_target = index_definition.resolve_target
			@index_name = index_definition.index_name
			@resolve_search_term = index_definition.resolve_search_term
		end
		def do_update_index(origin_id, search_term, target_id)
			puts "updating with values #{origin_id} #{search_term} #{target_id}"
			ODBA.storage.update_index(@index_name, origin_id, 
				search_term, target_id)
		end
		def fill(targets)
			@proc_origin = nil
			rows = []
			targets.flatten.each { |target|
				target_id = target.odba_id
				puts "calllign origin"
				origins = proc_instance_origin.call(target)
				origins.each { |origin|
=begin
					#puts "********ITEM****"
					puts origin.class
					#	puts "**************"
					#		origin = origin.odba_container
					#	origin = origin.first
					value = if(@resolve_search_term && origin.respond_to?(@search_term_mthd))
					#puts "sending #{origin}.#{@mthd}"
						origin.send(@resolve_search_term)
					else
						#	puts "sending #{origin}.to_s"
						origin.to_s
					end
					#	puts "origin #{origin}"
					rows << [origin.odba_id, value, target_id]
=end
					do_update_index( origin.odba_id, 
						self.search_term(origin), target_id)
					puts "origin is a"
					puts origin.class
					#self.update_origin(origin)
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
					puts "create proc"
					src = <<-EOS
						Proc.new { |odba_item| 
							#		puts odba_item.#{@resolve_origin}
							res = [odba_item.#{@resolve_origin}]
							puts res.size
							res.flatten!
							puts res.size
							puts "compacting"
							res.compact!
							puts res.size
							res
						}
					EOS
					puts src
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
									puts src
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
							puts "resolve search term #{@resolve_search_term}"
							puts origin.#{@resolve_search_term}
							origin.#{@resolve_search_term}
						}
					EOS
					@proc_resolve_search_term = eval(src)
				end
			end
			@proc_resolve_search_term
		end
		def resolve_targets(odba_obj)
			@proc_target = nil
			puts "sending to pro_target"
			puts odba_obj.class
			proc_instance_target.call(odba_obj)
		end
		def search_term(odba_obj)
			proc_resolve_search_term.call(odba_obj)
=begin
			if(odba_obj.respond_to?(@resolve_search_term))
				puts "responding"
				search_trm = odba_obj.send(@resolve_search_term)
				puts search_trm
				search_trm
			else
				puts "doing to s"
				odba_obj.to_s
			end
=end			
		end
		def update(object)
			if(object.is_a?(@target_klass))
				update_target(object)
			elsif(object.is_a?(@origin_klass))
				self.update_origin(object)
			end
		end
		def update_origin(object)
			puts "in origin"
			origin_id = object.odba_id
			search_term = search_term(object)
			puts search_term
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
		def retrieve_data(search_term)
			ODBA.storage.retrieve_from_index(@index_name, search_term)
		end
	end
	class FulltextIndex < IndexCommon
		def initialize(index_definition, origin_module)
			super(index_definition, origin_module)
			ODBA.storage.create_fulltext_index(index_definition.index_name)
		end
		def retrieve_data(search_term)
			bulks = ODBA.storage.retrieve_from_fulltext_index(@index_name, search_term)
		end
		def do_update_index(origin_id, search_term, target_id)
				puts "updating fulltext index"
				ODBA.storage.update_fulltext_index(@index_name, origin_id, search_term, target_id) 
		end
	end
end
