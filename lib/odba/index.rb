#!/usr/bin/env ruby
# -- oddb -- 13.05.2004 -- rwaltert@ywesee.com


module ODBA
	class Index
		include Persistable
		ODBA_EXCLUDE_VARS = ['@proc']
		def initialize(index_name, origin_klass, target_klass, mthd, resolve_target, resolve_origin  ='')
			@origin_klass = origin_klass
			@target_klass = target_klass
			@resolve_origin = resolve_origin
			@resolve_target = resolve_target
			@index_name = index_name
			@mthd = mthd
			ODBA.storage.create_index(index_name)
		end
		def proc_instance_origin
			if(@proc_origin.nil?)
				if(@resolve_origin.to_s.empty?)
					@proc_origin = Proc.new { |odba_item|  [odba_item] }
				else
					src = "Proc.new { |odba_item| 
						puts odba_item.#{@resolve_origin}
									[odba_item.#{@resolve_origin}]}"
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
					src = "Proc.new { |odba_item| 
									[odba_item.#{@resolve_target}]}"
									#					puts src
					@proc_target = eval(src)
				end
			end
			@proc_target
		end
		def resolve_targets(odba_obj)
		 if(target_obj = proc_instance_target.call(odba_obj))
			 [target_obj].flatten
		 else
			 []
		 end
		end
		def origin_class?(klass)
			(@origin_klass == klass)
		end
		def search_term(odba_obj)
			odba_obj.send(@mthd)
		end
		def fill(targets)
			@proc = nil
			rows = []
			targets.each { |target|
				target_id = target.odba_id
				items = proc_instance_origin.call(target)
				#puts "******************"
				#puts items.inspect
				#puts "*******************"
				items.flatten!
				#	puts "after flatten"
				#puts items.inspect
				items.each { |item|
					#puts "********ITEM****"
					puts item.class
					#	puts "**************"
					#		item = item.odba_container
					#	item = item.first
					value = if(@mthd && item.respond_to?(@mthd))
					#puts "sending #{item}.#{@mthd}"
						item.send(@mthd)
					else
						#	puts "sending #{item}.to_s"
						item.to_s
					end
					#	puts "item #{item}"
					rows << [item.odba_id, value, target_id]
				}
			}
			rows
		end
		def update(object)
			if(object.is_a?(@target_klass))
				puts "****"
				update_target(object)
			elsif(object.is_a?(@origin_klass))
				update_origin(object)
			end
		end
		def update_target(object)
			target_id = object.odba_id
			ODBA.storage.delete_target_ids(target_id, @index_name)
			fill([object])
		end
		def update_origin(object)
			origin_id = object.odba_id
			search_term = search_term(object)
			#ODBA.storage.delete_origin_ids(origin_id, @index_name)
			target_objs = resolve_targets(object)		
			target_objs.each { |target_id|
				ODBA.storage.update_index(@index_name, origin_id, search_term, target_id)
			}
		end
	end
end
