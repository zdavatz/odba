#!/usr/bin/env ruby
# -- oddb -- 13.05.2004 -- rwaltert@ywesee.com


module ODBA
	class Index
		include Persistable
		ODBA_EXCLUDE_VARS = ['@proc']
		def initialize(origin_klass, mthd, resolve_target, resolve_origin  ='')
			@origin_klass = origin_klass
			@resolve_origin = resolve_origin
			@resolve_target = resolve_target
			@mthd = mthd
		end
		def proc_instance
			if(@proc.nil?)
				if(@resolve_origin.to_s.empty?)
					@proc = Proc.new { |odba_item|  [odba_item] }
				else
					src = "Proc.new { |odba_item| 
						puts odba_item.#{@resolve_origin}
									[odba_item.#{@resolve_origin}]}"
					puts src
					@proc = eval(src)
				end
			end
			@proc
		end
		def resolve_target_id(odba_obj)
		 if(target_obj = odba_obj.send(@resolve_target))
			 target_obj.odba_id
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
				items = proc_instance.call(target)
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
	end
end
