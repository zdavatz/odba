#!/usr/bin/env ruby
# -- oddb -- 13.05.2004 -- rwaltert@ywesee.com


module ODBA
	class Index
		include Persistable
		ODBA_EXCLUDE_VARS = ['@proc']
		def initialize(origin_klass, mthd, proc_src = '')
			@origin_klass = origin_klass
			@proc_src = proc_src
			@mthd = mthd
		end
		def proc_instance
			if(@proc.nil?)
				if(@proc_src.to_s.empty?)
					@proc = Proc.new { |odba_item|  [odba_item] }
				else
					src = "Proc.new { |odba_item| 
						puts odba_item.#{@proc_src}
									[odba_item.#{@proc_src}]}"
					puts src
					@proc = eval(src)
				end
			end
			@proc
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
