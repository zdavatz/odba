#!/usr/bin/env ruby
# CacheEntry -- odba -- 29.04.2004 -- mwalder@ywesee.com

module ODBA
	class CacheEntry
		RETIRE_TIME = 10 
		DESTROY_TIME =  20 
		attr_accessor :last_access
		attr_reader :accessed_by
		def initialize(obj)
			@last_access = Time.now
			@odba_object = obj
			@accessed_by = []
		end
		def odba_add_reference(object)
			unless (object.nil?)
				@accessed_by.push(object)	
			end
		end
		def odba_object
			@last_access  = Time.now
			@odba_object
		end
		def ready_to_destroy?
			(!@odba_object.odba_prefetch? \
				&& ((Time.now - @last_access) > \
				self::class::DESTROY_TIME) && \
				@accessed_by.empty?)
		end
		def odba_old?
			Time.now - @last_access > self::class::RETIRE_TIME
		end
		def odba_retire
			#replace with stubs in accessed_by 
			keep = []
			@accessed_by.each { |item|
				if((item.is_a?(Enumerable) \
					&& ODBA.cache_server.include?(item.odba_id)) \
					|| (!item.is_a?(ODBA::Persistable)))
					keep.push(item)
				else
					item.odba_replace_persistable(@odba_object)	
				end
			}
			@accessed_by = keep
		end
	end
end