#!/usr/bin/env ruby
#-- CacheEntry -- odba -- 29.04.2004 -- hwyss@ywesee.com mwalder@ywesee.com

module ODBA
	class CacheEntry # :nodoc: all
		attr_accessor :last_access, :collection
		attr_reader :accessed_by
		def initialize(obj)
			@last_access = Time.now
			@odba_object = obj
			@collection = []
			@accessed_by = {}
		end	
		def odba_add_reference(object)
			if(object)
				@accessed_by.store(object, true)
			end
			object
		end
		def odba_cut_connections!
			@accessed_by.each_key { |item|
				if(item.is_a?(Persistable))
					item.odba_cut_connection(@odba_object) 
				end
			}
		end
		def odba_id
			@odba_object.odba_id
		end
    def odba_notify_observers(*args)
      @odba_object.odba_notify_observers(*args)
    end
		def odba_object
			@last_access  = Time.now
			@odba_object
		end
		def odba_old?(retire_horizon = Time.now - ODBA.cache.retire_age)
			!@odba_object.odba_unsaved? \
				&& (retire_horizon > @last_access)
		end
		def odba_retire
			# replace with stubs in accessed_by 
      # @accessed_by needs to be rehashed in some cases
      @accessed_by.rehash
			@accessed_by.delete_if { |item, key|
        !(item.is_a?(Enumerable) && ODBA.cache.include?(item.odba_id)) \
          && item.is_a?(ODBA::Persistable) \
          && item.odba_replace_persistable(@odba_object)
			}
		end
		def odba_replace!(obj)
			@odba_object = obj
			@accessed_by.each_key { |item|
        item.odba_replace(obj)
			}
		end
		def ready_to_destroy?(destroy_horizon = Time.now - ODBA.cache.destroy_age)
			!@odba_object.odba_unsaved? \
				&& @accessed_by.empty? \
				&& (destroy_horizon > @last_access) 
		end
	end
end
