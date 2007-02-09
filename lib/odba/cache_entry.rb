#!/usr/bin/env ruby
#-- CacheEntry -- odba -- 29.04.2004 -- hwyss@ywesee.com mwalder@ywesee.com

module ODBA
	class CacheEntry # :nodoc: all
		RETIRE_TIME = 300
		DESTROY_TIME = 600
		attr_accessor :last_access, :collection, :stored_version
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
		def odba_old?
			!@odba_object.odba_unsaved? \
				&& Time.now - @last_access > self::class::RETIRE_TIME
		end
		def odba_retire
			#replace with stubs in accessed_by 
			@accessed_by.delete_if { |item, key|
        !(item.is_a?(Enumerable) \
          && ODBA.cache.include?(item.odba_id)) \
          && item.is_a?(ODBA::Persistable) \
          && item.odba_replace_persistable(@odba_object)
			}
      @accessed_by.empty?
		end
		def odba_replace!(obj)
			@odba_object = obj
			@accessed_by.each_key { |item|
        item.odba_replace(obj)
			}
		end
		def ready_to_destroy?
			!@odba_object.odba_unsaved? \
				&& @accessed_by.empty? \
				&& ((Time.now - @last_access) > self::class::DESTROY_TIME) 
		end
	end
end
