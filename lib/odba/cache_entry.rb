#!/usr/bin/env ruby
#-- CacheEntry -- odba -- 29.04.2004 -- hwyss@ywesee.com mwalder@ywesee.com

module ODBA
	class CacheEntry # :nodoc: all
		attr_accessor :last_access
		attr_reader :accessed_by
		def initialize(obj)
			@last_access = Time.now
			@odba_object = obj
			@accessed_by = {}
		end	
    def object_id2ref(object_id)
      ObjectSpace._id2ref(object_id) 
    rescue Exception
    end
    def odba_id2ref(odba_id)
      odba_id && ODBA.cache.include?(odba_id) && ODBA.cache.fetch(odba_id)
    end
		def odba_add_reference(object)
      @accessed_by.store(object.object_id, object.odba_id)
			object
		end
		def odba_cut_connections!
			@accessed_by.each { |object_id, odba_id|
        if((item = odba_id2ref(odba_id) || object_id2ref(object_id)) \
          && item.respond_to?(:odba_cut_connection))
					item.odba_cut_connection(@odba_object)
				end
			}
		end
    def odba_destroy!
      @odba_object = nil
      @accessed_by = nil
      true
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
			@accessed_by.delete_if { |object_id, odba_id|
        if(item = odba_id2ref(odba_id))
          item.odba_stubize(@odba_object)
        elsif(item = object_id2ref(object_id))
          case item
          when Stub
            true
          when Persistable
            item.odba_stubize(@odba_object)
          end
        else
          true
        end
			}
		end
		def odba_replace!(obj)
      oldhash = @odba_object.hash
			@odba_object.odba_replace!(obj)
      if(@odba_object.hash != oldhash)
        @accessed_by.each { |object_id, odba_id|
          if(item = odba_id2ref(odba_id) || object_id2ref(object_id))
            item.rehash if(item.respond_to? :rehash)
          end
        }
      end
		end
		def ready_to_destroy?(destroy_horizon = Time.now - ODBA.cache.destroy_age)
			!@odba_object.odba_unsaved? \
				&& @accessed_by.empty? \
				&& (destroy_horizon > @last_access) 
		end
	end
end
