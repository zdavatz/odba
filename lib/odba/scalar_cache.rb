#!/usr/bin/env ruby
# ScalarCache -- oddb -- 13.07.2004 -- mwalder@ywesee.com, rwaltert@ywesee.com


module ODBA
	class ScalarCache
		ODBA_INDEXABLE = false
		ODBA_PREFETCH = true
		ODBA_SERIALIZABLE = ['@scalar_cache']
		attr_reader :scalar_cache
		include Persistable
		def initialize
			@scalar_cache = Hash.new
		end
		def update(cache_values)
			delete(cache_values.first.first)
			cache_values.each { |val|
				@scalar_cache[[[val.at(0)], [val.at(1)]]] = val.at(2)
			}
		end
		def delete(odba_id)
			@scalar_cache.keys.each{ |key|
				if(key.first.first == odba_id)
					@scalar_cache.delete(key)
				end
			}	
		end
		def fetch(odba_id, method)
				@scalar_cache[[[odba_id],[method]]]
		end
	end
end
