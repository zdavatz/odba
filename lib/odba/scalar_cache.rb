#!/usr/bin/env ruby
# ScalarCache -- oddb -- 13.07.2004 -- mwalder@ywesee.com, rwaltert@ywesee.com


module ODBA
	class ScalarCache
		include Persistable
		ODBA_INDEXABLE = false
		ODBA_PREFETCH = true
		ODBA_SERIALIZABLE = ['@hash']
		def initialize
			@hash = Hash.new
		end
		def delete(odba_id)
			@hash.delete_if { |key, val|
				key.first == odba_id
			}	
		end
		def fetch(*args)
			@hash[args]
		end
		def size
			@hash.size
		end
		def update(cache_values)
			cache_values.each { |odba_id, method_name, value|
				@hash.store([odba_id, method_name], value)
			}
		end
	end
end
