#!/usr/bin/env ruby
#-- IdServer -- odba -- 10.11.2004 -- hwyss@ywesee.com

require 'odba'

module ODBA
	class IdServer
		include Persistable
		ODBA_SERIALIZABLE = ['@ids']
		def initialize
			@ids = {}
		end
		def next_id(key, startval=0)
			@ids[key] ||= (startval - 1)
			res = @ids[key] += 1
			odba_store
			res
		end
	end
end
