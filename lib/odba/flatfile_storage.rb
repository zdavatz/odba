#!/usr/bin/env ruby
# FlatFileStorage -- odba -- 12.08.2004 -- hwyss@ywesee.com

require 'fileutils'

module ODBA
	class FlatFileStorage
		DELIMITER = "\t"
		NULL = '\N'
		def initialize(datadir)
			opath = File.expand_path('object.csv', datadir)
			cpath = File.expand_path('object_connection.csv', datadir)
			FileUtils.mkdir_p(datadir)
			@object = File.open(opath, 'w')
			@connection = File.open(cpath, 'w')
			@odba_id = 0
		end
		def add_object_connection(origin, target)
			@connection.puts([origin, target].join(DELIMITER))
		end
		def close
			@object.close
			@connection.close
		end
		def flush
			@object.flush
			@connection.flush
		end
		def next_id
			@odba_id += 1
			@odba_id
		end
		def store(odba_id, dump, name, prefetchable)
			@object.puts [
				odba_id, dump, name || NULL, prefetchable
			].join(DELIMITER)
		end
	end
end
