#!/usr/bin/env ruby
# FlatFileStorage -- odba -- 12.08.2004 -- hwyss@ywesee.com

require 'fileutils'

module ODBA
	class DumpWrapper
		attr_reader :arguments
		def initialize(*args)
			@arguments = args
		end
	end
	class FlatFileStorage
		DELIMITER = "\t"
		NULL = '\N'
		def initialize(datadir)
			@datadir = datadir
			@opath = File.expand_path('object.csv', @datadir)
			@cpath = File.expand_path('object_connection.csv', @datadir)
			FileUtils.mkdir_p(@datadir)
			@object = File.open(@opath, 'w')
			@connection = File.open(@cpath, 'w')
			@odba_id = 0
			@dump_wrappers = {}
		end
		def add_object_connection(origin, target)
			@connection.puts([origin, target].join(DELIMITER))
		end
		def close
			store_dump_wrappers
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
		def object_store(odba_id, dump, name, prefetchable)
			@object.puts [
				odba_id, dump, name || NULL, prefetchable
			].join(DELIMITER)
		end
		def restore_named(name)
			if(@dump_wrappers.has_key?(name))
				@dump_wrappers[name].dump
			end
		end
		def store(odba_id, dump, name, prefetchable)
			if(name)
				wrapper = ODBA::DumpWrapper.new(odba_id, dump, name, prefetchable)
				@dump_wrappers.store(name, wrapper)
			else
				object_store(odba_id,dump, name, prefetchable)
			end
		end
		def store_dump_wrappers
			@dump_wrappers.each { |key, dump_wrapper|
				object_store(*dump_wrapper.arguments)
			}
		end
		def transaction(&block)
			block.call
		end
=begin
# does not belong here, will be moved to ?
		def system_call_csv
			datao = "#{@datadir}/unique_object.csv"
			dataoc = "#{@datadir}/unique_object_connection.csv"
			system("sort #{@opath} | uniq > #{datao} -> #{datao}")
			system("sort #{@cpath} | uniq > #{dataoc} -> #{dataoc}")
		end
=end
	end
end
