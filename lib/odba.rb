#!/usr/bin/env ruby
# -- oddb -- 13.05.2004 -- rwaltert@ywesee.com mwalder@ywesee.com

require 'odba/persistable'
require 'odba/storage'
require 'odba/cache'
require 'odba/stub'
require 'odba/marshal'
require 'odba/cache_entry'
require 'odba/odba_error'
require 'odba/index'
require 'thread'

module ODBA
	def cache_server
		@cache_server ||= ODBA::Cache.instance
	end
	def cache_server=(cache_server)
		@cache_server = cache_server
	end
	def marshaller
		@marshaller ||= ODBA::Marshal
	end
	def marshaller=(marshaller)
		@marshaller = marshaller
	end
	def storage
		@storage ||= ODBA::Storage.instance
	end
	def storage=(storage)	
		@storage = storage
	end
	def transaction(&block)
		@odba_mutex ||= Mutex.new
		@odba_mutex.synchronize{
			ODBA.storage.transaction {
				block.call
			}
		}
	end
	def index_factory(index_definition, origin_module)	
		if(index_definition.fulltext)
			puts "creating fulltext index"
			FulltextIndex.new(index_definition, origin_module)
		else
			Index.new(index_definition, origin_module)
		end
	end
	module_function :index_factory
	module_function :cache_server
	module_function :cache_server=
	module_function :marshaller
	module_function :marshaller=
	module_function :storage
	module_function :storage=
	module_function :transaction
	
end

