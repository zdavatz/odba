#!/usr/bin/env ruby
# ODBA -- odba -- 13.05.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

require 'odba/persistable'
require 'odba/storage'
require 'odba/cache'
require 'odba/stub'
require 'odba/marshal'
require 'odba/cache_entry'
require 'odba/odba_error'
require 'odba/index'
require 'odba/scalar_cache'
require 'thread'

module ODBA
	def batch(&block)
		transaction {
			begin
				@batch_mode = true
				cache_server.batch(&block)
			ensure
				@batch_mode = false
			end
		}
	end
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
	def scalar_cache=(scalar_cache)
		@scalar_cache = scalar_cache
	end
	def scalar_cache
		@scalar_cache ||= #cache_server.fetch_named('__scalar_cache__', self) {
			ScalarCache.new
		#}
	end
	def storage
		@storage ||= ODBA::Storage.instance
	end
	def storage=(storage)	
		@storage = storage
	end
	def transaction(&block)
		if(@batch_mode)
			block.call
		else
			@odba_mutex ||= Mutex.new
			@odba_mutex.synchronize {
				ODBA.storage.transaction {
					#res = 
					block.call
					#scalar_cache.odba_isolated_store
					#res
				}
			}
		end
	end
	def index_factory(index_definition, origin_module)	
		if(index_definition.fulltext)
			FulltextIndex.new(index_definition, origin_module)
		else
			Index.new(index_definition, origin_module)
		end
	end
	module_function :batch
	module_function :index_factory
	module_function :cache_server
	module_function :cache_server=
	module_function :marshaller
	module_function :marshaller=
	module_function :scalar_cache
	module_function :scalar_cache=
	module_function :storage
	module_function :storage=
	module_function :transaction
end
