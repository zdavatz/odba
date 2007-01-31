#!/usr/bin/env ruby
# ODBA -- odba -- 26.01.2007 -- hwyss@ywesee.com

module ODBA
	# reader for the Cache server. Defaults to ODBA::Cache.instance
	def ODBA.cache
		@cache ||= ODBA::Cache.instance
	end
	# writer for the Cache server. You will probably never need this.
	def ODBA.cache=(cache_server)
		@cache = cache_server
	end
	# reader for the Marshaller. Defaults to ODBA.Marshal
	def ODBA.marshaller
		@marshaller ||= ODBA::Marshal
	end
	# writer for the Marshaller. Example: override the default Marshaller to
	# serialize your objects in a custom format (yaml, xml, ...).
	def ODBA.marshaller=(marshaller)
		@marshaller = marshaller
	end
	# reader for the Storage Server. Defaults to ODBA::Storage.instance
	def ODBA.storage
		@storage ||= ODBA::Storage.instance
	end
	# writer for the Storage Server. Example: override the default Storage Server
	# to dump all your data in a flatfile.
	def ODBA.storage=(storage)	
		@storage = storage
	end
	# Convenience method. Delegates the transaction-call to the Cache server.
	def ODBA.transaction(&block)
		ODBA.cache.transaction(&block)
	end
end
