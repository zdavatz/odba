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

module ODBA
	def make_atc_index
		src = <<-EOS
		Proc.new { |atc_class|
			atc_class.sequences
		}
		EOS
		create_index('atc_index', AtcClass, src, :name)
	end
	def make_fachinfo_index
		a_proc = Proc.new { |atc_class| 
			atc_class.registrations.inject([]) { |inj, reg|
				if(fi = reg.fachinfo)
					inj += reg.fachinfo.descriptions.values
				end
			}
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
	def storage
		@storage ||= ODBA::Storage.instance
	end
	def storage=(storage)	
		@storage = storage
	end
	module_function :cache_server
	module_function :cache_server=
	module_function :marshaller
	module_function :marshaller=
	module_function :storage
	module_function :storage=
end

