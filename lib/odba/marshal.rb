#!/usr/bin/env ruby
# Marshal -- odba -- 29.04.2004 -- rwaltert@ywesee.com mwalder@ywesee.com

module ODBA
	module Marshal
		def dump(obj)
			binary = ::Marshal.dump(obj)
			binary.unpack('H*').first
		end
		def load(hexdump)
			binary = [hexdump].pack('H*')
			::Marshal.load(binary)
		end
		module_function :dump
		module_function :load
	end
end
