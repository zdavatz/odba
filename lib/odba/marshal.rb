#!/usr/bin/env ruby

module ODBA
	module PersistableHash
	end
	module PersistableArray
	end
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
