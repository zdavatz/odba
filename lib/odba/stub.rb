#!/usr/bin/env ruby

require 'yaml'

module ODBA
	class Stub
		attr_accessor :odba_id, :odba_container
		attr_reader :receiver
		def initialize(odba_id, odba_container, receiver=nil)
			@odba_id = odba_id
			@odba_container = odba_container
			#=begin
			@carry_bag = {}
			if(receiver)
				receiver.odba_carry_methods.each { |symbol|
					begin
						@carry_bag.store(symbol, receiver.send(symbol))
					rescue
					end
				}
			end
			#=end
			#			delegate_object_methods
		end
		def eql?(other)
			other.is_a?(Persistable) && other.odba_id == @odba_id
		end
		def hash
			@odba_id.to_i
		end
		def is_a?(klass)
			if(klass == Stub  || klass == Persistable)
				true
			else
				odba_replace
				@receiver.is_a?(klass)
			end
		end
		def method_missing(meth_symbol, *args, &block)
				@carry_bag[meth_symbol] or begin
				odba_replace
				@receiver.send(meth_symbol, *args, &block)
				end
		end
		def odba_replace(name=nil)
			if(@receiver.nil?)
				begin
					@receiver = ODBA.cache_server.fetch(@odba_id, @odba_container)
					@odba_container.odba_replace_stubs(self, @receiver)
				rescue OdbaError => e
					puts self.inspect
				end
			end
		end
		no_override = [
			"is_a?", "__id__", "__send__", "inspect", "hash", "eql?"
		]
		override_methods = Object.public_methods - no_override
		override_methods.each { |method|
			eval <<-EOS
				def #{method}(*args)
					#	puts "replaced method #{method}"
					odba_replace
					@receiver.#{method}(*args)
				end
			EOS
		}
=begin
		def respond_to?(*args)
			replace
			@receiver.respond_to?(*args)
		end
		def to_s
			replace
			@receiver.to_s
		end
=end
	end
end
