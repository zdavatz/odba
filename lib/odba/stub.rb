#!/usr/bin/env ruby
# Stub -- odba -- 29.04.2004 -- rwaltert@ywesee.com mwalder@ywesee.com

require 'yaml'

module ODBA
	class Stub
		attr_accessor :odba_id, :odba_container
		attr_reader :receiver
		def initialize(odba_id, odba_container, receiver)
			@odba_id = odba_id
			@odba_container = odba_container
			@odba_class = receiver.class unless receiver.nil? 
		end
		def eql?(other)
			other.is_a?(Persistable) && other.odba_id == @odba_id
		end
		def hash
			@odba_id.to_i
		end
		def is_a?(klass)
			if([Stub, Persistable, @odba_class].include?(klass))
			#if(klass == Stub  || klass == Persistable)
				true
			else
				odba_replace
				@receiver.is_a?(klass)
			end
		end
		def method_missing(meth_symbol, *args, &block)
			if(@odba_class \
				&& @odba_class::ODBA_CACHE_METHODS.include?(meth_symbol) \
				&& (res = ODBA.scalar_cache.fetch(@odba_id, meth_symbol)))
				res
			else
				odba_replace
				@receiver.send(meth_symbol, *args, &block)
			end
		end
		def odba_instance
			odba_replace
			@receiver
		end
		def odba_isolated_stub
			stub = dup
			stub.odba_container = nil
			stub
		end
		def odba_replace(name=nil)
			if(@receiver.nil?)
				begin
					@receiver = ODBA.cache_server.fetch(@odba_id, @odba_container)
					@odba_container.odba_replace_stubs(self, @receiver)
				rescue OdbaError => e
					#require 'debug'
					#puts "ODBA::Stub was unable to replace #{@odba_class}:#{@odba_id}"
				end
			end
		end
		# A stub always references a Persistable that has already been saved.
		def odba_unsaved?(snapshot_level=nil)
			false
		end
		#added odba_id because it is now definded in object
		no_override = [
			"odba_id", "is_a?", "__id__", "__send__", "inspect", "hash", "eql?", "nil?",
		]
		override_methods = Object.public_methods - no_override
		override_methods.each { |method|
			eval <<-EOS
				def #{method}(*args)
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
class Array
	alias :odba_amp :&
	def &(stub)
		self.odba_amp(stub.odba_instance)
	end
	alias :odba_plus :+
	def +(stub)
		self.odba_plus(stub.odba_instance)
	end
	alias :odba_minus :-
	def -(stub)
		self.odba_minus(stub.odba_instance)
	end
	alias :odba_weight :<=>
	def <=>(stub)
		self.odba_weight(stub.odba_instance)
	end
	alias :odba_equal? :==
	def ==(stub)
		self.odba_equal?(stub.odba_instance)
	end
	alias :odba_union :|
	def |(stub)
		self.odba_union(stub.odba_instance)
	end
	['concat', 'replace'].each { |method|
		eval <<-EOS
			alias :odba_#{method} :#{method}
			def #{method}(stub)
				self.odba_#{method}(stub.odba_instance)
			end
		EOS
	}
end
class Hash
	alias :odba_equal? :==
	def ==(stub)
		self.odba_equal?(stub.odba_instance)
	end
	['merge', 'merge!', 'replace'].each { |method|
		eval <<-EOS
			alias :odba_#{method} :#{method}
			def #{method}(stub)
				self.odba_#{method}(stub.odba_instance)
			end
		EOS
	}
end
