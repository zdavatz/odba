#!/usr/bin/env ruby
# Stub -- odba -- 29.04.2004 -- rwaltert@ywesee.com mwalder@ywesee.com

require 'yaml'

module ODBA
	class Stub
		attr_accessor :odba_id, :odba_container
		def initialize(odba_id, odba_container, receiver)
			@odba_id = odba_id
			@odba_container = odba_container
			@odba_class = receiver.class unless receiver.nil? 
		end
		def class
			@odba_class || odba_instance.class
		end
		def eql?(other)
			@odba_id == other.odba_id ## defined in Object
		end
		def hash
			@odba_id.to_i
		end
		def is_a?(klass)
			[Stub, Persistable, @odba_class].include?(klass) \
				|| odba_instance.is_a?(klass)
		end
		def method_missing(meth_symbol, *args, &block)
			odba_instance.send(meth_symbol, *args, &block)
		end
		def odba_clear_receiver
			@receiver = nil
		end
		def odba_instance
			odba_replace
		end
		def odba_isolated_stub
			stub = dup
			stub.odba_container = nil
			stub
		end
		def odba_replace(name=nil)
			@receiver || begin
				@receiver = ODBA.cache_server.fetch(@odba_id, @odba_container)
				if(@odba_container)
					@odba_container.odba_replace_stubs(self, @receiver)
				end
				@receiver
			rescue OdbaError => e
				#require 'debug'
				warn "ODBA::Stub was unable to replace #{@odba_class}:#{@odba_id}"
			end
		end
		# A stub always references a Persistable that has 
		# already been saved.
		def odba_unsaved?(snapshot_level=nil)
			false
		end
		no_override = [
			"class", "dup", "is_a?", "__id__", "__send__", "inspect", "hash",
			"eql?", "nil?", "respond_to?", 
			## methods defined in persistable.rb:Object
			"odba_id", "odba_instance", "odba_isolated_stub"
		]
		override_methods = Object.public_methods - no_override
		override_methods.each { |method|
			eval <<-EOS
				def #{method}(*args)
					odba_instance.#{method}(*args)
				end
			EOS
		}
		def respond_to?(meth)
			if([:marshal_dump, :_dump].include?(meth))
				super
			else
				odba_instance.respond_to?(meth)
			end
		end
		## FIXME
		#  implement full hash/array access - separate collection stub?
		def [](*args, &block)
			if(@odba_class == Hash \
				&& !ODBA.cache_server.include?(@odba_id))
				ODBA.cache_server.fetch_collection_element(@odba_id, args.first)
			end || method_missing(:[], *args, &block)
		end
	end
end
class Array
	alias :_odba_amp :&
	def &(stub)
		self._odba_amp(stub.odba_instance)
	end
	alias :_odba_plus :+
	def +(stub)
		self._odba_plus(stub.odba_instance)
	end
	alias :_odba_minus :-
	def -(stub)
		self._odba_minus(stub.odba_instance)
	end
	alias :_odba_weight :<=>
	def <=>(stub)
		self._odba_weight(stub.odba_instance)
	end
	alias :_odba_equal? :==
	def ==(stub)
		self._odba_equal?(stub.odba_instance)
	end
	alias :_odba_union :|
	def |(stub)
		self._odba_union(stub.odba_instance)
	end
	['concat', 'replace', 'include?'].each { |method|
		eval <<-EOS
			alias :_odba_#{method} :#{method}
			def #{method}(stub)
				self._odba_#{method}(stub.odba_instance)
			end
		EOS
	}
end
class Hash
	alias :_odba_equal? :==
	def ==(stub)
		self._odba_equal?(stub.odba_instance)
	end
	['merge', 'merge!', 'replace'].each { |method|
		eval <<-EOS
			alias :_odba_#{method} :#{method}
			def #{method}(stub)
				self._odba_#{method}(stub.odba_instance)
			end
		EOS
	}
end
