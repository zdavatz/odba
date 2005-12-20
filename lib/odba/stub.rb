#!/usr/bin/env ruby
#-- Stub -- odba -- 29.04.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

module ODBA
	class Stub # :nodoc: all
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
		def inspect
			"#<ODBA::Stub:#{object_id}##@odba_id @odba_class=#@odba_class @odba_container=#{@odba_container.object_id}>"
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
				@receiver = ODBA.cache.fetch(@odba_id, @odba_container)
				if(@odba_container)
					@odba_container.odba_replace_stubs(self, @receiver)
				end
				@receiver
			rescue OdbaError => e
				#require 'debug'
				warn "ODBA::Stub was unable to replace #{@odba_class}:#{@odba_id}"
				warn e.backtrace.join("\n")
			end
		end
		# A stub always references a Persistable that has 
		# already been saved.
		def odba_unsaved?(snapshot_level=nil)
			false
		end
		no_override = [
			"class", "dup", "is_a?", "__id__", "__send__", "inspect", 
			"eql?", "nil?", "respond_to?", "object_id", 
			"instance_variables", "instance_variable_get",
			"instance_variable_set",
			## methods defined in persistable.rb:Object
			"odba_id", "odba_instance", "odba_isolated_stub"
		]
		override_methods = Object.public_methods - no_override
		override_methods.each { |method|
			src = (method[-1] == ?=) ? <<-EOW : <<-EOS
				def #{method}(args)
					odba_instance.#{method}(args)
				end
			EOW
				def #{method}(*args)
					odba_instance.#{method}(*args)
				end
			EOS
			eval src
		}
		def respond_to?(msg_id)
			case msg_id
			when :_dump, :marshal_dump
				false
			else
				odba_instance.respond_to?(msg_id)
			end
		end
		## FIXME
		#  implement full hash/array access - separate collection stub?
		def [](*args, &block)
			if(@odba_class == Hash \
				&& !ODBA.cache.include?(@odba_id))
				ODBA.cache.fetch_collection_element(@odba_id, args.first)
			end || method_missing(:[], *args, &block)
		end
	end
end
class Array # :nodoc: all
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
class Hash # :nodoc: all
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
