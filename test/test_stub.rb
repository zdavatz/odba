
#!/usr/bin/env ruby

$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'odba'
require 'test/unit'
require 'mock'

module ODBA
	class Stub
		attr_accessor :receiver
	end
	class MockReceiver < Mock
		def taint
			super
		end
	end
	class TestStub < Test::Unit::TestCase
		def setup
			#Stub.delegate_object_methods
			@odba_container = Mock.new
			ODBA.cache_server = Mock.new
			@stub = Stub.new(9, @odba_container)
		end
		def test_method_missing
			receiver = Mock.new
			@stub.receiver = receiver	
			receiver.__next(:foo_method){ |number|
				assert_equal(3, number)
			}
			@stub.foo_method(3)
			receiver.__verify
			@odba_container.__verify
		end
		def test_method_missing_receiver_nil
			@stub.receiver = nil
			cache = ODBA.cache_server	
			receiver = Mock.new
			cache.__next(:fetch){|odba_id, odba_container|
				receiver
			}
			@odba_container.__next(:odba_replace_stubs){ |stub,receiver|}
			@stub.odba_replace
			@odba_container.__verify
			@stub.receiver.__next(:foo_method){ |number|
				assert_equal(3, number)
			}
			@stub.foo_method(3)
			cache.__verify
		end
		def test_odba_replace
			cache = ODBA.cache_server 
			cache.__next(:fetch) { |odba_id, odba_container| }
			@odba_container.__next(:odba_replace_stubs) { |stub,receiver|}
			@stub.odba_replace
			@odba_container.__verify
			cache.__verify
		end
		def test_send_instance_methods
			receiver = MockReceiver.new
			@odba_container.__next(:odba_replace_stubs) { |obj, rec|}
			ODBA.cache_server.__next(:fetch) { |id,container|
				receiver
			}
			receiver.__next(:taint) {}
			@stub = Stub.new(1, @odba_container)
			@stub.taint
			@odba_container.__verify
			ODBA.cache_server.__verify
			receiver.__verify
			assert_equal(true, receiver.tainted?)
		end
		def test_instance_method_not_sent
			assert_equal(true, @stub.is_a?(Persistable))
		end
		def test_send_class
			receiver = Mock.new
			@odba_container.__next(:odba_replace_stubs) { |obj, rec|}
			ODBA.cache_server.__next(:fetch) { |id,container|
				receiver
			}
			assert_equal(Mock, @stub.class)
		end
		def test_respond_to
			receiver = Mock.new
			@odba_container.__next(:odba_replace_stubs) { |obj, rec|}
			ODBA.cache_server.__next(:fetch) { |id,container|
				receiver
			}
			receiver.__verify
			assert_equal(false, @stub.respond_to?(:odba_replace))
		end
	end
end
