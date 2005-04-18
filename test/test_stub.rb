
#!/usr/bin/env ruby

$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'odba'
require 'test/unit'
require 'mock'

module ODBA
	class Stub
		attr_accessor :receiver, :odba_class
	end
	class MockReceiver < Mock
		ODBA_CACHE_METHODS = []
		def taint
			super
		end
	end
	class TestStub < Test::Unit::TestCase
		def setup
			@odba_container = Mock.new("odba_container")
			ODBA.cache_server = Mock.new("cache")
			@receiver = MockReceiver.new("receiver")
			@stub = Stub.new(9, @odba_container, @receiver)
		end
		def test_method_missing
			receiver = Mock.new
			ODBA.cache_server.__next(:fetch) { |id, caller|
				assert_equal(9, id)
				receiver
			}
			receiver.__next(:foo_method){ |number|
				assert_equal(3, number)
			}
			@odba_container.__next(:odba_replace_stubs) { |stub, rec| 
				assert_equal(receiver, rec)
			}
			@stub.foo_method(3)
			receiver.__verify
			@odba_container.__verify
		end
		def test_method_missing_receiver_nil
			@stub.receiver = nil
			cache = ODBA.cache_server	
			receiver = Mock.new
			cache.__next(:fetch) { |odba_id, odba_container|
				receiver
			}
			receiver.__next(:foo_method) { |number|
				assert_equal(3, number)
			}
			@odba_container.__next(:odba_replace_stubs) { |stub,receiver| }
			assert_nothing_raised { @stub.foo_method(3) }
			@odba_container.__verify
			cache.__verify
		end
		def test_method_missing__odba_class_nil # backward-compatibility
			@stub.odba_class = nil
			receiver = Mock.new
			ODBA.cache_server.__next(:fetch) { |odba_id, odba_container|
				receiver
			}
			receiver.__next(:foo_method) { |number|
				assert_equal(3, number)
			}
			@odba_container.__next(:odba_replace_stubs) { |stub, receiver| }
			assert_nothing_raised { @stub.foo_method(3) }
			@odba_container.__verify
			ODBA.cache_server.__verify
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
			@stub = Stub.new(1, @odba_container, @receiver)
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
			assert_equal(MockReceiver, @stub.class)
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
		def test_array_methods
			stub = Stub.new(9, [], [])
			ODBA.cache_server.__next(:fetch) { |odba_id, container| [] }
			assert_equal([], stub)
			stub = Stub.new(9, [], [])
			ODBA.cache_server.__next(:fetch) { |odba_id, container| [] }
			assert([] == stub)
			[
				"&", "+", "-", "<=>", "==", 
				"concat", "equal?", "replace", "|"
			].each { |method|
				ODBA.cache_server.__next(:fetch) { |odba_id, container| [] }
				stub = Stub.new(9, [], [])
				assert_nothing_raised("failed method: #{method}") {
					[].send(method, stub)
				}
			}
		end
		def test_hash_methods
			stub = Stub.new(9, [], {})
			ODBA.cache_server.__next(:fetch) { |odba_id, container| {} }
			assert_equal({}, stub)
			stub = Stub.new(9, [], {})
			ODBA.cache_server.__next(:fetch) { |odba_id, container| {} }
			assert({} == stub)
			[
				"merge", "merge!", "replace",
			].each { |method|
				ODBA.cache_server.__next(:fetch) { |odba_id, container| {} }
				stub = Stub.new(9, [], {})
				assert_nothing_raised("failed method: #{method}") {
					{}.send(method, stub)
				}
			}
		end
		def test_hash__fetch
			stub = Stub.new(9, [], {})
			ODBA.cache_server.__next(:fetch_collection_element) { |odba_id, key| 
				assert_equal(9, odba_id)
				assert_equal('bar', key)
				'foo'
			}
			assert_nothing_raised { 
				assert_equal('foo', stub['bar'])
			}
		end
		def test_hash__fetch__2
			stub = Stub.new(9, [], {})
			ODBA.cache_server.__next(:fetch_collection_element) { |odba_id, key| 
				assert_equal(9, odba_id)
				assert_equal('bar', key)
				nil
			}
			ODBA.cache_server.__next(:fetch) { |odba_id, caller|
				assert_equal(9, odba_id)
				assert_equal([], caller)
				{'bar' => 'foo'}
			}
			assert_nothing_raised { 
				assert_equal('foo', stub['bar'])
			}
		end
	end
end
