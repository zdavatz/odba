#!/usr/bin/env ruby
# encoding: utf-8
# ODBA::TestStub -- odba -- 08.12.2011 -- mhatakeyama@ywesee.com

$: << File.expand_path('../lib/', File.dirname(__FILE__))
$: << File.dirname(__FILE__)

require 'odba/stub'
require 'odba/persistable'
require 'odba/odba'
require 'test/unit'
require 'flexmock'
require 'yaml'

module ODBA
	class Stub
		attr_accessor :receiver, :odba_class
	end
	class TestStub < Test::Unit::TestCase
    include FlexMock::TestCase
		def setup
			@odba_container = flexmock("odba_container")
			@cache = ODBA.cache = flexmock("cache")
			@receiver = flexmock("receiver")
			@stub = Stub.new(9, @odba_container, @receiver)
		end
		def test_method_missing
			receiver = flexmock
			@cache.mock_handle(:fetch) { |id, caller|
				assert_equal(9, id)
				receiver
			}
			receiver.mock_handle(:foo_method){ |number|
				assert_equal(3, number)
			}
			@odba_container.mock_handle(:odba_replace_stubs) { |id, rec| 
				assert_equal(receiver, rec)
			}
			@stub.foo_method(3)
			receiver.mock_verify
			@odba_container.mock_verify
		end
		def test_method_missing_receiver_nil
			@stub.receiver = nil
			cache = ODBA.cache	
			receiver = flexmock
			cache.mock_handle(:fetch) { |odba_id, odba_container|
				receiver
			}
			receiver.mock_handle(:foo_method) { |number|
				assert_equal(3, number)
			}
			@odba_container.mock_handle(:odba_replace_stubs) { |id,receiver| }
			assert_nothing_raised { @stub.foo_method(3) }
			@odba_container.mock_verify
			cache.mock_verify
		end
		def test_method_missing__odba_class_nil # backward-compatibility
			@stub.odba_class = nil
			receiver = flexmock
			@cache.mock_handle(:fetch) { |odba_id, odba_container|
				receiver
			}
			receiver.mock_handle(:foo_method) { |number|
				assert_equal(3, number)
			}
			@odba_container.mock_handle(:odba_replace_stubs) { |id, receiver| }
			assert_nothing_raised { @stub.foo_method(3) }
			@odba_container.mock_verify
			ODBA.cache.mock_verify
		end
		def test_odba_receiver
			@cache.should_receive(:fetch).with(9, @odba_container)\
        .and_return('odba_instance')
			@odba_container.should_receive(:odba_replace_stubs)\
        .with(@stub.odba_id, 'odba_instance').and_return { assert(true) }
			@stub.odba_receiver
		end
		def test_send_instance_methods
      receiver = 'odba_instance'
			@odba_container.should_ignore_missing
			@cache.mock_handle(:fetch).with(9, @odba_container)\
        .and_return(receiver)
			@stub.taint
			assert_equal(true, receiver.tainted?)
		end
		def test_instance_method_not_sent
			assert_equal(true, @stub.is_a?(Persistable))
		end
		def test_send_class
			receiver = flexmock
			@odba_container.mock_handle(:odba_replace_stubs) { |id, rec|}
			@cache.mock_handle(:fetch) { |id,container|
				receiver
			}
			assert_equal(FlexMock, @stub.class)
		end
		def test_respond_to
			receiver = flexmock('receiver')
			@odba_container.mock_handle(:odba_replace_stubs) { |id, rec|}
			@cache.mock_handle(:fetch) { |id,container|
				receiver
			}
			receiver.mock_verify
			assert_equal(false, @stub.respond_to?(:odba_replace))
		end
		def test_array_methods
			stub = Stub.new(9, [], [])
			@cache.mock_handle(:fetch) { |odba_id, container| [] }
			assert_equal([], stub)
			stub = Stub.new(9, [], [])
			@cache.mock_handle(:fetch) { |odba_id, container| [] }
			assert([] == stub)
			[
				"&", "+", "-", "<=>", "==", 
				"concat", "equal?", "replace", "|"
			].each { |method|
				@cache.mock_handle(:fetch) { |odba_id, container| [] }
				stub = Stub.new(9, [], [])
				assert_nothing_raised("failed method: #{method}") {
					[].send(method, stub)
				}
			}
		end
		def test_hash_methods
			stub = Stub.new(9, [], {})
			@cache.mock_handle(:fetch) { |odba_id, container| {} }
			assert_equal({}, stub)
			stub = Stub.new(9, [], {})
			@cache.mock_handle(:fetch) { |odba_id, container| {} }
			assert({} == stub)
			[
				"merge", "merge!", "replace",
			].each { |method|
				@cache.mock_handle(:fetch) { |odba_id, container| {} }
				stub = Stub.new(9, [], {})
				assert_nothing_raised("failed method: #{method}") {
					{}.send(method, stub)
				}
			}
		end
		def test_hash__fetch
			stub = Stub.new(9, [], {})
			@cache.mock_handle(:include?) { |odba_id|
				assert_equal(9, odba_id)
				false
			}
			@cache.mock_handle(:fetch_collection_element) { |odba_id, key| 
				assert_equal(9, odba_id)
				assert_equal('bar', key)
				'foo'
			}
			assert_equal('foo', stub['bar'])
		end
		def test_hash__fetch__2
			stub = Stub.new(9, [], {})
			@cache.mock_handle(:include?) { |odba_id|
				assert_equal(9, odba_id)
				false
			}
			@cache.mock_handle(:fetch_collection_element) { |odba_id, key| 
				assert_equal(9, odba_id)
				assert_equal('bar', key)
				nil
			}
			@cache.mock_handle(:fetch) { |odba_id, caller|
				assert_equal(9, odba_id)
				assert_equal([], caller)
				{'bar' => 'foo'}
			}
			assert_equal('foo', stub['bar'])
		end
		def test_hash__fetch__already_in_cache
			stub = Stub.new(9, [], {})
			@cache.mock_handle(:include?) { |odba_id|
				assert_equal(9, odba_id)
				true 
			}
			@cache.mock_handle(:fetch) { |odba_id, fetcher| 
				assert_equal(9, odba_id)
				{'bar' => 'foo'}
			}
			assert_equal('foo', stub['bar'])
		end
		def test_hash_key__1
			stub = Stub.new(9, nil, nil)
			@cache.should_receive(:fetch).with(9, nil).and_return(@receiver)
			@cache.should_receive(:fetch).with(9, @odba_container)\
        .and_return(@receiver)
			@cache.should_receive(:fetch).with(8, nil).and_return('other')
			@odba_container.should_ignore_missing
			hash = {stub => 'success'}
			assert_equal('success', hash[@stub])
			other = Stub.new(8, nil, nil)
			assert_nil(hash[other])
		end
		def test_to_yaml
      flexmock(@cache, :fetch => nil)
			yaml = ''
			assert_nothing_raised {
				yaml = @stub.odba_isolated_stub.to_yaml
			}
			loaded = YAML.load(yaml)
			assert(loaded.is_a?(Stub))
			assert_equal(9, loaded.odba_id)
		end
    def test_odba_clear_receiver
      @stub.instance_variable_set('@receiver', flexmock)
      @stub.odba_clear_receiver
      assert_nil(@stub.instance_variable_get('@receiver'))
    end
    def test_odba_unsaved
      assert_equal(false, @stub.odba_unsaved?)
    end
		def test_hash_key__2
      receiver = Object.new
      receiver.extend(Persistable)
      receiver.instance_variable_set('@odba_id', 9)
      stub = Stub.new(9, nil, nil)
			@cache.mock_handle(:fetch) { |odba_id, caller|
				assert_equal(9, odba_id)
				receiver
			}
			hash = {stub => 'success'}
			assert_equal('success', hash[stub])
			assert_equal('success', hash[receiver])
		end
	end
end
