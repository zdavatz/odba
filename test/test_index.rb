#!/usr/bin/env ruby
# -- oddb -- 13.05.2004 -- mwalder@ywesee.com

$: << File.dirname(__FILE__)
$: << File.expand_path("../src", File.dirname(__FILE__))

require 'test/unit'
require 'odba'
require 'mock'

module ODBA
	class TestIndex < Test::Unit::TestCase
		def setup
			ODBA.storage = Mock.new("storage")
			ODBA.cache_server = Mock.new("cacher_server")
			ODBA.storage.__next(:create_index) { |name| }
		end
		def test_fill_array
			foo = Mock.new("foo")
			bar = Mock.new("bar")
			baz = Mock.new("baz")
			targets = [foo, [baz, bar]]
			index = Index.new("index", Mock, "baz", :foo_method, "foo")
			
			foo.__next(:odba_id) { || 3}
			foo.__next(:odba_id) { || 3}

			ODBA.storage.__next(:update_index) { |name, orid, term, tarid|  }
			ODBA.storage.__next(:update_index) { |name, orid, term, tarid|  }
			ODBA.storage.__next(:update_index) { |name, orid, term, tarid|  }
			
			bar.__next(:odba_id) { || 5 }
			bar.__next(:odba_id) { || 5 }
			baz.__next(:odba_id) { || 5 }
			baz.__next(:odba_id) { || 5 }
	
			index.fill(targets)

			foo.__verify
			baz.__verify
			ODBA.storage.__verify
		end
		def test_proc
			proc_code = <<-EOS
				Proc.new { |target|
					target.proc_method
				}
			EOS
			target = Mock.new
			bar = Mock.new
			index = Index.new("foo", "baz", "bar", proc_code, "foo")
			result = index.proc_instance_origin
			assert_instance_of(Proc, result)
			target.__verify
			bar.__verify
		end
		def test_origin_class
			foo = Mock.new("foo")
			index = Index.new("index_name", Mock, "baz", :foo_method, "foo")
			result = index.origin_class?(Mock)
			assert_equal(true, result)
			foo.__verify
		end
		def test_search_term
			foo = Mock.new("foo")
			index = Index.new("index_name", "bar", "baz", :foo_method, "foobar")
			result = index.search_term(foo)
			assert_equal(result, foo.to_s)
			foo.__verify
		end
		def test_resolve_target_id
			mock = Mock.new("mock")
			bar = Mock.new("target")
			index = Index.new("index", Mock, "baz", :foo_method, :target_method)
			mock.__next(:target_method){
				bar
			}
			result = index.resolve_targets(mock)
			assert_equal([bar], result)
			assert_equal(nil, bar.__verify)
		end
		def test_update_target
			foo = Mock.new("foo")
			index = Index.new("foo_index", Hash, Mock, :foo_method, :foo)
			foo.__next(:odba_id) { 1 }
			ODBA.storage.__next(:delete_target_ids) { |name, id| 
				assert_equal(1, id)
				assert_equal("foo_index", name)
			}
			ODBA.storage.__next(:update_index) { |name, ogid, term, tarid | }	
			foo.__next(:odba_id){ 1}
			foo.__next(:odba_id){ 1}
			ODBA.cache_server.__next(:bulk_fetch) { |ids, caller| }
			index.update(foo)
			assert_equal(nil, foo.__verify)
		end
		def test_update_origin
			foo  = Mock.new("foo")
			target = Mock.new("target")
			index = Index.new("foo_index", Mock, Hash, :foo_method, :resolve_target)
			target.__next(:odba_id) { 1 }
			foo.__next(:odba_id) {2}
			foo.__next(:resolve_target){
				target
			}
			ODBA.storage.__next(:update_index){|index_name, orig_id,search, tar_id|
			}
			index.update(foo)
			assert_equal(nil, foo.__verify)
			assert_equal(nil, ODBA.storage.__verify)
			assert_equal(nil, target.__verify)
		end
	end
end
