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
			ODBA.cache_server = Mock.new("cache_server")
			ODBA.storage.__next(:create_index) { |name| }
		end
		def test_fill_array
			foo = Mock.new("foo")
			index_definition = Mock.new("index_definition")
			foo_origin = Mock.new("foo_origin")
			baz_origin = Mock.new("baz_origin")
			bar_origin = Mock.new("bar_origin")
			bar = Mock.new("bar")
			baz = Mock.new("baz")
			targets = [foo, [baz, bar]]
			index_definition.__next(:origin_klass){:Index}	
			index_definition.__next(:target_klass){:Index}	
			index_definition.__next(:resolve_origin){"origin"}	
			index_definition.__next(:resolve_target){"target"}	
			index_definition.__next(:index_name){"test_index"}	
			index_definition.__next(:resolve_search_term){"res_s"}	
			index_definition.__next(:index_name){"test_index"}	
			index = Index.new(index_definition, ODBA)
			
			foo.__next(:odba_id) { || 3}
			foo.__next(:origin){foo_origin}

			
			baz.__next(:odba_id) { || 4}
			baz.__next(:origin){baz_origin}
		

			bar.__next(:odba_id) { || 5}
			bar.__next(:origin){bar_origin}
			
			ODBA.cache_server.__next(:bulk_fetch){}
			foo_origin.__next(:odba_id){45}
			foo_origin.__next(:res_s){"result"}
			foo_origin.__next(:res_s){"result"}
		

			ODBA.cache_server.__next(:bulk_fetch){}
			baz_origin.__next(:odba_id){47}
			baz_origin.__next(:res_s){"result"}
			baz_origin.__next(:res_s){"result"}
		

			ODBA.cache_server.__next(:bulk_fetch){}
			bar_origin.__next(:odba_id){48}
			bar_origin.__next(:res_s){"result"}
			bar_origin.__next(:res_s){"result"}
			
			ODBA.storage.__next(:update_index) { |name, orid, term, tarid|  }
			ODBA.storage.__next(:update_index) { |name, orid, term, tarid|  }
			ODBA.storage.__next(:update_index) { |name, orid, term, tarid|  }
			index.fill(targets)
			foo.__verify
			baz.__verify
			ODBA.storage.__verify
		end
		def test_proc
			index_definition = Mock.new("index_definition")
			proc_code = <<-EOS
				Proc.new { |target|
					target.proc_method
				}
			EOS
			target = Mock.new
			bar = Mock.new
			index_definition.__next(:origin_klass){:Index}	
			index_definition.__next(:target_klass){:Index}	
			index_definition.__next(:resolve_origin){"origin"}	
			index_definition.__next(:resolve_target){"target"}	
			index_definition.__next(:index_name){"test_index"}	
			index_definition.__next(:resolve_search_term){"res_s"}	
			index_definition.__next(:index_name){"test_index"}	
			index = Index.new(index_definition, ODBA)
			result = index.proc_instance_origin
			assert_instance_of(Proc, result)
			target.__verify
			bar.__verify
		end
		def test_origin_class
			foo = Mock.new("foo")
			index_definition = Mock.new("index_definition")
			index_definition.__next(:origin_klass){:Index}	
			index_definition.__next(:target_klass){:Index}	
			index_definition.__next(:resolve_origin){"origin"}	
			index_definition.__next(:resolve_target){"target"}	
			index_definition.__next(:index_name){"test_index"}	
			index_definition.__next(:resolve_search_term){"res_s"}	
			index_definition.__next(:index_name){"test_index"}	
		
			index = Index.new(index_definition, ODBA)
			result = index.origin_class?(Index)
			assert_equal(true, result)
			foo.__verify
		end
		def test_search_term
			foo = Mock.new("foo")
			index_definition = Mock.new("index_definition")
			index_definition.__next(:origin_klass){:Index}	
			index_definition.__next(:target_klass){:Index}	
			index_definition.__next(:resolve_origin){"origin"}	
			index_definition.__next(:resolve_target){"target"}	
			index_definition.__next(:index_name){"test_index"}	
			index_definition.__next(:resolve_search_term){"res_s"}	
			index_definition.__next(:index_name){"test_index"}	
			index = Index.new(index_definition, ODBA)
			foo.__next(:res_s){"result"}
			foo.__next(:res_s){"result"}
			result = index.search_term(foo)
			assert_equal(result, "result")
			foo.__verify
		end
		def test_resolve_tagets
			index_definition = Mock.new("index_definition")
			index_definition.__next(:origin_klass){:Index}	
			index_definition.__next(:target_klass){:Index}	
			index_definition.__next(:resolve_origin){"origin"}	
			index_definition.__next(:resolve_target){"target"}	
			index_definition.__next(:index_name){"test_index"}	
			index_definition.__next(:resolve_search_term){"res_s"}	
			index_definition.__next(:index_name){"test_index"}	
			mock = Mock.new("mock")
			bar = Mock.new("target")
			index = Index.new(index_definition, ODBA)
			mock.__next(:target){
				bar
			}
			ODBA.cache_server.__next(:bulk_fetch){|ids, obj|}
			result = index.resolve_targets(mock)
			assert_equal([bar], result)
			assert_equal(nil, bar.__verify)
		end
		def test_update_target
			index_definition = Mock.new("index_definition")
			index_definition.__next(:origin_klass){:Index}	
			index_definition.__next(:target_klass){:Index}	
			index_definition.__next(:resolve_origin){"origin"}	
			index_definition.__next(:resolve_target){"target"}	
			index_definition.__next(:index_name){"test_index"}	
			index_definition.__next(:resolve_search_term){"res_s"}	
			index_definition.__next(:index_name){"test_index"}	
			foo = Mock.new("foo")
			index = Index.new(index_definition, ODBA)
			ODBA.storage.__next(:index_delete_target) { |name, id| 
				assert_equal(1, id)
				assert_equal("test_index", name)
			}
			ODBA.storage.__next(:update_index) { |name, ogid, term, tarid | }	
			ODBA.cache_server.__next(:bulk_fetch) { |ids, caller| }
			index.update(foo)
			assert_equal(nil, foo.__verify)
		end
		def test_update_origin
			index_definition = Mock.new("index_definition")
			index_definition.__next(:origin_klass){:Index}	
			index_definition.__next(:target_klass){:Index}	
			index_definition.__next(:resolve_origin){"origin"}	
			index_definition.__next(:resolve_target){"target"}	
			index_definition.__next(:index_name){"test_index"}	
			index_definition.__next(:resolve_search_term){"res_s"}	
			index_definition.__next(:index_name){"test_index"}	
			foo  = Mock.new("foo")
			target = Mock.new("target")
			index = Index.new(index_definition, ODBA)
			ODBA.cache_server.__next(:bulk_fetch){|ids, obj|}
			index.update(foo)
			assert_equal(nil, foo.__verify)
			assert_equal(nil, ODBA.storage.__verify)
			assert_equal(nil, target.__verify)
		end
	end
end
