#!/usr/bin/env ruby
# -- oddb -- 13.05.2004 -- mwalder@ywesee.com

$: << File.dirname(__FILE__)
$: << File.expand_path("../lib", File.dirname(__FILE__))

require 'test/unit'
require 'odba'
require 'mock'

module ODBA
	class TestIndex < Test::Unit::TestCase
		def test_fill_array
			foo = Mock.new("foo")
			bar = Mock.new("bar")
			baz = Mock.new("baz")
			ODBA.storage = Mock.new("storage")
			targets = [foo, [baz, bar]]
			index = Index.new(Mock, :foo_method)
			
			foo.__next(:odba_id) { || 3}
			#	foo.__next(:foo_method) { || }	
			foo.__next(:odba_id) { || 3}

			ODBA.storage.__next(:next_id) { || 7 }
			baz.__next(:odba_id) { || 4 }
			#baz.__next(:foo_method) { ||}	
			
			bar.__next(:odba_id) { || 5 }
			
			result = index.fill(targets)
			expected = [[3, foo.to_s, 3], [4, baz.to_s, 7],[5, bar.to_s, 7]]
			assert_equal(expected, result) 

			foo.__verify
			baz.__verify
			ODBA.storage.__verify
		end
		def test_fill_array2
			foo = Mock.new("foo")
			bar = Mock.new("bar")
			baz = Mock.new("baz")
			ODBA.storage = Mock.new("storage")
			targets = [foo, baz, bar]
			index = Index.new(Mock, :foo_method)
			
			foo.__next(:odba_id) { || 3}
			#	foo.__next(:foo_method) { || }	
			foo.__next(:odba_id) { || 3}

			baz.__next(:odba_id) { || 4 }
			baz.__next(:odba_id) { || 4 }
			#baz.__next(:foo_method) { ||}	
			
			bar.__next(:odba_id) { || 5 }
			bar.__next(:odba_id) { || 5 }
			
			result = index.fill(targets)
			expected = [[3, foo.to_s, 3], [4, baz.to_s, 4],[5, bar.to_s, 5]]
			assert_equal(expected, result) 

			foo.__verify
			baz.__verify
		end
		def test_proc
			proc_code = <<-EOS
				Proc.new { |target|
					target.proc_method
				}
			EOS
			target = Mock.new
			bar = Mock.new
			index = Index.new("foo", proc_code)
			result = index.proc_instance
			assert_instance_of(Proc, result)
			target.__verify
			bar.__verify
		end
		def test_origin_class
			foo = Mock.new("foo")
			index = Index.new(Mock, :foo_method)
			result = index.origin_class?(Mock)
			assert_equal(true, result)
			foo.__verify
		end
		def test_search_term
			foo = Mock.new("foo")
			index = Index.new(Mock, :foo_method)
			foo.__next(:foo_method) { "foobar" }
			result = index.search_term(foo)
			assert_equal(result, "foobar")
			foo.__verify
		end
	end
end
