#!/usr/bin/env ruby
# TestFlatFileStorage -- odba -- 12.08.2004 -- hwyss@ywesee.com

$: << File.expand_path('../lib', File.dirname(__FILE__))

require 'test/unit'
require 'odba/flatfile_storage'

module ODBA
	class TestFlatFileStorage < Test::Unit::TestCase
		def setup
			@datadir = File.expand_path('data/flatfiles', 
				File.dirname(__FILE__))
			@storage = FlatFileStorage.new(@datadir)
		end
		def teardown
			FileUtils.rm_rf(@datadir)
		end
		def test_add_object_connection
			@storage.add_object_connection(1, 3)
			@storage.flush
			assert(File.exist?(@datadir), "datadir was not created")
			datafile = File.expand_path('object_connection.csv', @datadir)
			assert(File.exist?(datafile), "datafile was not created")
			expected = <<-EOF
1	3
			EOF
			assert_equal(expected, File.read(datafile))
			@storage.add_object_connection(3, 4)
			@storage.flush
			expected = <<-EOF
1	3
3	4
			EOF
			assert_equal(expected, File.read(datafile))
		end
		def test_store
			@storage.store(1,"foodump", "foo", true)
			@storage.flush
			assert(File.exist?(@datadir), "datadir was not created")
			datafile = File.expand_path('object.csv', @datadir)
			assert(File.exist?(datafile), "datafile was not created")
			expected = <<-EOF
1	foodump	foo	true
			EOF
			assert_equal(expected, File.read(datafile))
			@storage.store(2,"bardump", nil, false)
			@storage.flush
			expected = <<-EOF
1	foodump	foo	true
2	bardump	\\N	false
			EOF
			assert_equal(expected, File.read(datafile))
		end
		def test_next_id
			id = nil
			assert_nothing_raised {
				id = @storage.next_id
			}
			assert_equal(1, id)
			assert_equal(2, @storage.next_id)
			assert_equal(3, @storage.next_id)
		end
	end
end
