#!/usr/bin/env ruby
# TestStorage -- odba -- 10.05.2004 -- rwaltert@ywesee.com mwalder@ywesee.com

$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'odba'
require 'test/unit'
require 'mock'

module ODBA
	class Storage
		public :restore_max_id
		attr_writer :next_id
	end
	class TestStorage < Test::Unit::TestCase
		def setup
			@storage = ODBA::Storage.instance
		end
		def test_bulk_restore
			dbi = Mock.new("dbi")
			array = [1, 23, 4]
			@storage.dbi = dbi
			dbi.__next(:select_all) { |query|
				assert_not_nil(query.index('IN (1,23,4)'))
				[]
			}
			@storage.bulk_restore(array)
			dbi.__verify
		end
		def test_delete_persistable
			dbi = Mock.new("dbi")
			sth = Mock.new
			@storage.dbi = dbi
			expected1 = <<-SQL
				DELETE FROM object WHERE odba_id = ?
			SQL
			dbi.__next(:prepare) { |sql|
				assert_equal(expected1, sql)
				sth
			}
			sth.__next(:execute) { |id|
				assert_equal(2, id)
			}
			expected2 = <<-SQL
				DELETE FROM object_connection WHERE ? IN (origin_id, target_id)
			SQL
			dbi.__next(:prepare) { |sql|
				assert_equal(expected2, sql)
				sth
			}
			sth.__next(:execute) { |id|
				assert_equal(2, id)
			}
			@storage.delete_persistable(2)
			dbi.__verify
			sth.__verify
		end
		def test_restore_prefetchable
			dbi = Mock.new("dbi")
			rows = Mock.new("row")
			@storage.dbi = dbi
			dbi.__next(:select_all){ |sql|
				assert_equal("\t\t\t\tSELECT odba_id, content FROM object WHERE prefetchable = true\n", sql)
				rows
			}
			@storage.restore_prefetchable
			dbi.__verify
			rows.__verify
		end
		def test_bulk_restore_empty
			dbi = Mock.new("dbi")
			array = []
			@storage.dbi = dbi
			assert_nothing_raised {
				@storage.bulk_restore(array)
			}
			dbi.__verify
		end
		def test_create_index
			dbi = Mock.new
			sth = Mock.new
			sth2 = Mock.new
			sth3 = Mock.new
			@storage.dbi = dbi
			dbi.__next(:prepare){ |query| 
				assert_not_nil(query.index('TABLE sequences'))
				sth
			}
			sth.__next(:execute){  }
			dbi.__next(:prepare){ |query| 
				assert_not_nil(query.index('origin_id_sequences'))
				sth2
			}
			sth2.__next(:execute){  }
			dbi.__next(:prepare){ |query|
				assert_not_nil(query.index('CREATE INDEX search_term'))
				sth3
			}
			sth3.__next(:execute){}
			@storage.create_index("sequences")
			dbi.__verify
			sth.__verify
			sth2.__verify
			sth3.__verify
		end
		def test_next_id
			@storage.next_id = 1
			assert_equal(2, @storage.next_id)
			assert_equal(3, @storage.next_id)
		end
		def test_store
			dbi = Mock.new("dbi")
			sth = Mock.new("sth")
			@storage.dbi = dbi
			dbi.__next(:prepare) { |query| 
				assert_equal('SELECT update_object(?, ?, ?, ?)', query)
				sth
			} 
			sth.__next(:execute){ |id, dump, name, prefetch| 
				assert_equal(1, id)	
				assert_equal("foodump", dump)	
				assert_equal("foo", name)	
				assert_equal(true, prefetch)
			}
			@storage.store(1,"foodump", "foo", true)
			dbi.__verify
			sth.__verify
		end
		def test_restore
			dbi = Mock.new
			row = Mock.new
			@storage.dbi = dbi
			dbi.__next(:select_one){ |arg, name| row}
			row.__next(:first){ || }
			@storage.restore(1)
			dbi.__verify
		end
		def test_restore_named
			dbi = Mock.new
			row = Mock.new
			@storage.dbi = dbi
			dbi.__next(:select_one){ |arg, name| row}
			row.__next(:first){ || }
			@storage.restore_named("foo")
			dbi.__verify
		end
		def test_restore_max_id
			dbi = Mock.new
			row = Mock.new
			@storage.dbi = dbi
			dbi.__next(:select_one){|var|
				row
			}
			row.__next(:first) { 23 }
			row.__next(:first) { 23 }
			assert_equal(23, @storage.restore_max_id)
			row.__verify
			dbi.__verify
		end
		def test_restore_max_id__nil
			dbi = Mock.new
			row = Mock.new
			@storage.dbi = dbi
			dbi.__next(:select_one){|var|
				row
			}
			row.__next(:first){ || }
			id = nil
			assert_nothing_raised {
				id = @storage.restore_max_id
			}
			assert_equal(0, id)
			dbi.__verify
			row.__verify
		end
		def test_retrieve_named
			dbi = Mock.new("dbi")
			sth = Mock.new
			@storage.dbi = dbi
			dbi.__next(:select_all) { |sql, search|
				assert_not_nil(sql.index("SELECT DISTINCT target_id"))
				sth	
			}
			@storage.retrieve_from_index("bar","foo")
			dbi.__verify
			sth.__verify
		end
		def test_update_index
			dbi = Mock.new("dbi")
			rows = [3]
			sth_delete = Mock.new("sth_delete")
			sth_insert = Mock.new("sth_insert")
			@storage.dbi = dbi

			#insert query
			dbi.__next(:prepare){ |sql| 
				assert_not_nil(sql.index("INSERT INTO"))	
				sth_insert
			}
			sth_insert.__next(:execute) { |id, term, target_id| }

			@storage.update_index("foo", 2,"baz", "foobar")
			dbi.__verify
			sth_insert.__verify
			sth_delete.__verify
		end
		def test_delete_index_element
			dbi = Mock.new("dbi")
			sth = Mock.new
			@storage.dbi = dbi
			expected = <<-SQL
				DELETE FROM foo WHERE origin_id = ?
			SQL
			dbi.__next(:prepare) { |sql|
				assert_equal(expected, sql)
				sth
			}
			sth.__next(:execute) { |id|
				assert_equal(2, id)
			}
			@storage.delete_index_element("foo", 2)
			dbi.__verify
			sth.__verify
		end
		def test_retrieve_connected_objects
			dbi = Mock.new("dbi")
			@storage.dbi = dbi
			dbi.__next(:select_all){|sql, target_id| 
				assert_not_nil(sql.index('SELECT origin_id FROM object_connection'))
				assert_equal(target_id, 1)
			}	
			@storage.retrieve_connected_objects(1)
		end
		def test_remove_dead_connection
			dbi = Mock.new("dbi")
			sth1 = Mock.new("sth1")
			sth2 = Mock.new("sth2")
			@storage.dbi = dbi
			dbi.__next(:prepare){|sql|
				sth1
			}
			sth1.__next(:execute){}
			@storage.remove_dead_connections(0, 10)
			sth1.__verify
			sth2.__verify
			dbi.__verify
		end
		def test_remove_dead_objects
			dbi = Mock.new("dbi")
			sth1 = Mock.new("sth1")
			@storage.dbi = dbi
			dbi.__next(:prepare) { |sql|
				assert_not_nil(sql.index('DELETE FROM'))
				sth1
			}
			sth1.__next(:execute) {}
			@storage.remove_dead_objects(0, 10)
			sth1.__verify
			dbi.__verify
		end
		def test_index_delete_target
			dbi = Mock.new("dbi")
			sth = Mock.new("sth")
			@storage.dbi = dbi
			dbi.__next(:prepare){|sql|
				sth
			}
			sth.__next(:execute){|target_id|}
			@storage.index_delete_target("foo_index", 1)
			sth.__verify
			dbi.__verify
		end
		def test_drop_index
			dbi = Mock.new("dbi")
			sth = Mock.new("sth")
			@storage.dbi = dbi
			dbi.__next(:prepare){|sql| sth}
			sth.__next(:execute){}
			@storage.drop_index("foo_index")
		end
		def test_index_delete_origin
			dbi = Mock.new("dbi")
			sth = Mock.new("sth")
			@storage.dbi = dbi
			dbi.__next(:prepare){|sql| sth}
			sth.__next(:execute){|origin_id|}
			@storage.index_delete_origin("foo_index", 1)
			sth.__verify
			dbi.__verify
		end
		def test_retrieve_from_fulltext_index
			dbi = Mock.new("dbi")
			@storage.dbi = dbi
			dbi.__next(:select_all) { |sql, d1, t1, d2, t2| 
				assert_equal('\(+\)-cloprostenolum&natricum', t1)		
				[] 
			}
			@storage.retrieve_from_fulltext_index('index_name',
				'(+)-cloprostenolum natricum', 'default_german')
		end
		def test_retrieve_from_fulltext_index
			dbi = Mock.new("dbi")
			@storage.dbi = dbi
			dbi.__next(:select_all) { |sql, d1, t1, d2, t2| 
				assert_equal('phenylbutazonum&calcicum&\(2\:1\)', t1)		
				[] 
			}
			@storage.retrieve_from_fulltext_index('index_name',
				'phenylbutazonum&calcicum&(2:1)', 'default_german')
		end
		def test_retrieve_from_fulltext_index__umlaut
			dbi = Mock.new("dbi")
			@storage.dbi = dbi
			dbi.__next(:select_all) { |sql, d1, t1, d2, t2| 
				assert_equal('dràgées&ähnlïch&kömprüssèn&ëtç', t1)		
				[] 
			}
			@storage.retrieve_from_fulltext_index('index_name',
				'dràgées ähnlïch kömprüssèn ëtç', 'default_german')
		end
	end
end
