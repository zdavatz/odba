#!/usr/bin/env ruby
# TestStorage -- 10.05.2004 -- rwaltert@ywesee.com mwalder@ywesee.com

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
				assert_not_nil(query.index('in (1,23,4)'))
			}
			@storage.bulk_restore(array)
			dbi.__verify
		end
		def test_delete_persistable
			dbi = Mock.new("dbi")
			sth = Mock.new
			@storage.dbi = dbi
			dbi.__next(:prepare) { |sql|
				assert_equal(sql, "delete from object where odba_id = ?")
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
				assert_equal("select odba_id, content from object where prefetchable = true", sql)
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
			@storage.dbi = dbi
			dbi.__next(:prepare){ |query| 
				assert_not_nil(query.index('table sequences'))
				sth
			}
			sth.__next(:execute){  }
			dbi.__next(:prepare){ |query| 
				assert_not_nil(query.index('index search_term_sequences on'))
				sth2
			}
			sth2.__next(:execute){  }
			@storage.create_index("sequences")
			dbi.__verify
			sth.__verify
		end
		def test_update
			dbi = Mock.new
			sth = Mock.new
			row = Mock.new
			@storage.dbi = dbi
			dbi.__next(:prepare){ |arg| sth}
			sth.__next(:execute){ |id,dump,name, prefetch| row}
			sth.__next(:rows){ || 0}
			@storage.update(2,"34353", "foo", true)
			dbi.__verify
			sth.__verify
		end
		def test_next_id
			@storage.next_id = 1
			assert_equal(2, @storage.next_id)
			assert_equal(3, @storage.next_id)
		end
		def test_store_insert
			dbi = Mock.new
			sth = Mock.new
			row = Mock.new
			@storage.dbi = dbi
			dbi.__next(:prepare) { |arg| sth} 
			sth.__next(:execute){ |id, dump, name, prefetch| row }
			sth.__next(:rows){ || 0}
			dbi.__next(:prepare) { |arg| sth} 
			sth.__next(:execute){ |id, dump, name, prefetch| row }
			@storage.store(1,"foodump", "foo", true)
			dbi.__verify
			sth.__verify
		end
		def test_store_update
			dbi = Mock.new
			sth = Mock.new
			row = Mock.new
			@storage.dbi = dbi
			dbi.__next(:prepare) { |arg| sth} 
			sth.__next(:execute){ |id, dump, name, prefetch| row }
			sth.__next(:rows){ || 1}
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
				assert_not_nil(sql.index("inner join bar"))
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
			dbi.__next(:prepare) { |sql|
				assert_equal(sql, "delete from foo where origin_id = ?")
				sth
			}
			sth.__next(:execute) { |id|
				assert_equal(2, id)
			}
			@storage.delete_index_element("foo", 2)
			dbi.__verify
			sth.__verify
		end
=begin
		def test_update_index
			dbi = Mock.new("dbi")
			sth = Mock.new
			@storage.dbi = dbi
			dbi.__next(:prepare){|sql|
				assert_not_nil(sql.index(""))
				sth
			}
			sth.__next(:execute){|search_term, origin_id|
				assert_equal(search_term, "foobar")
				assert_equal(origin_id, 1)
			}
			@storage.update_index("foo", 1, "foobar")
		end
=end
		def test_add_object_connection
				dbi = Mock.new("dbi")
				sth = Mock.new("sth")
				@storage.dbi = dbi
				rows = [[0]]
				dbi.__next(:select_all){ |query, id, traget_id| 
					assert_not_nil(query.index('select count(origin_id)'))
					rows
				}
				dbi.__next(:prepare){ |query| 
					assert_not_nil(query.index('insert into object_connection'))
					sth
				}
				sth.__next(:execute){  }
				@storage.add_object_connection(1, 3)
				dbi.__verify
				sth.__verify
		end
		def test_retrieve_connected_objects
			dbi = Mock.new("dbi")
			@storage.dbi = dbi
			dbi.__next(:select_all){|sql, target_id| 
				assert_not_nil(sql.index('select origin_id from object_connection'))
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
	end
end
