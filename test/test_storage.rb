#!/usr/bin/env ruby
# encoding: utf-8
# TestStorage -- odba -- 10.05.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

$: << File.dirname(__FILE__)
$: << File.expand_path('../lib/', File.dirname(__FILE__))

require 'odba/storage'
require 'test/unit'
require 'flexmock'

module ODBA
	class Storage
		public :restore_max_id
		attr_writer :next_id
	end
	class TestStorage < Test::Unit::TestCase
    include FlexMock::TestCase
		def setup
			@storage = ODBA::Storage.instance
      @dbi = flexmock('DBI')
      @storage.dbi = @dbi
		end
		def test_bulk_restore
			dbi = flexmock("dbi")
			array = [1, 23, 4]
			@storage.dbi = dbi
			dbi.should_receive(:select_all).times(1).and_return { |query|
				assert_not_nil(query.index('IN (1,23,4)'))
				[]
			}
			@storage.bulk_restore(array)
		end
		def test_delete_persistable
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			expected1 = <<-SQL
				DELETE FROM object_connection WHERE origin_id = ?
			SQL
			dbi.should_receive(:do).with(expected1, 2).times(1).and_return do
        assert true
      end
			expected2 = <<-SQL
				DELETE FROM object_connection WHERE target_id = ?
			SQL
			dbi.should_receive(:do).with(expected2, 2).times(1).and_return do
        assert true
      end
			expected3 = <<-SQL
				DELETE FROM collection WHERE odba_id = ?
			SQL
			dbi.should_receive(:do).with(expected3, 2).times(1).and_return do
        assert true
      end
			expected4 = <<-SQL
				DELETE FROM object WHERE odba_id = ?
			SQL
			dbi.should_receive(:do).with(expected4, 2).times(1).and_return do
        assert true
      end
			@storage.delete_persistable(2)
		end
		def test_restore_prefetchable
			dbi = flexmock("dbi")
			rows = flexmock("row")
			@storage.dbi = dbi
			dbi.should_receive(:select_all).times(1).and_return{ |sql|
				assert_equal("\t\t\t\tSELECT odba_id, content FROM object WHERE prefetchable = true\n", sql)
				rows
			}
			@storage.restore_prefetchable
		end
		def test_bulk_restore_empty
			dbi = flexmock("dbi")
			array = []
			@storage.dbi = dbi
			assert_nothing_raised {
				@storage.bulk_restore(array)
			}
		end
		def test_create_index
			dbi = flexmock('dbi')
			@storage.dbi = dbi
      sql = <<-SQL
        CREATE TABLE index_name (
          origin_id INTEGER,
          search_term TEXT,
          target_id INTEGER
        );
      SQL
      dbi.should_receive(:do).times(1).with(sql).and_return do
        assert true
      end
      sql = <<-SQL
        CREATE INDEX origin_id_index_name
        ON index_name(origin_id)
      SQL
      dbi.should_receive(:do).times(1).with(sql).and_return do
        assert true
      end
      sql = <<-SQL
        CREATE INDEX search_term_index_name
        ON index_name(search_term)
      SQL
      dbi.should_receive(:do).times(1).with(sql).and_return do
        assert true
      end
      sql = <<-SQL
        CREATE INDEX target_id_index_name
        ON index_name(target_id)
      SQL
      dbi.should_receive(:do).times(1).with(sql).and_return do
        assert true
      end
			@storage.create_index("index_name")
		end
		def test_next_id
			@storage.next_id = 1
			assert_equal(2, @storage.next_id)
			assert_equal(3, @storage.next_id)
		end
		def test_store__1
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			dbi.should_receive(:select_one).times(1).and_return { |query, id| 
				assert_equal('SELECT name FROM object WHERE odba_id = ?', 
					query)
				assert_equal(1, id)
				nil
			} 
			dbi.should_receive(:do).times(1).and_return { |query, id, dump, name, prefetch, klass|
				expected= <<-SQL
					INSERT INTO object (odba_id, content, name, prefetchable, extent)
					VALUES (?, ?, ?, ?, ?)
				SQL
				assert_equal(expected, query)
				assert_equal(1, id)	
				assert_equal("foodump", dump)	
				assert_equal("foo", name)	
				assert_equal(true, prefetch)
				assert_equal("FlexMock", klass)
			}
			@storage.store(1,"foodump", "foo", true, FlexMock)
		end
		def test_store__2
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			dbi.should_receive(:select_one).times(1).and_return { |query, id| 
				assert_equal('SELECT name FROM object WHERE odba_id = ?', 
					query)
				assert_equal(1, id)
				['name']
			} 
			dbi.should_receive(:do).times(1).and_return { |query, dump, name, prefetch, klass, id|
				expected= <<-SQL
					UPDATE object SET 
					content = ?,
					name = ?,
					prefetchable = ?,
          extent = ?
					WHERE odba_id = ?
				SQL
				assert_equal(expected, query)
				assert_equal(1, id)	
				assert_equal("foodump", dump)	
				assert_equal("foo", name)	
				assert_equal(true, prefetch)
				assert_equal("FlexMock", klass)
			}
			@storage.store(1,"foodump", "foo", true, FlexMock)
		end
		def test_store__3__name_only_set_in_db
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			dbi.should_receive(:select_one).times(1).and_return { |query, id| 
				assert_equal('SELECT name FROM object WHERE odba_id = ?', 
					query)
				assert_equal(1, id)
				{'name' => 'name_in_db'}
			} 
			dbi.should_receive(:do).times(1).and_return { |query, dump, name, prefetch, klass, id|
				expected= <<-SQL
					UPDATE object SET 
					content = ?,
					name = ?,
					prefetchable = ?,
          extent = ?
					WHERE odba_id = ?
				SQL
				assert_equal(expected, query)
				assert_equal(1, id)	
				assert_equal("foodump", dump)	
				assert_equal("name_in_db", name)	
				assert_equal(true, prefetch)
				assert_equal("FlexMock", klass)
			}
			@storage.store(1,"foodump", nil, true, FlexMock)
		end
		def test_restore
			dbi = flexmock
			@storage.dbi = dbi
			dbi.should_receive(:select_one).times(1).and_return{ |arg, name| ['dump'] }
			assert_equal('dump', @storage.restore(1))
		end
		def test_restore_named
			dbi = flexmock
			@storage.dbi = dbi
			dbi.should_receive(:select_one).times(1).and_return{ |arg, name| ['dump'] }
			assert_equal('dump', @storage.restore_named('foo'))
		end
		def test_max_id
			dbi = flexmock
			row = flexmock
			@storage.dbi = dbi
			dbi.should_receive(:select_one).and_return{|var|
				row
			}
			row.should_receive(:first).times(1).and_return { 23 }
			row.should_receive(:first).times(1).and_return { 23 }
			assert_equal(23, @storage.max_id)
		end
		def test_restore_max_id__nil
			dbi = flexmock
			row = flexmock
			@storage.dbi = dbi
			dbi.should_receive(:select_one).times(1).and_return{|var|
				row
			}
			row.should_receive(:first).times(1).and_return{ || }
			id = nil
			assert_nothing_raised {
				id = @storage.restore_max_id
			}
			assert_equal(0, id)
		end
    def test_retrieve
      dbi = flexmock("dbi")
      sth = flexmock
      @storage.dbi = dbi
      sql = <<-SQL
        SELECT target_id, COUNT(target_id) AS relevance
        FROM index
        WHERE search_term LIKE ?
        GROUP BY target_id
      SQL
      dbi.should_receive(:select_all).with(sql, 'foo%')\
        .and_return { assert(true) }
      @storage.retrieve_from_index("index","foo")
    end
    def test_retrieve_exact
      dbi = flexmock("dbi")
      sth = flexmock
      @storage.dbi = dbi
      sql = <<-SQL
        SELECT target_id, COUNT(target_id) AS relevance
        FROM index
        WHERE search_term LIKE ?
        GROUP BY target_id
      SQL
      dbi.should_receive(:select_all).with(sql, 'foo')\
        .and_return { assert(true) }
      @storage.retrieve_from_index("index","foo", true)
    end
    def test_retrieve_one
      dbi = flexmock("dbi")
      sth = flexmock
      @storage.dbi = dbi
      sql = <<-SQL << " LIMIT 1"
        SELECT target_id, COUNT(target_id) AS relevance
        FROM index
        WHERE search_term LIKE ?
        GROUP BY target_id
      SQL
      dbi.should_receive(:select_all).with(sql, 'foo%')\
        .and_return { assert(true) }
      @storage.retrieve_from_index("index","foo", false, 1)
    end
		def test_update_index
			dbi = flexmock("dbi")
			rows = [3]
			@storage.dbi = dbi

			#insert query
			dbi.should_receive(:do).times(1).and_return{ |sql, id, term, target_id| 
				assert_not_nil(sql.index("INSERT INTO"))	
			}

			@storage.update_index("foo", 2,"baz", 3)
		end
    def test_update_index__without_target_id
      sql = <<-'SQL'
          UPDATE index SET search_term=?
          WHERE origin_id=?
      SQL
      handle = flexmock('StatementHandle')
      @dbi.should_receive(:do).with(sql, 'term', 2).times(1).and_return do
        assert true
      end
      @storage.update_index("index", 2, "term", nil)
    end
    def test_delete_index_origin
      dbi = flexmock("dbi")
      @storage.dbi = dbi
      expected = <<-SQL
        DELETE FROM foo 
        WHERE origin_id = ?
        AND search_term = ?
      SQL
      dbi.should_receive(:do).and_return { |sql, id, term|
        assert_equal(expected, sql)
        assert_equal(2, id)
        assert_equal('search-term', term)
      }
      @storage.index_delete_origin("foo", 2, 'search-term')
    end
		def test_retrieve_connected_objects
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			dbi.should_receive(:select_all).and_return{|sql, target_id| 
				assert_not_nil(sql.index('SELECT origin_id FROM object_connection'))
				assert_equal(target_id, 1)
			}	
			@storage.retrieve_connected_objects(1)
		end
		def test_index_delete_target
			dbi = flexmock("dbi")
			sth = flexmock("sth")
			@storage.dbi = dbi
      sql = <<-SQL
        DELETE FROM foo_index 
        WHERE origin_id = ?
        AND search_term = ?
        AND target_id = ?
      SQL
			dbi.should_receive(:do).with(sql, 6, 'search-term', 5).times(1).and_return do
        assert true
      end
			@storage.index_delete_target("foo_index", 6, 'search-term', 5)
		end
		def test_drop_index
			dbi = flexmock("dbi")
			@storage.dbi = dbi
      sql = "DROP TABLE foo_index"
			dbi.should_receive(:do).with(sql).and_return do
        assert true
      end
			@storage.drop_index("foo_index")
		end
		def test_retrieve_from_fulltext_index
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			dbi.should_receive(:select_all).and_return { |sql, d1, t1, d2, t2| 
				assert_equal('\(+\)-cloprostenolum&natricum', t1)		
				[] 
			}
			@storage.retrieve_from_fulltext_index('index_name',
				'(+)-cloprostenolum natricum', 'default_german')
		end
		def test_retrieve_from_fulltext_index__2
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			dbi.should_receive(:select_all).and_return { |sql, d1, t1, d2, t2| 
				assert_equal('phenylbutazonum&calcicum&\(2\:1\)', t1)		
				[] 
			}
			@storage.retrieve_from_fulltext_index('index_name',
				'phenylbutazonum&calcicum&(2:1)', 'default_german')
		end
		def test_retrieve_from_fulltext_index__umlaut
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			dbi.should_receive(:select_all).and_return { |sql, d1, t1, d2, t2| 
				assert_equal('dràgées&ähnlïch&kömprüssèn&ëtç', t1)		
				[] 
			}
			@storage.retrieve_from_fulltext_index('index_name',
				'dràgées ähnlïch kömprüssèn ëtç', 'default_german')
		end
		def test_ensure_object_connections
			dbi = flexmock("dbi")
			@storage.dbi = dbi
      sql = <<-SQL
        SELECT target_id FROM object_connection
        WHERE origin_id = ?
      SQL
			dbi.should_receive(:select_all).with(sql, 123).and_return { 
        assert(true)
				[[1], [3], [5], [7], [9]]
			}
      sql = <<-SQL
              DELETE FROM object_connection
              WHERE origin_id = ? AND target_id IN (7,9)
      SQL
      dbi.should_receive(:do).with(sql, 123).and_return {
        assert(true)
      }
      sql = <<-SQL
        INSERT INTO object_connection (origin_id, target_id)
        VALUES (?, ?)
      SQL
      sth = flexmock('sth')
      dbi.should_receive(:prepare).with(sql).and_return(sth)
      sth.should_receive(:execute).with(123, 2).times(1).and_return {
        assert(true)
      }
      sth.should_receive(:execute).with(123, 4).times(1).and_return {
        assert(true)
      }
      sth.should_receive(:execute).with(123, 6).times(1).and_return {
        assert(true)
      }
      sth.should_receive(:finish).times(1)
			@storage.ensure_object_connections(123, [1,2,2,3,4,4,5,6,6])
		end
		def test_transaction_returns_blockval_even_if_dbi_does_not
			@dbi.should_receive(:transaction).and_return { |block|
				block.call({})
				false 
			}
			res = @storage.transaction { "foo" }
			assert_equal("foo", res)
		end
    def test_create_condition_index
      definition = [
        [:foo, 'Integer'],
        [:bar, 'Date'],
      ]
      sql = <<-'SQL'
CREATE TABLE conditions (
  origin_id INTEGER,
  foo Integer,
  bar Date,
  target_id INTEGER
);
      SQL
      @dbi.should_receive(:do).with(sql).and_return do
        assert true
      end
      sql = <<-'SQL'
CREATE INDEX origin_id_conditions ON conditions(origin_id);
      SQL
      @dbi.should_receive(:do).with(sql).and_return do
        assert true
      end
      sql = <<-'SQL'
CREATE INDEX foo_conditions ON conditions(foo);
      SQL
      @dbi.should_receive(:do).with(sql).and_return do
        assert true
      end
      sql = <<-'SQL'
CREATE INDEX bar_conditions ON conditions(bar);
      SQL
      @dbi.should_receive(:do).with(sql).and_return do
        assert true
      end
      sql = <<-'SQL'
CREATE INDEX target_id_conditions ON conditions(target_id);
      SQL
      @dbi.should_receive(:do).with(sql).and_return do
        assert true
      end
      @storage.create_condition_index('conditions', definition)
    end
    def test_create_fulltext_index
      sql = <<-'SQL'
CREATE TABLE fulltext (
  origin_id INTEGER,
  search_term tsvector,
  target_id INTEGER
);
      SQL
      statement = flexmock('StatementHandle')
      @dbi.should_receive(:do).with(sql).and_return do
        assert true
      end
      sql = <<-'SQL'
CREATE INDEX origin_id_fulltext ON fulltext(origin_id);
      SQL
      @dbi.should_receive(:do).with(sql).and_return do
        assert true
      end
      sql = <<-'SQL'
CREATE INDEX search_term_fulltext
ON fulltext USING gist(search_term);
      SQL
      @dbi.should_receive(:do).with(sql).and_return do
        assert true
      end
      sql = <<-'SQL'
CREATE INDEX target_id_fulltext ON fulltext(target_id);
      SQL
      @dbi.should_receive(:do).with(sql).and_return do
        assert true
      end
      @storage.create_fulltext_index('fulltext')
    end
    def test_extent_ids
      sql = <<-'SQL'
        SELECT odba_id FROM object WHERE extent = ?
      SQL
      @dbi.should_receive(:select_all).with(sql, 'Object').and_return {
        [[1], [2], [3], [4], [5]]
      }
      expected = [1,2,3,4,5]
      assert_equal(expected, @storage.extent_ids(Object))
    end
    def test_collection_fetch
      sql = <<-'SQL'
        SELECT value FROM collection 
        WHERE odba_id = ? AND key = ?
      SQL
      @dbi.should_receive(:select_one).with(sql, 34, 'key_dump').and_return {
        ["dump"]
      }
      assert_equal("dump", @storage.collection_fetch(34, "key_dump"))
    end
    def test_collection_remove
      sql = <<-'SQL'
        DELETE FROM collection
        WHERE odba_id = ? AND key = ?
      SQL
      statement = flexmock('StatementHandle')
      @dbi.should_receive(:do).with(sql, 34, 'key_dump').and_return do
        assert true
      end
      @storage.collection_remove(34, "key_dump")
    end
    def test_collection_store
      sql = <<-'SQL'
        INSERT INTO collection (odba_id, key, value)
        VALUES (?, ?, ?)
      SQL
      statement = flexmock('StatementHandle')
      @dbi.should_receive(:do).with(sql, 34, 'key_dump', 'dump').and_return do
        assert true
      end
      @storage.collection_store(34, "key_dump", 'dump')
    end
    def test_index_fetch_keys
      sql = <<-'SQL'
        SELECT DISTINCT search_term AS key
        FROM index
        ORDER BY key
      SQL
      @dbi.should_receive(:select_all).with(sql).and_return { 
        [['key1'], ['key2'], ['key3']]
      }
      assert_equal(%w{key1 key2 key3}, 
                   @storage.index_fetch_keys('index'))
      sql = <<-'SQL'
        SELECT DISTINCT substr(search_term, 1, 2) AS key
        FROM index
        ORDER BY key
      SQL
      @dbi.should_receive(:select_all).with(sql).and_return { 
        [['k1'], ['k2'], ['k3']]
      }
      assert_equal(%w{k1 k2 k3}, 
                   @storage.index_fetch_keys('index', 2))
    end
    def test_index_target_ids
      sql = <<-'SQL'
        SELECT DISTINCT target_id, search_term
        FROM index
        WHERE origin_id=?
      SQL
      @dbi.should_receive(:select_all).with(sql, 5).and_return { 
        [[1, 'search-term'], [2, 'search-term'], [3, 'search-term']]
      }
      expected = [[1, 'search-term'], [2, 'search-term'], [3, 'search-term']]
      assert_equal(expected, @storage.index_target_ids('index', 5))
    end
    def test_retrieve_from_condition_index
      sql = <<-'SQL'
        SELECT target_id, COUNT(target_id) AS relevance
        FROM index
        WHERE TRUE
          AND cond1 = ?
          AND cond2 IS NULL
          AND cond3 LIKE ?
          AND cond4 > ?
        GROUP BY target_id
      SQL
      @dbi.should_receive(:select_all)\
        .with(sql, 'foo', 'bar%', '5').and_return {
        assert(true)
      }
      conds = [
        ['cond1', 'foo'],
        ['cond2', nil],
        ['cond3', {'condition' => 'LIKE', 'value' => 'bar'}],
        ['cond4', {'condition' => '>', 'value' => 5}],
      ]
      @storage.retrieve_from_condition_index('index', conds)
      sql << ' LIMIT 1'
      @dbi.should_receive(:select_all)\
        .with(sql, 'foo', 'bar%', 5).and_return {
        assert(true)
      }
      @storage.retrieve_from_condition_index('index', conds, 1)
    end
    def test_setup__object
      tables = %w{object_connection collection}
      @dbi.should_receive(:tables).and_return(tables)
      sql = <<-'SQL'
CREATE TABLE object (
  odba_id INTEGER NOT NULL, content TEXT,
  name TEXT, prefetchable BOOLEAN, extent TEXT,
  PRIMARY KEY(odba_id), UNIQUE(name)
);
CREATE INDEX prefetchable_index ON object(prefetchable);
CREATE INDEX extent_index ON object(extent);
      SQL
      @dbi.should_receive(:execute).with(sql).and_return {
        assert(true) }
      col = flexmock('Column')
      col.should_receive(:name).and_return('extent')
      @dbi.should_receive(:columns).and_return([col])
      @storage.setup
    end
    def test_setup__object_connection
      tables = %w{object collection}
      @dbi.should_receive(:tables).and_return(tables)
      sql = <<-'SQL'
CREATE TABLE object_connection (
  origin_id integer, target_id integer,
  PRIMARY KEY(origin_id, target_id)
);
CREATE INDEX target_id_index ON object_connection(target_id);
      SQL
      @dbi.should_receive(:execute).with(sql).and_return {
        assert(true) }
      col = flexmock('Column')
      col.should_receive(:name).and_return('extent')
      @dbi.should_receive(:columns).and_return([col])
      @storage.setup
    end
    def test_setup__collection
      tables = %w{object object_connection}
      @dbi.should_receive(:tables).and_return(tables)
      sql = <<-'SQL'
CREATE TABLE collection (
  odba_id integer NOT NULL, key text, value text,
  PRIMARY KEY(odba_id, key)
);
      SQL
      @dbi.should_receive(:execute).with(sql).and_return {
        assert(true) }
      col = flexmock('Column')
      col.should_receive(:name).and_return('extent')
      @dbi.should_receive(:columns).and_return([col])
      @storage.setup
    end
    def test_setup__extent
      tables = %w{object object_connection collection}
      @dbi.should_receive(:tables).and_return(tables)
      sql = "CREATE TABLE object (\n  odba_id INTEGER NOT NULL, content TEXT,\n  name TEXT, prefetchable BOOLEAN, extent TEXT,\n  PRIMARY KEY(odba_id), UNIQUE(name)\n);\n"
      @dbi.should_receive(:do).with(sql).and_return(true)
      sql  = "CREATE INDEX prefetchable_index ON object(prefetchable);\n"
      @dbi.should_receive(:do).with(sql).and_return(true)
      sql = "CREATE INDEX extent_index ON object(extent);\n"
      @dbi.should_receive(:do).with(sql).and_return(true)
      sql = "CREATE TABLE object_connection (\n  origin_id integer, target_id integer,\n  PRIMARY KEY(origin_id, target_id)\n);\n"
      @dbi.should_receive(:do).with(sql).and_return(true)
      sql = "CREATE INDEX target_id_index ON object_connection(target_id);\n"
      @dbi.should_receive(:do).with(sql).and_return(true)
      sql = "CREATE TABLE collection (\n  odba_id integer NOT NULL, key text, value text,\n  PRIMARY KEY(odba_id, key)\n);\n"
      @dbi.should_receive(:do).with(sql).and_return(true)
      sql = <<-'SQL'
ALTER TABLE object ADD COLUMN extent TEXT;
CREATE INDEX extent_index ON object(extent);
      SQL
      @dbi.should_receive(:do).with(sql).and_return {
        assert(true) }
      @dbi.should_receive(:columns).and_return([])
      @storage.setup
    end
    def test_update_condition_index__with_target_id
      handle = flexmock('StatementHandle')
      sql = <<-'SQL'
INSERT INTO index (origin_id, target_id, foo, bar)
VALUES (?, ?, ?, ?)
      SQL
      @dbi.should_receive(:do).with(sql, 12, 15, 14, 'blur').times(1).and_return {
        assert(true)
      }
      terms = [
        ['foo', 14],
        ['bar', 'blur'],
      ]
      @storage.update_condition_index('index', 12, terms, 15)
    end
    def test_update_condition_index__without_target_id
      handle = flexmock('StatementHandle')
      sql = <<-'SQL'
UPDATE index SET foo=?, bar=?
WHERE origin_id = ?
      SQL
      @dbi.should_receive(:do).with(sql, 14, 'blur', 12).times(1).and_return {
        assert(true)
      }
      terms = [
        ['foo', 14],
        ['bar', 'blur'],
      ]
      @storage.update_condition_index('index', 12, terms, nil)
    end
    def test_update_fulltext_index__with_target_id
      handle = flexmock('StatementHandle')
      sql = <<-'SQL'
INSERT INTO index (origin_id, search_term, target_id)
VALUES (?, to_tsvector(?, ?), ?)
      SQL
      @dbi.should_receive(:do).with(sql, 12, "german", "some text", 
                                    15).and_return {
        assert(true)
      }
      @storage.update_fulltext_index('index', 12, "some  text", 15, 
                                     'german')
    end
    def test_update_fulltext_index__without_target_id
      handle = flexmock('StatementHandle')
      sql = <<-'SQL'
UPDATE index SET search_term=to_tsvector(?, ?)
WHERE origin_id=?
      SQL
      @dbi.should_receive(:do).with(sql, "german", "some text", 
                                    12).and_return {
        assert(true)
      }
      @storage.update_fulltext_index('index', 12, "some  text", nil,
                                     'german')
    end
    def test_condition_index_delete
      sql = <<-SQL
DELETE FROM index WHERE origin_id = ? AND c1 = ? AND c2 = ?
      SQL
      if /^1\.8/.match(RUBY_VERSION)
        sql = "DELETE FROM index WHERE origin_id = ? AND c2 = ? AND c1 = ?"
        @dbi.should_receive(:do).with(sql.chomp, 3, 7, 'f').times(1).and_return(true)
      else
        sql = "DELETE FROM index WHERE origin_id = ? AND c1 = ? AND c2 = ?"
        @dbi.should_receive(:do).with(sql.chomp, 3, 'f', 7).times(1).and_return(true)
      end
      handle = flexmock('DBHandle')
      @storage.condition_index_delete('index', 3, {'c1' => 'f','c2' => 7})
    end
    def test_condition_index_delete__with_target_id
      handle = flexmock('DBHandle')
      if /^1\.8/.match(RUBY_VERSION)
        sql = "DELETE FROM index WHERE origin_id = ? AND c2 = ? AND c1 = ? AND target_id = ?"
        @dbi.should_receive(:do).with(sql.chomp, 3, 7, 'f', 4).times(1).and_return(true)
      else
        sql = "DELETE FROM index WHERE origin_id = ? AND c1 = ? AND c2 = ? AND target_id = ?"
        @dbi.should_receive(:do).with(sql.chomp, 3, 'f', 7, 4).times(1).and_return(true)
      end
      @storage.condition_index_delete('index', 3, {'c1' => 'f','c2' => 7}, 4)
    end
    def test_condition_index_ids__origin_id
      sql = <<-SQL
        SELECT DISTINCT *
        FROM index
        WHERE origin_id=?
      SQL
      @dbi.should_receive(:select_all).with(sql, 5)\
        .times(1).and_return { assert(true) }
      @storage.condition_index_ids('index', 5, 'origin_id')
    end
    def test_condition_index_ids__target_id
      sql = <<-SQL
        SELECT DISTINCT *
        FROM index
        WHERE target_id=?
      SQL
      @dbi.should_receive(:select_all).with(sql, 5)\
        .times(1).and_return { assert(true) }
      @storage.condition_index_ids('index', 5, 'target_id')
    end
    def test_ensure_target_id_index
      sql = <<-SQL
        CREATE INDEX target_id_index
        ON index(target_id)
      SQL
      @dbi.should_receive(:execute).with(sql).and_return { 
        raise DBI::Error }
      assert_nothing_raised {
        @storage.ensure_target_id_index('index')   
      }
    end
    def test_fulltext_index_delete__origin
      sql = <<-SQL
        DELETE FROM index
        WHERE origin_id = ?
      SQL
      @dbi.should_receive(:do).with(sql, 4)\
        .times(1).and_return { assert(true) }
      @storage.fulltext_index_delete('index', 4, 'origin_id')
    end
    def test_fulltext_index_delete__target
      sql = <<-SQL
        DELETE FROM index
        WHERE target_id = ?
      SQL
      @dbi.should_receive(:do).with(sql, 4)\
        .times(1).and_return { assert(true) }
      @storage.fulltext_index_delete('index', 4, 'target_id')
    end
    def test_fulltext_index_target_ids
      sql = <<-SQL
        SELECT DISTINCT target_id
        FROM index
        WHERE origin_id=?
      SQL
      @dbi.should_receive(:select_all).with(sql, 4)\
        .times(1).and_return { assert(true) }
      @storage.fulltext_index_target_ids('index', 4)
    end
    def test_index_origin_ids
      sql = <<-SQL
        SELECT DISTINCT origin_id, search_term
        FROM index
        WHERE target_id=?
      SQL
      @dbi.should_receive(:select_all).with(sql, 4)\
        .times(1).and_return { assert(true) }
      @storage.index_origin_ids('index', 4)
    end
    def test_delete_index_element__origin
      handle = flexmock('DB-Handle')
      @dbi.should_receive(:do).with(<<-SQL, 15).times(1).and_return {
        DELETE FROM index WHERE origin_id = ?
      SQL
        assert(true)
      }
      @storage.delete_index_element('index', 15, 'origin_id')
    end
    def test_delete_index_element__target
      handle = flexmock('DB-Handle')
      @dbi.should_receive(:do).with(<<-SQL, 15).times(1).and_return {
        DELETE FROM index WHERE target_id = ?
      SQL
        assert(true)
      }
      @storage.delete_index_element('index', 15, 'target_id')
    end
	end
end
