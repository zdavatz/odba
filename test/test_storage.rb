#!/usr/bin/env ruby
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
			dbi.mock_handle(:select_all) { |query|
				assert_not_nil(query.index('IN (1,23,4)'))
				[]
			}
			@storage.bulk_restore(array)
			dbi.mock_verify
		end
		def test_delete_persistable
			dbi = flexmock("dbi")
			sth = flexmock("sth")
			@storage.dbi = dbi
			expected1 = <<-SQL
				DELETE FROM object_connection WHERE ? IN (origin_id, target_id)
			SQL
			dbi.should_receive(:prepare).with(expected1).and_return(sth)
			sth.should_receive(:execute).with(2).times(3).and_return { 
        assert(true)
      }
			expected2 = <<-SQL
				DELETE FROM collection WHERE odba_id = ?
			SQL
			dbi.should_receive(:prepare).with(expected2).and_return(sth)
			expected3 = <<-SQL
				DELETE FROM object WHERE odba_id = ?
			SQL
			dbi.should_receive(:prepare).with(expected3).and_return(sth)
			@storage.delete_persistable(2)
		end
		def test_restore_prefetchable
			dbi = flexmock("dbi")
			rows = flexmock("row")
			@storage.dbi = dbi
			dbi.mock_handle(:select_all){ |sql|
				assert_equal("\t\t\t\tSELECT odba_id, content FROM object WHERE prefetchable = true\n", sql)
				rows
			}
			@storage.restore_prefetchable
			dbi.mock_verify
			rows.mock_verify
		end
		def test_bulk_restore_empty
			dbi = flexmock("dbi")
			array = []
			@storage.dbi = dbi
			assert_nothing_raised {
				@storage.bulk_restore(array)
			}
			dbi.mock_verify
		end
		def test_create_index
			dbi = flexmock('dbi')
			sth = flexmock('sth')
      sth.should_receive(:execute).times(4).and_return { assert(true) }
			@storage.dbi = dbi
      sql = <<-SQL
        CREATE TABLE index_name (
          origin_id INTEGER,
          search_term TEXT,
          target_id INTEGER
        );
      SQL
      dbi.should_receive(:prepare).with(sql).and_return(sth)
      sql = <<-SQL
        CREATE INDEX origin_id_index_name
        ON index_name(origin_id)
      SQL
      dbi.should_receive(:prepare).with(sql).and_return(sth)
      sql = <<-SQL
        CREATE INDEX search_term_index_name
        ON index_name(search_term)
      SQL
      dbi.should_receive(:prepare).with(sql).and_return(sth)
      sql = <<-SQL
        CREATE INDEX target_id_index_name
        ON index_name(target_id)
      SQL
      dbi.should_receive(:prepare).with(sql).and_return(sth)
			@storage.create_index("index_name")
		end
		def test_next_id
			@storage.next_id = 1
			assert_equal(2, @storage.next_id)
			assert_equal(3, @storage.next_id)
		end
		def test_store__1
			dbi = flexmock("dbi")
			sth = flexmock("sth")
			@storage.dbi = dbi
			dbi.mock_handle(:select_one) { |query, id| 
				assert_equal('SELECT name FROM object WHERE odba_id = ?', 
					query)
				assert_equal(1, id)
				nil
			} 
			dbi.mock_handle(:prepare) { |query|
				expected= <<-SQL
					INSERT INTO object (odba_id, content, name, prefetchable, extent)
					VALUES (?, ?, ?, ?, ?)
				SQL
				assert_equal(expected, query)
				sth
			}
			sth.mock_handle(:execute){ |id, dump, name, prefetch, klass| 
				assert_equal(1, id)	
				assert_equal("foodump", dump)	
				assert_equal("foo", name)	
				assert_equal(true, prefetch)
				assert_equal("FlexMock", klass)
			}
			@storage.store(1,"foodump", "foo", true, FlexMock)
			dbi.mock_verify
			sth.mock_verify
		end
		def test_store__2
			dbi = flexmock("dbi")
			sth = flexmock("sth")
			@storage.dbi = dbi
			dbi.mock_handle(:select_one) { |query, id| 
				assert_equal('SELECT name FROM object WHERE odba_id = ?', 
					query)
				assert_equal(1, id)
				['name']
			} 
			dbi.mock_handle(:prepare) { |query|
				expected= <<-SQL
					UPDATE object SET 
					content = ?,
					name = ?,
					prefetchable = ?,
          extent = ?
					WHERE odba_id = ?
				SQL
				assert_equal(expected, query)
				sth
			}
			sth.mock_handle(:execute){ |dump, name, prefetch, klass, id| 
				assert_equal(1, id)	
				assert_equal("foodump", dump)	
				assert_equal("foo", name)	
				assert_equal(true, prefetch)
				assert_equal("FlexMock", klass)
			}
			@storage.store(1,"foodump", "foo", true, FlexMock)
			dbi.mock_verify
			sth.mock_verify
		end
		def test_store__3__name_only_set_in_db
			dbi = flexmock("dbi")
			sth = flexmock("sth")
			@storage.dbi = dbi
			dbi.mock_handle(:select_one) { |query, id| 
				assert_equal('SELECT name FROM object WHERE odba_id = ?', 
					query)
				assert_equal(1, id)
				{'name' => 'name_in_db'}
			} 
			dbi.mock_handle(:prepare) { |query|
				expected= <<-SQL
					UPDATE object SET 
					content = ?,
					name = ?,
					prefetchable = ?,
          extent = ?
					WHERE odba_id = ?
				SQL
				assert_equal(expected, query)
				sth
			}
			sth.mock_handle(:execute){ |dump, name, prefetch, klass, id| 
				assert_equal(1, id)	
				assert_equal("foodump", dump)	
				assert_equal("name_in_db", name)	
				assert_equal(true, prefetch)
				assert_equal("FlexMock", klass)
			}
			@storage.store(1,"foodump", nil, true, FlexMock)
			dbi.mock_verify
			sth.mock_verify
		end
		def test_restore
			dbi = flexmock
			@storage.dbi = dbi
			dbi.mock_handle(:select_one){ |arg, name| ['dump'] }
			assert_equal('dump', @storage.restore(1))
		end
		def test_restore_named
			dbi = flexmock
			@storage.dbi = dbi
			dbi.mock_handle(:select_one){ |arg, name| ['dump'] }
			assert_equal('dump', @storage.restore_named('foo'))
		end
		def test_max_id
			dbi = flexmock
			row = flexmock
			@storage.dbi = dbi
			dbi.mock_handle(:select_one){|var|
				row
			}
			row.mock_handle(:first) { 23 }
			row.mock_handle(:first) { 23 }
			assert_equal(23, @storage.max_id)
			row.mock_verify
			dbi.mock_verify
		end
		def test_restore_max_id__nil
			dbi = flexmock
			row = flexmock
			@storage.dbi = dbi
			dbi.mock_handle(:select_one){|var|
				row
			}
			row.mock_handle(:first){ || }
			id = nil
			assert_nothing_raised {
				id = @storage.restore_max_id
			}
			assert_equal(0, id)
			dbi.mock_verify
			row.mock_verify
		end
		def test_retrieve_named
			dbi = flexmock("dbi")
			sth = flexmock
			@storage.dbi = dbi
			dbi.mock_handle(:select_all) { |sql, search|
				assert_not_nil(sql.index("SELECT target_id, COUNT(target_id) AS relevance"))
				sth	
			}
			@storage.retrieve_from_index("bar","foo")
			dbi.mock_verify
			sth.mock_verify
		end
		def test_update_index
			dbi = flexmock("dbi")
			rows = [3]
			sth_delete = flexmock("sth_delete")
			sth_insert = flexmock("sth_insert")
			@storage.dbi = dbi

			#insert query
			dbi.mock_handle(:prepare){ |sql| 
				assert_not_nil(sql.index("INSERT INTO"))	
				sth_insert
			}
			sth_insert.mock_handle(:execute) { |id, term, target_id| }

			@storage.update_index("foo", 2,"baz", 3)
			dbi.mock_verify
			sth_insert.mock_verify
			sth_delete.mock_verify
		end
    def test_update_index__without_target_id
      sql = <<-'SQL'
          UPDATE index SET search_term=?
          WHERE origin_id=?
      SQL
      handle = flexmock('StatementHandle')
      @dbi.should_receive(:prepare).with(sql).and_return(handle)
      handle.should_receive(:execute).with('term', 2).and_return {
        assert(true) }
      @storage.update_index("index", 2, "term", nil)
    end
    def test_delete_index_origin
      dbi = flexmock("dbi")
      sth = flexmock
      @storage.dbi = dbi
      expected = <<-SQL
        DELETE FROM foo 
        WHERE origin_id = ?
        AND search_term = ?
      SQL
      dbi.mock_handle(:prepare) { |sql|
        assert_equal(expected, sql)
        sth
      }
      sth.mock_handle(:execute) { |id, term|
        assert_equal(2, id)
        assert_equal('search-term', term)
      }
      @storage.index_delete_origin("foo", 2, 'search-term')
      dbi.mock_verify
      sth.mock_verify
    end
		def test_retrieve_connected_objects
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			dbi.mock_handle(:select_all){|sql, target_id| 
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
			dbi.should_receive(:prepare).with(sql).times(1).and_return(sth)
			sth.should_receive(:execute).with(6, 'search-term', 5)\
        .times(1).and_return {
        assert(true) }
			@storage.index_delete_target("foo_index", 6, 'search-term', 5)
		end
		def test_drop_index
			dbi = flexmock("dbi")
			sth = flexmock("sth")
			@storage.dbi = dbi
      sql = "DROP TABLE foo_index"
			dbi.should_receive(:prepare).with(sql).and_return(sth)
			sth.should_receive(:execute).and_return { assert(true) }
			@storage.drop_index("foo_index")
		end
		def test_retrieve_from_fulltext_index
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			dbi.mock_handle(:select_all) { |sql, d1, t1, d2, t2| 
				assert_equal('\(+\)-cloprostenolum&natricum', t1)		
				[] 
			}
			@storage.retrieve_from_fulltext_index('index_name',
				'(+)-cloprostenolum natricum', 'default_german')
		end
		def test_retrieve_from_fulltext_index
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			dbi.mock_handle(:select_all) { |sql, d1, t1, d2, t2| 
				assert_equal('phenylbutazonum&calcicum&\(2\:1\)', t1)		
				[] 
			}
			@storage.retrieve_from_fulltext_index('index_name',
				'phenylbutazonum&calcicum&(2:1)', 'default_german')
		end
		def test_retrieve_from_fulltext_index__umlaut
			dbi = flexmock("dbi")
			@storage.dbi = dbi
			dbi.mock_handle(:select_all) { |sql, d1, t1, d2, t2| 
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
              WHERE target_id IN (7,9)
      SQL
      dbi.should_receive(:execute).with(sql).and_return {
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
			@storage.ensure_object_connections(123, [1,2,2,3,4,4,5,6,6])
		end
		def test_transaction_returns_blockval_even_if_dbi_does_not
			@dbi.mock_handle(:transaction) { |block|
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
      statement = flexmock('StatementHandle')
      @dbi.should_receive(:prepare).with(sql).and_return(statement)
      statement.should_receive(:execute).times(5).and_return { 
        assert(true)
      }
      sql = <<-'SQL'
CREATE INDEX origin_id_conditions ON conditions(origin_id);
      SQL
      @dbi.should_receive(:prepare).with(sql).and_return(statement)
      sql = <<-'SQL'
CREATE INDEX foo_conditions ON conditions(foo);
      SQL
      @dbi.should_receive(:prepare).with(sql).and_return(statement)
      sql = <<-'SQL'
CREATE INDEX bar_conditions ON conditions(bar);
      SQL
      @dbi.should_receive(:prepare).with(sql).and_return(statement)
      sql = <<-'SQL'
CREATE INDEX target_id_conditions ON conditions(target_id);
      SQL
      @dbi.should_receive(:prepare).with(sql).and_return(statement)
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
      @dbi.should_receive(:prepare).with(sql).and_return(statement)
      statement.should_receive(:execute).times(4).and_return {
        assert(true)
      }
      sql = <<-'SQL'
CREATE INDEX origin_id_fulltext ON fulltext(origin_id);
      SQL
      @dbi.should_receive(:prepare).with(sql).and_return(statement)
      sql = <<-'SQL'
CREATE INDEX search_term_fulltext
ON fulltext USING gist(search_term);
      SQL
      @dbi.should_receive(:prepare).with(sql).and_return(statement)
      sql = <<-'SQL'
CREATE INDEX target_id_fulltext ON fulltext(target_id);
      SQL
      @dbi.should_receive(:prepare).with(sql).and_return(statement)
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
      @dbi.should_receive(:prepare).with(sql).and_return(statement)
      statement.should_receive(:execute).with(34, 'key_dump').and_return {
        assert(true)
      }
      @storage.collection_remove(34, "key_dump")
    end
    def test_collection_store
      sql = <<-'SQL'
        INSERT INTO collection (odba_id, key, value)
        VALUES (?, ?, ?)
      SQL
      statement = flexmock('StatementHandle')
      @dbi.should_receive(:prepare).with(sql).and_return(statement)
      statement.should_receive(:execute).with(34, 'key_dump', 'dump').and_return {
        assert(true)
      }
      @storage.collection_store(34, "key_dump", 'dump')
    end
    def test_generate_dictionary
      dir = File.expand_path('data', File.dirname(__FILE__))
      sql = <<-'SQL'
        INSERT INTO pg_ts_cfg (ts_name, prs_name, locale)
        VALUES ('default_german', 'default', 'DE');
      SQL
      @dbi.should_receive(:execute).with(sql).and_return {
        assert(true)
      }
      sql = <<-'SQL'
        INSERT INTO pg_ts_dict (
          SELECT 'german_ispell', dict_init, ?, dict_lexize
          FROM pg_ts_dict
          WHERE dict_name = 'ispell_template'
        );
      SQL
      statement = flexmock('StatementHandle')
      @dbi.should_receive(:prepare).with(sql).and_return {
        statement
      }
      path = 'AffFile="' << dir \
        << '/fulltext.aff",DictFile="' << dir \
        << '/fulltext.dict",StopFile="' << dir << '/fulltext.stop"'
      statement.should_receive(:execute).with(path).and_return { 
        assert(true)
      }
      sql = <<-'SQL'
        INSERT INTO pg_ts_dict (
          dict_name, dict_init, dict_lexize
        )
        VALUES (
          'german_stem', 'dinit_german(internal)',
          'snb_lexize(internal, internal, int4)'
        );
      SQL
      @dbi.should_receive(:execute).with(sql).and_return {
        assert(true)
      }
      @dbi.should_receive(:execute).with(/INSERT INTO pg_ts_cfgmap/).times(19).and_return { assert(true) }
      @storage.generate_dictionary('german', 'DE', dir)
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
    def test_remove_dictionary
      sql = <<-'SQL'
        DELETE FROM pg_ts_cfg
        WHERE ts_name='default_german'
      SQL
      @dbi.should_receive(:execute).with(sql).and_return {
        assert(true)
      }
      sql = <<-'SQL'
        DELETE FROM pg_ts_dict
        WHERE dict_name IN ('german_ispell', 'german_stem')
      SQL
      @dbi.should_receive(:execute).with(sql).and_return {
        assert(true)
      }
      sql = <<-'SQL'
        DELETE FROM pg_ts_cfgmap
        WHERE ts_name='default_german'
      SQL
      @dbi.should_receive(:execute).with(sql).and_return {
        assert(true)
      }
      @storage.remove_dictionary('german')
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
        GROUP BY target_id;
      SQL
      @dbi.should_receive(:select_all)\
        .with(sql, 'foo', 'bar%', 5).and_return {
        assert(true)
      }
      conds = [
        ['cond1', 'foo'],
        ['cond2', nil],
        ['cond3', {'condition' => 'LIKE', 'value' => 'bar'}],
        ['cond4', {'condition' => '>', 'value' => 5}],
      ]
      @storage.retrieve_from_condition_index('index', conds)
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
CREATE INDEX origin_id_index ON object_connection(origin_id);
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
      sql = <<-'SQL'
ALTER TABLE object ADD COLUMN extent TEXT;
CREATE INDEX extent_index ON object(extent);
      SQL
      @dbi.should_receive(:execute).with(sql).and_return {
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
      @dbi.should_receive(:prepare).with(sql).and_return(handle)
      handle.should_receive(:execute).with(12, 15, 14, 'blur').and_return {
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
      @dbi.should_receive(:prepare).with(sql).and_return(handle)
      handle.should_receive(:execute).with(14, 'blur', 12).and_return {
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
      @dbi.should_receive(:prepare).with(sql).and_return(handle)
      handle.should_receive(:execute).with(12, "german", "some text", 
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
      @dbi.should_receive(:prepare).with(sql).and_return(handle)
      handle.should_receive(:execute).with("german", "some text", 
                                           12).and_return {
        assert(true)
      }
      @storage.update_fulltext_index('index', 12, "some  text", nil,
                                     'german')
    end
    def test_condition_index_delete
      sql = <<-SQL
        DELETE FROM index
        WHERE origin_id = ?\n AND c1 = ? AND c2 = ?
      SQL
      handle = flexmock('DBHandle')
      @dbi.should_receive(:prepare).with(sql.chomp)\
        .times(1).and_return(handle)
      handle.should_receive(:execute).with(3, 'f', 7)\
        .times(1).and_return { assert(true) }
      @storage.condition_index_delete('index', 3, {'c1','f','c2',7})
    end
    def test_condition_index_delete__with_target_id
      sql = <<-SQL
        DELETE FROM index
        WHERE origin_id = ?\n AND c1 = ? AND c2 = ? AND target_id = ?
      SQL
      handle = flexmock('DBHandle')
      @dbi.should_receive(:prepare).with(sql.chomp)\
        .times(1).and_return(handle)
      handle.should_receive(:execute).with(3, 'f', 7, 4)\
        .times(1).and_return { assert(true) }
      @storage.condition_index_delete('index', 3, {'c1','f','c2',7}, 4)
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
      @dbi.should_receive(:execute).with(sql, 4)\
        .times(1).and_return { assert(true) }
      @storage.fulltext_index_delete('index', 4, 'origin_id')
    end
    def test_fulltext_index_delete__target
      sql = <<-SQL
        DELETE FROM index
        WHERE target_id = ?
      SQL
      @dbi.should_receive(:execute).with(sql, 4)\
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
	end
end
