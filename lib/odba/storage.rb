#!/usr/bin/env ruby
#-- Storage -- odba -- 29.04.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

require 'thread'
require 'singleton'
require 'dbi'

module ODBA
	class Storage # :nodoc: all
		include Singleton
		attr_writer :dbi
		BULK_FETCH_STEP = 2500
    TABLES = {
      # in table 'object', the isolated dumps of all objects are stored
      'object'            => <<-'SQL',
CREATE TABLE object (
  odba_id INTEGER NOT NULL, content TEXT,
  name TEXT, prefetchable BOOLEAN, extent TEXT,
  PRIMARY KEY(odba_id), UNIQUE(name)
);
CREATE INDEX prefetchable_index ON object(prefetchable);
CREATE INDEX extent_index ON object(extent);
      SQL
      # helper table 'object_connection'
      'object_connection' => <<-'SQL',
CREATE TABLE object_connection (
  origin_id integer, target_id integer,
  PRIMARY KEY(origin_id, target_id)
);
CREATE INDEX target_id_index ON object_connection(target_id);
      SQL
      # helper table 'collection'
      'collection'        => <<-'SQL',
CREATE TABLE collection (
  odba_id integer NOT NULL, key text, value text,
  PRIMARY KEY(odba_id, key)
);
      SQL
    }
		def initialize
			@id_mutex = Mutex.new
		end
		def bulk_restore(bulk_fetch_ids)
			if(bulk_fetch_ids.empty?)
				[]
			else
				bulk_fetch_ids = bulk_fetch_ids.uniq
				rows = []
				while(!(ids = bulk_fetch_ids.slice!(0, BULK_FETCH_STEP)).empty?)
					sql = <<-SQL
						SELECT odba_id, content FROM object 
						WHERE odba_id IN (#{ids.join(',')})
					SQL
					rows.concat(self.dbi.select_all(sql))
				end
				rows
			end
		end
    def collection_fetch(odba_id, key_dump)
      sql = <<-SQL
        SELECT value FROM collection 
        WHERE odba_id = ? AND key = ?
      SQL
      row = self.dbi.select_one(sql, odba_id, key_dump)
      row.first unless row.nil?
    end
    def collection_remove(odba_id, key_dump)
      sth = self.dbi.prepare <<-SQL
        DELETE FROM collection
        WHERE odba_id = ? AND key = ?
      SQL
      sth.execute(odba_id, key_dump)
    end
    def collection_store(odba_id, key_dump, value_dump)
      sth = self.dbi.prepare <<-SQL 
        INSERT INTO collection (odba_id, key, value)
        VALUES (?, ?, ?)
      SQL
      sth.execute(odba_id, key_dump, value_dump)
    end
    def condition_index_delete(index_name, origin_id, 
                               search_terms, target_id=nil)
      values = []
      sql = <<-SQL
        DELETE FROM #{index_name}
        WHERE origin_id = ?
      SQL
      search_terms.each { |key, value|
        sql << " AND %s = ?" % key
        values << value
      }
      if(target_id)
        sql << " AND target_id = ?"
        values << target_id
      end
      sth = self.dbi.prepare(sql)
      sth.execute(origin_id, *values)
    end
    def condition_index_ids(index_name, id, id_name)
      sql = <<-SQL
        SELECT DISTINCT *
        FROM #{index_name}
        WHERE #{id_name}=?
      SQL
      self.dbi.select_all(sql, id)
    end
    def create_dictionary_map(language)
      %w{lhword lpart_hword lword}.each { |token|
        self.dbi.execute <<-SQL
          INSERT INTO pg_ts_cfgmap (ts_name, tok_alias, dict_name)
          VALUES ('default_#{language}', '#{token}',
          '{#{language}_ispell,#{language}_stem}')
        SQL
      }
      [ 'url', 'host', 'sfloat', 'uri', 'int', 'float', 'email',
        'word', 'hword', 'nlword', 'nlpart_hword', 'part_hword',
        'nlhword', 'file', 'uint', 'version' 
      ].each { |token|
        self.dbi.execute <<-SQL
          INSERT INTO pg_ts_cfgmap (ts_name, tok_alias, dict_name)
          VALUES ('default_#{language}', '#{token}', '{simple}')
        SQL
      }
    end
    def create_condition_index(table_name, definition)
      self.dbi.prepare(<<-SQL).execute
CREATE TABLE #{table_name} (
  origin_id INTEGER,
  #{definition.collect { |*pair| pair.join(' ') }.join(",\n  ") },
  target_id INTEGER
);
      SQL
      #index origin_id
      self.dbi.prepare(<<-SQL).execute
CREATE INDEX origin_id_#{table_name} ON #{table_name}(origin_id);
      SQL
      #index search_term
      definition.each { |name, datatype|
        self.dbi.prepare(<<-SQL).execute
CREATE INDEX #{name}_#{table_name} ON #{table_name}(#{name});
        SQL
      }
      #index target_id
      self.dbi.prepare(<<-SQL).execute
CREATE INDEX target_id_#{table_name} ON #{table_name}(target_id);
      SQL
    end
    def create_fulltext_index(table_name)
      self.dbi.prepare(<<-SQL).execute
CREATE TABLE #{table_name} (
  origin_id INTEGER,
  search_term tsvector,
  target_id INTEGER
);
      SQL
      #index origin_id
      self.dbi.prepare(<<-SQL).execute
CREATE INDEX origin_id_#{table_name} ON #{table_name}(origin_id);
      SQL
      #index search_term
      self.dbi.prepare(<<-SQL).execute
CREATE INDEX search_term_#{table_name}
ON #{table_name} USING gist(search_term);
      SQL
      #index target_id
      self.dbi.prepare(<<-SQL).execute
CREATE INDEX target_id_#{table_name} ON #{table_name}(target_id);
      SQL
    end
    def create_index(table_name)
      self.dbi.prepare(<<-SQL).execute
        CREATE TABLE #{table_name} (
          origin_id INTEGER,
          search_term TEXT,
          target_id INTEGER
        );
      SQL
      #index origin_id
      self.dbi.prepare(<<-SQL).execute
        CREATE INDEX origin_id_#{table_name}
        ON #{table_name}(origin_id)
      SQL
      #index search_term
      self.dbi.prepare(<<-SQL).execute
        CREATE INDEX search_term_#{table_name}
        ON #{table_name}(search_term)
      SQL
      #index target_id
      self.dbi.prepare(<<-SQL).execute
        CREATE INDEX target_id_#{table_name}
        ON #{table_name}(target_id)
      SQL
    end
		def dbi
			Thread.current[:txn] || @dbi
		end
		def drop_index(index_name)
			self.dbi.prepare("DROP TABLE #{index_name}").execute
		end
    def delete_index_element(index_name, odba_id, id_name)
      self.dbi.prepare(<<-SQL).execute(odba_id)
        DELETE FROM #{index_name} WHERE #{id_name} = ?
      SQL
    end
		def delete_persistable(odba_id)
      # delete origin from connections
			self.dbi.prepare(<<-SQL).execute(odba_id)
				DELETE FROM object_connection WHERE origin_id = ?
			SQL
      # delete target from connections
			self.dbi.prepare(<<-SQL).execute(odba_id)
				DELETE FROM object_connection WHERE target_id = ?
			SQL
      # delete from collections
			self.dbi.prepare(<<-SQL).execute(odba_id)
				DELETE FROM collection WHERE odba_id = ?
			SQL
      # delete from objects
			self.dbi.prepare(<<-SQL).execute(odba_id)
				DELETE FROM object WHERE odba_id = ?
			SQL
		end
    def ensure_object_connections(origin_id, target_ids)
      sql = <<-SQL
        SELECT target_id FROM object_connection
        WHERE origin_id = ?
      SQL
      target_ids.uniq!
      update_ids = target_ids
      old_ids = []
      ## use self.dbi instead of @dbi to get information about
      ## object_connections previously stored within this transaction
      if(rows = self.dbi.select_all(sql, origin_id))
        old_ids = rows.collect { |row| row[0] }
        old_ids.uniq!
        delete_ids = old_ids - target_ids
        update_ids = target_ids - old_ids
        unless(delete_ids.empty?)
          while(!(ids = delete_ids.slice!(0, BULK_FETCH_STEP)).empty?)
            sql = <<-SQL
              DELETE FROM object_connection
              WHERE origin_id = ? AND target_id IN (#{ids.join(',')})
            SQL
            self.dbi.execute(sql, origin_id)
          end
        end
      end
      sth = self.dbi.prepare <<-SQL
        INSERT INTO object_connection (origin_id, target_id)
        VALUES (?, ?)
      SQL
      update_ids.each { |id|
        sth.execute(origin_id, id)
      }
    end
    def ensure_target_id_index(table_name)
      #index target_id
      self.dbi.execute(<<-SQL)
        CREATE INDEX target_id_#{table_name}
        ON #{table_name}(target_id)
      SQL
    rescue
    end
    def extent_count(klass)
      self.dbi.select_one(<<-EOQ, klass.name).first
        SELECT COUNT(odba_id) FROM object WHERE extent = ?
      EOQ
    end
    def extent_ids(klass)
      self.dbi.select_all(<<-EOQ, klass.name).flatten
        SELECT odba_id FROM object WHERE extent = ?
      EOQ
    end
    def fulltext_index_delete(index_name, id, id_name)
      self.dbi.execute(<<-SQL, id)
        DELETE FROM #{index_name}
        WHERE #{id_name} = ?
      SQL
    end
    def fulltext_index_target_ids(index_name, origin_id)
      sql = <<-SQL
        SELECT DISTINCT target_id
        FROM #{index_name}
        WHERE origin_id=?
      SQL
      self.dbi.select_all(sql, origin_id)
    end
    def generate_dictionary(language, locale, dict_dir)
      # setup configuration
      self.dbi.execute <<-SQL
        INSERT INTO pg_ts_cfg (ts_name, prs_name, locale)
        VALUES ('default_#{language}', 'default', '#{locale}');
      SQL
      # insert path to dictionary
      sth = self.dbi.prepare <<-SQL
        INSERT INTO pg_ts_dict (
          SELECT '#{language}_ispell', dict_init, ?, dict_lexize
          FROM pg_ts_dict
          WHERE dict_name = 'ispell_template'
        );
      SQL
      prepath = File.expand_path("fulltext", dict_dir)
      path = %w{Aff Dict Stop}.collect { |type|
        sprintf('%sFile="%s.%s"', type, prepath, type.downcase)
      }.join(',')
      sth.execute(path)
      create_dictionary_map(language)
      self.dbi.execute <<-SQL
        INSERT INTO pg_ts_dict (
          dict_name, dict_init, dict_lexize
        )
        VALUES (
          '#{language}_stem', 'dinit_#{language}(internal)',
          'snb_lexize(internal, internal, int4)'
        );
      SQL
    end
    def index_delete_origin(index_name, odba_id, term)
      self.dbi.prepare(<<-SQL).execute(odba_id, term)
        DELETE FROM #{index_name} 
        WHERE origin_id = ?
        AND search_term = ?
      SQL
    end
    def index_delete_target(index_name, origin_id, search_term, target_id)
      sth = self.dbi.prepare <<-SQL
        DELETE FROM #{index_name} 
        WHERE origin_id = ?
        AND search_term = ?
        AND target_id = ?
      SQL
      sth.execute(origin_id, search_term, target_id)
    end
    def index_fetch_keys(index_name, length=nil)
      expr = if(length)
               "substr(search_term, 1, #{length})"
             else
               "search_term"
             end
      sql = <<-SQL
        SELECT DISTINCT #{expr} AS key
        FROM #{index_name}
        ORDER BY key
      SQL
      self.dbi.select_all(sql).flatten
    end
    def index_origin_ids(index_name, target_id)
      sql = <<-SQL
        SELECT DISTINCT origin_id, search_term
        FROM #{index_name}
        WHERE target_id=?
      SQL
      self.dbi.select_all(sql, target_id)
    end
    def index_target_ids(index_name, origin_id)
      sql = <<-SQL
        SELECT DISTINCT target_id, search_term
        FROM #{index_name}
        WHERE origin_id=?
      SQL
      self.dbi.select_all(sql, origin_id)
    end
		def max_id
			ensure_next_id_set
			@next_id
		end
		def next_id
			ensure_next_id_set
			@next_id += 1
		end
    def remove_dictionary(language)
      # remove configuration
      self.dbi.execute <<-SQL
        DELETE FROM pg_ts_cfg
        WHERE ts_name='default_#{language}'
      SQL
      # remove dictionaries
      self.dbi.execute <<-SQL
        DELETE FROM pg_ts_dict
        WHERE dict_name IN ('#{language}_ispell', '#{language}_stem')
      SQL
      # remove tokens
      self.dbi.execute <<-SQL
        DELETE FROM pg_ts_cfgmap
        WHERE ts_name='default_#{language}'
      SQL
    end
		def restore(odba_id)
			row = self.dbi.select_one("SELECT content FROM object WHERE odba_id = ?", odba_id)
			row.first unless row.nil?
		end	
		def retrieve_connected_objects(target_id)
			sql = <<-SQL 
				SELECT origin_id FROM object_connection 
				WHERE target_id = ?
			SQL
			self.dbi.select_all(sql, target_id)
		end
    def retrieve_from_condition_index(index_name, conditions, limit=nil)
      sql = <<-EOQ
        SELECT target_id, COUNT(target_id) AS relevance
        FROM #{index_name}
        WHERE TRUE
      EOQ
      values = []
      lines = conditions.collect { |name, info|
        val = nil
        condition = nil
        if(info.is_a?(Hash))
          condition = info['condition']
          if(val = info['value']) 
            if(/i?like/i.match(condition))
              val += '%'
            end
            condition = "#{condition || '='} ?"
            values.push(val)
          end
        elsif(info)
          condition = "= ?"
          values.push(info)
        end
        sql << <<-EOQ
          AND #{name} #{condition || 'IS NULL'}
        EOQ
      }
      sql << "        GROUP BY target_id\n"
      if(limit)
        sql << " LIMIT #{limit}"
      end
      self.dbi.select_all(sql, *values)
    end
		def retrieve_from_fulltext_index(index_name, search_term, dict)
      ## this combination of gsub statements solves the problem of 
      #  properly escaping strings of this form: "(2:1)" into 
      #  '\(2\:1\)' (see test_retrieve_from_fulltext_index)
			term = search_term.gsub(/\s+/, '&').gsub(/&+/, '&')\
        .gsub(/[():]/i, '\\ \\&').gsub(/\s/, '')
	    sql = <<-EOQ
				SELECT target_id, 
					max(rank(search_term, to_tsquery(?, ?))) AS relevance
				FROM #{index_name} 
				WHERE search_term @@ to_tsquery(?, ?) 
				GROUP BY target_id
				ORDER BY relevance DESC
			EOQ
			self.dbi.select_all(sql, dict, term, dict, term)
		rescue DBI::ProgrammingError => e
			warn("ODBA::Storage.retrieve_from_fulltext_index rescued a DBI::ProgrammingError(#{e.message}). Query:")
			warn("self.dbi.select_all(#{sql}, #{dict}, #{term}, #{dict}, #{term})")
			warn("returning empty result")
			[]
		end
    def retrieve_from_index(index_name, search_term, 
                            exact=nil, limit=nil)
      unless(exact)
        search_term = search_term + "%"
      end
      sql = <<-EOQ
        SELECT target_id, COUNT(target_id) AS relevance
        FROM #{index_name}
        WHERE search_term LIKE ?
        GROUP BY target_id
      EOQ
      if(limit)
        sql << " LIMIT #{limit}"
      end
      self.dbi.select_all(sql, search_term)	 
    end
		def restore_collection(odba_id)
			self.dbi.select_all <<-EOQ
				SELECT key, value FROM collection WHERE odba_id = #{odba_id}
			EOQ
		end
		def restore_named(name)
			row = self.dbi.select_one("SELECT content FROM object WHERE name = ?", 
				name)
			row.first unless row.nil?
		end
		def restore_prefetchable
			self.dbi.select_all <<-EOQ
				SELECT odba_id, content FROM object WHERE prefetchable = true
			EOQ
		end
    def setup
      tables = self.dbi.tables
      TABLES.each { |name, definition|
        unless(tables.include?(name))
          self.dbi.execute(definition)
        end
      }
      unless(self.dbi.columns('object').any? { |col| col.name == 'extent' })
        self.dbi.execute <<-EOS
ALTER TABLE object ADD COLUMN extent TEXT;
CREATE INDEX extent_index ON object(extent);
        EOS
      end
    end
		def store(odba_id, dump, name, prefetchable, klass)
			sql = "SELECT name FROM object WHERE odba_id = ?"
			if(row = self.dbi.select_one(sql, odba_id))
				name ||= row['name']
				sth = self.dbi.prepare <<-SQL
					UPDATE object SET 
					content = ?,
					name = ?,
					prefetchable = ?,
          extent = ?
					WHERE odba_id = ?
				SQL
				sth.execute(dump, name, prefetchable, klass.name, odba_id)
			else
				sth = self.dbi.prepare <<-SQL
					INSERT INTO object (odba_id, content, name, prefetchable, extent)
					VALUES (?, ?, ?, ?, ?)
				SQL
				sth.execute(odba_id, dump, name, prefetchable, klass.name)
			end
		end
		def transaction(&block)
			dbi = nil
			retval = nil
			@dbi.transaction { |dbi|
				dbi['AutoCommit'] = false
				Thread.current[:txn] = dbi
				retval = block.call
			}
			retval
		ensure
			dbi['AutoCommit'] = true
			Thread.current[:txn] = nil
		end
    def update_condition_index(index_name, origin_id, search_terms, target_id)
      keys = []
      vals = []
      search_terms.each { |key, val|
        keys.push(key)
        vals.push(val)
      }
      if(target_id)
        sth_insert = self.dbi.prepare <<-SQL
INSERT INTO #{index_name} (origin_id, target_id, #{keys.join(', ')})
VALUES (?, ?#{', ?' * keys.size})
        SQL
        sth_insert.execute(origin_id, target_id, *vals)
      else
        key_str = keys.collect { |key| "#{key}=?" }.join(', ')
        sth_update = self.dbi.prepare <<-SQL
UPDATE #{index_name} SET #{key_str}
WHERE origin_id = ?
        SQL
        sth_update.execute(*(vals.push(origin_id)))
      end
    end
    def update_fulltext_index(index_name, origin_id, search_term, target_id, dict)
      search_term = search_term.gsub(/\s+/, ' ').strip
      if(target_id)
        sth_insert = self.dbi.prepare <<-SQL
INSERT INTO #{index_name} (origin_id, search_term, target_id)
VALUES (?, to_tsvector(?, ?), ?)
        SQL
        sth_insert.execute(origin_id, dict, search_term, target_id)
      else
        sth_update = self.dbi.prepare <<-SQL
UPDATE #{index_name} SET search_term=to_tsvector(?, ?)
WHERE origin_id=?
        SQL
        sth_update.execute(dict, search_term, origin_id)
      end
    end
		def update_index(index_name, origin_id, search_term, target_id)
      if(target_id)
        sth_insert = self.dbi.prepare <<-SQL
          INSERT INTO #{index_name} (origin_id, search_term, target_id) 
          VALUES (?, ?, ?)
        SQL
        sth_insert.execute(origin_id, search_term, target_id)
      else
        sth_update = self.dbi.prepare <<-SQL
          UPDATE #{index_name} SET search_term=?
          WHERE origin_id=?
        SQL
        sth_update.execute(search_term, origin_id)
      end
		end
		private
		def ensure_next_id_set
			@id_mutex.synchronize {
				if(@next_id.nil?)
					@next_id = restore_max_id
				end
			}
		end
		def restore_max_id
			row = self.dbi.select_one("select MAX(odba_id) from object")
			unless(row.first.nil?)
				row.first
			else
				0
			end
		end
	end
end
