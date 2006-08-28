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
			'object'						=> 'CREATE TABLE object ( odba_id integer NOT NULL, 
															content text, name text, prefetchable boolean,
															PRIMARY KEY(odba_id), UNIQUE(name));',
			'object_connection'	=> 'CREATE TABLE object_connection ( origin_id integer,
															target_id integer, 
															PRIMARY KEY(origin_id, target_id));
															CREATE INDEX target_id_index 
															ON object_connection (target_id);
															CREATE INDEX origin_id_index 
															ON object_connection (origin_id);',
			'collection'				=> 'CREATE TABLE collection ( odba_id integer NOT NULL,
															key text, value text,
															PRIMARY KEY(odba_id, key));', 
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
		def create_dictionary_map(language)
			['lhword', 'lpart_hword', 'lword'].each { |token|
				self.dbi.execute <<-SQL
					INSERT INTO pg_ts_cfgmap (ts_name, tok_alias, dict_name)
					VALUES ('default_#{language}', '#{token}',
					'{#{language}_ispell,#{language}_stem}')
				SQL
			}
			['url', 'host', 'sfloat', 'uri', 'int', 'float', 'email', 'word',
				'hword', 'nlword', 'nlpart_hword', 'part_hword', 'nlhword', 
				'file', 'uint', 'version'].each { |token|
				self.dbi.execute <<-SQL
					INSERT INTO pg_ts_cfgmap (ts_name, tok_alias, dict_name)
					VALUES ('default_#{language}', '#{token}', '{simple}')
				SQL
			}
		end
		def create_condition_index(table_name, definition)
			sql = <<-SQL
				CREATE TABLE #{table_name} (
					origin_id integer, 
					#{definition.collect { |*pair| pair.join(' ') }.join(",\n") }, 
					target_id integer
				)
			SQL
			self.dbi.prepare(sql).execute
			#index origin_id
			sql = <<-SQL
				CREATE INDEX origin_id_#{table_name} 
				ON #{table_name}(origin_id)
			SQL
			self.dbi.prepare(sql).execute
			#index search_term
			definition.each_key { |name|
				sql = <<-SQL
					CREATE INDEX #{name}_#{table_name} 
					ON #{table_name}(#{name})
				SQL
				self.dbi.prepare(sql).execute
			}
		end
		def create_fulltext_index(table_name)
			sql = <<-SQL
				CREATE TABLE #{table_name} (
					origin_id integer, 
					search_term tsvector, 
					target_id integer
				)
			SQL
			self.dbi.prepare(sql).execute
			#index origin_id
			sql = <<-SQL
				CREATE INDEX origin_id_#{table_name} 
				ON #{table_name}(origin_id)
			SQL
			self.dbi.prepare(sql).execute
			#index search_term
			sql = <<-SQL
				CREATE INDEX search_term_#{table_name} 
				ON #{table_name} USING gist(search_term)
			SQL
			self.dbi.prepare(sql).execute
		end
		def create_index(table_name)
			sql = <<-SQL
				CREATE TABLE #{table_name} (
					origin_id integer, 
					search_term text, 
					target_id integer
				)
			SQL
			self.dbi.prepare(sql).execute
			#index origin_id
			sql = <<-SQL
				CREATE INDEX origin_id_#{table_name} 
				ON #{table_name}(origin_id)
			SQL
			self.dbi.prepare(sql).execute
			#index search_term
			sql = <<-SQL
				CREATE INDEX search_term_#{table_name} 
				ON #{table_name}(search_term)
			SQL
			self.dbi.prepare(sql).execute
		end
		def dbi
			Thread.current[:txn] || @dbi
		end
		def drop_index(index_name)
			sth = self.dbi.prepare("DROP TABLE #{index_name}")
			sth.execute
		end
		def delete_index_element(index_name, odba_id)
			sth = self.dbi.prepare <<-SQL
				DELETE FROM #{index_name} WHERE origin_id = ?
			SQL
			sth.execute(odba_id)
		end
		def delete_persistable(odba_id)
			sql = <<-SQL
				DELETE FROM object_connection WHERE ? IN (origin_id, target_id)
			SQL
			sth = self.dbi.prepare(sql)
			sth.execute(odba_id)
			sql = <<-SQL
				DELETE FROM collection WHERE odba_id = ?
			SQL
			sth = self.dbi.prepare(sql)
			sth.execute(odba_id)
			sql = <<-SQL
				DELETE FROM object WHERE odba_id = ?
			SQL
			sth = self.dbi.prepare(sql)
			sth.execute(odba_id)
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
							WHERE target_id IN (#{ids.join(',')})
						SQL
						self.dbi.execute(sql)
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
		def generate_dictionary(language, locale, dict_dir)
			self.dbi.execute <<-SQL
				INSERT INTO pg_ts_cfg (ts_name, prs_name, locale)
				VALUES ('default_#{language}', 'default', '#{locale}')
			SQL
			sth = self.dbi.prepare <<-SQL
				INSERT INTO pg_ts_dict (
					SELECT '#{language}_ispell', dict_init, ?, dict_lexize 
					FROM pg_ts_dict 
					WHERE dict_name = 'ispell_template'
				)
			SQL
			stopfile = File.expand_path("fulltext.stop", dict_dir)
			path = [
				'AffFile="' << File.expand_path("fulltext.aff", dict_dir) << '"',
				'DictFile="' << File.expand_path("fulltext.dict", dict_dir) << '"',
				'StopFile="' << stopfile << '"',
			].join(',')
			sth.execute(path)
			create_dictionary_map(language)
			self.dbi.execute <<-SQL
				INSERT INTO pg_ts_dict (
					dict_name, dict_init, dict_initoption, dict_lexize, dict_comment
				) 
				VALUES (
					'#{language}_stem', 'dinit_#{language}(internal)', NULL, 
					'snb_lexize(internal, internal, int4)', '#{language} stemmer. snowball'
				)
			SQL
		end
		def index_delete_origin(index_name, origin_id)
			sth = self.dbi.prepare <<-SQL
				DELETE FROM #{index_name} WHERE origin_id = ?
			SQL
			sth.execute(origin_id)
		end
		def index_delete_target(index_name, target_id)
			sth = self.dbi.prepare <<-SQL
				DELETE FROM #{index_name} WHERE target_id = ?
			SQL
			sth.execute(target_id)
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
		def max_id
			ensure_next_id_set
			@next_id
		end
		def next_id
			ensure_next_id_set
			@next_id += 1
		end
		def remove_dead_connections(min_id, max_id)
			sth = self.dbi.prepare <<-EOQ
				DELETE FROM object_connection
				WHERE origin_id BETWEEN ? AND ?
				AND (
				(
					SELECT odba_id FROM object 
					WHERE odba_id=origin_id
				) IS NULL
				OR
				(
					SELECT odba_id FROM object 
					WHERE odba_id=target_id
				)	IS NULL
			)
			EOQ
			sth.execute(min_id, max_id)
			#if(rows = sth.rows)
			#	warn("deleted #{rows} dead connections")
			#end
		end
		def remove_dead_objects(min_id, max_id)
			# remove all objects which are not being linked to and have no name
			sth = self.dbi.prepare <<-EOQ
			DELETE FROM object
			WHERE (
				SELECT DISTINCT target_id 
				FROM object_connection 
				WHERE target_id=odba_id
				AND origin_id!=odba_id
			) IS NULL
			AND name IS NULL
			AND odba_id BETWEEN ? AND ?
			EOQ
			sth.execute(min_id, max_id)
			#if(rows = sth.rows)
			#	warn("deleted #{rows} dead objects")
			#end
		end
		def remove_dictionary(language)
			self.dbi.execute <<-SQL
				DELETE FROM pg_ts_cfg 
				WHERE ts_name='default_#{language}'
			SQL
			self.dbi.execute <<-SQL
				DELETE FROM pg_ts_dict 
				WHERE dict_name IN ('#{language}_ispell', '#{language}_stem')
			SQL
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
		def retrieve_from_condition_index(index_name, conditions)
			sql = <<-EOQ
				SELECT target_id, COUNT(target_id) AS relevance
        FROM #{index_name}
				WHERE	TRUE
			EOQ
			values = []
			lines = conditions.collect { |name, info|
				val = nil
				condition = info['condition']
				if(val = info['value']) 
					if(/i?like/i.match(condition))
						val += '%'
					end
					condition = "#{condition || '='} ?"
					values.push(val)
				end
				sql << <<-EOQ
					AND #{name} #{condition || 'IS NULL'}
				EOQ
			}
      sql << " GROUP BY target_id"
			self.dbi.select_all(sql, *values)
		end
		def retrieve_from_fulltext_index(index_name, search_term, dict)
			term = search_term.gsub(/\s+/, '&').gsub(/[():]/i, 
				'\\ \\&').gsub(/\s/, '')
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
		def retrieve_from_index(index_name, search_term, exact=nil)
			unless(exact)
				search_term = search_term + "%"
			end
			sql = <<-EOQ
				SELECT target_id, COUNT(target_id) AS relevance
				FROM #{index_name} 
				WHERE search_term LIKE ?
				GROUP BY target_id
			EOQ
			self.dbi.select_all(sql, search_term.downcase)	 
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
			rows = []
			TABLES.each { |name, definition|
				unless(tables.include?(name))
					sth = self.dbi.prepare(definition)
					rows = sth.execute
				end
			}
			rows
		end
		def store(odba_id, dump, name, prefetchable)
			sql = "SELECT name FROM object WHERE odba_id = ?"
			if(row = self.dbi.select_one(sql, odba_id))
				name ||= row['name']
				sth = self.dbi.prepare <<-SQL
					UPDATE object SET 
					content = ?,
					name = ?,
					prefetchable = ?
					WHERE odba_id = ?
				SQL
				sth.execute(dump, name, prefetchable, odba_id)
			else
				sth = self.dbi.prepare <<-SQL
					INSERT INTO object (odba_id, content, name, prefetchable)
					VALUES (?, ?, ?, ?)
				SQL
				sth.execute(odba_id, dump, name, prefetchable)
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
			sth_insert = self.dbi.prepare <<-SQL
				INSERT INTO #{index_name} (origin_id, target_id, #{keys.join(', ')}) 
				VALUES (?, ?#{', ?' * keys.size})
			SQL
			sth_insert.execute(origin_id, target_id, *vals)
		end
		def update_fulltext_index(index_name, origin_id, search_term, target_id, dict)
			sth_insert = self.dbi.prepare("INSERT INTO #{index_name} (origin_id, search_term, target_id) VALUES (?, to_tsvector(?, ? ), ?)")
			sth_insert.execute(origin_id, dict, search_term, target_id)
		end
		def update_index(index_name, origin_id, search_term, target_id)
			sth_insert = self.dbi.prepare <<-SQL
				INSERT INTO #{index_name} (origin_id, search_term, target_id) 
				VALUES (?, ?, ?)
			SQL
			sth_insert.execute(origin_id, search_term.downcase, target_id)
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
