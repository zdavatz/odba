#!/usr/bin/env ruby
# Storage -- odba -- 29.04.2004 -- rwaltert@ywesee.com mwalder@ywesee.com

require 'thread'
require 'singleton'
require 'dbi'

module ODBA
	class Storage
		include Singleton
		attr_accessor :dbi
		def initialize
			@id_mutex = Mutex.new
		end
		def add_object_connection(origin_id, target_id)
			sth = @dbi.prepare("SELECT ensure_object_connection(?, ?)")
			sth.execute(origin_id, target_id)	
=begin
			#SELECT
			sql = <<-SQL
				SELECT COUNT(origin_id) FROM object_connection 
				WHERE origin_id = ? AND target_id = ?
			SQL
			rows = @dbi.select_all(sql, origin_id, target_id)
			if(rows.first.first == 0)
				#INSERT
				sth = @dbi.prepare <<-SQL
					INSERT INTO object_connection(origin_id, target_id) 
					VALUES (?,?)
				SQL
				sth.execute(origin_id, target_id)	
			end
=end
		end
		def bulk_restore(bulk_fetch_ids)
			if(bulk_fetch_ids.empty?)
				[]
			else
				sql = "select odba_id, content from object where odba_id in (#{bulk_fetch_ids.join(',')})"
				@dbi.select_all(sql)
			end
		end
		def create_dictionary_map(language)
			['lhword', 'lpart_hword', 'lword'].each { |token|
				@dbi.execute <<-SQL
					INSERT INTO pg_ts_cfgmap (ts_name, tok_alias, dict_name)
					VALUES ('default_#{language}', '#{token}',
					'{#{language}_ispell,#{language}_stem}')
				SQL
			}
			['url', 'host', 'sfloat', 'uri', 'int', 'float', 'email', 'word',
				'hword', 'nlword', 'nlpart_hword', 'part_hword', 'nlhword', 
				'file', 'uint', 'version'].each { |token|
				@dbi.execute <<-SQL
					INSERT INTO pg_ts_cfgmap (ts_name, tok_alias, dict_name)
					VALUES ('default_#{language}', '#{token}', '{simple}')
				SQL
			}
		end
		def create_index(table_name)
			sql = "create table #{table_name} (origin_id integer, search_term text, target_id integer)" 
			@dbi.prepare(sql).execute
			#index search_term
			sql = "create index search_term_#{table_name} on #{table_name}(search_term)"
			@dbi.prepare(sql).execute
		end
		def create_fulltext_index(table_name)
			sql = "create table #{table_name} (origin_id integer, search_term tsvector, target_id integer)" 
			@dbi.prepare(sql).execute
			sql = "CREATE INDEX search_term_#{table_name} ON #{table_name} USING gist(search_term)"
			@dbi.prepare(sql).execute
		end
		def drop_index(index_name)
			sth = @dbi.prepare("DROP TABLE #{index_name}")
			sth.execute
		end
		def delete_index_element(index_name, odba_id)
			sth = @dbi.prepare <<-SQL
				DELETE FROM #{index_name} WHERE origin_id = ?
			SQL
			sth.execute(odba_id)
		end
		def delete_persistable(odba_id)
			sql = <<-SQL
				DELETE FROM object WHERE odba_id = ?
			SQL
			sth = @dbi.prepare(sql)
			sth.execute(odba_id)
			sql = <<-SQL
				DELETE FROM object_connection WHERE ? IN (origin_id, target_id)
			SQL
			sth = @dbi.prepare(sql)
			sth.execute(odba_id)
		end
		def generate_dictionary(language, locale, dict_dir)
			@dbi.execute <<-SQL
				INSERT INTO pg_ts_cfg (ts_name, prs_name, locale)
				VALUES ('default_#{language}', 'default', '#{locale}')
			SQL
			sth = @dbi.prepare <<-SQL
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
			@dbi.execute <<-SQL
				INSERT INTO pg_ts_dict (
					dict_name, dict_init, dict_initoption, dict_lexize, dict_comment
				) 
				VALUES (
					'#{language}_stem', 'snb_en_init(text)', '#{stopfile}', 
					'snb_lexize(internal, internal, integer)', '#{language} stem'
				)
			SQL
		end
		def index_delete_origin(index_name, origin_id)
			sth = @dbi.prepare <<-SQL
				DELETE FROM #{index_name} WHERE origin_id = ?
			SQL
			sth.execute(origin_id)
		end
		def index_delete_target(index_name, target_id)
			sth = @dbi.prepare <<-SQL
				DELETE FROM #{index_name} WHERE target_id = ?
			SQL
			sth.execute(target_id)
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
=begin
			sth = @dbi.prepare <<-EOQ
				DELETE FROM object_connection
				WHERE origin_id BETWEEN ? AND ?
				AND (origin_id NOT IN 
				(
					SELECT odba_id 
					FROM object 
					WHERE odba_id BETWEEN ? AND ?
				) 
				OR target_id NOT IN (SELECT odba_id FROM object))
			EOQ
=end
			sth = @dbi.prepare <<-EOQ
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
=begin
				DELETE FROM object_connection 
				WHERE target_id NOT IN 
				( SELECT odba_id FROM object)
				OR origin_id NOT IN 
				( SELECT odba_id FROM object)
				AND origin_id BETWEEN ? AND ?
=end
=begin
			rows_target = @dbi.select_all("select target_id from object_connection left join object on object.odba_id = target_id where odba_id is null")
			rows_origin = @dbi.select_all("select origin_id from object_connection left join object on object.odba_id = origin_id where odba_id is null")
			total_rows = rows_target.concat(rows_origin)
			total_rows.each { |row|
				id = row.first
				sth = @dbi.prepare("delete from object_connection where target_id = ? or origin_id = ?");
				sth.execute(id, id)
			}
=end
		end
		def remove_dead_objects(min_id, max_id)
			# remove all objects which are not being linked to 
=begin
			sth = @dbi.prepare <<-EOQ
			DELETE FROM object
			WHERE odba_id IN ( 
				SELECT object.odba_id FROM object_connection 
				RIGHT JOIN object ON 
					(object.odba_id BETWEEN ? AND ?)
				AND ((object_connection.origin_id BETWEEN ? AND ?) 
				OR (object_connection.target_id BETWEEN ? AND ?)) 
				AND ((object_connection.origin_id = object.odba_id) 
				OR (object_connection.target_id = object.odba_id)) 
				WHERE target_id IS null
			)
			AND odba_id BETWEEN ? AND ?
			EOQ
=end
			sth = @dbi.prepare <<-EOQ
			DELETE FROM object
			WHERE (
				SELECT DISTINCT target_id 
				FROM object_connection 
				WHERE target_id=odba_id
			) IS NULL
			AND odba_id BETWEEN ? AND ?
			EOQ
			sth.execute(min_id, max_id)
=begin
			unless(rows.first.nil?)
				sth = @dbi.prepare("delete from object where odba_id in (#{rows.join(',')})");
				sth.execute
			end
=end
		end
		def remove_dictionary(language)
			@dbi.execute <<-SQL
				DELETE FROM pg_ts_cfg 
				WHERE ts_name='default_#{language}'
			SQL
			@dbi.execute <<-SQL
				DELETE FROM pg_ts_dict 
				WHERE dict_name IN ('#{language}_ispell', '#{language}_stem')
			SQL
			@dbi.execute <<-SQL
				DELETE FROM pg_ts_cfgmap
				WHERE ts_name='default_#{language}'
			SQL
		end
		def restore(odba_id)
			row = @dbi.select_one("SELECT content FROM object WHERE odba_id = ?", odba_id)
			row.first unless row.nil?
		end	
		def retrieve_connected_objects(target_id)
			sql = <<-SQL 
				SELECT origin_id FROM object_connection 
				WHERE target_id = ?
			SQL
			@dbi.select_all(sql, target_id)
		end
		def retrieve_from_fulltext_index(index_name, search_term, dict)
			term = search_term.gsub(/\s+/, '&').gsub(/[():]/i, 
				'\\ \\&').gsub(/\s/, '')
	    sql = <<-EOQ
				SELECT odba_id, content,
				max(rank(search_term, to_tsquery(?, ?))) AS relevance
				FROM object INNER JOIN #{index_name} 
				ON odba_id = #{index_name}.target_id 
				WHERE search_term @@ to_tsquery(?, ?) 
				GROUP BY odba_id, content
				ORDER BY relevance DESC
			EOQ
			@dbi.select_all(sql, dict, term, dict, term)
		rescue DBI::ProgrammingError => e
			warn("ODBA::Storage.retrieve_from_fulltext_index rescued a DBI::ProgrammingError(#{e.message}). Query:")
			warn("@dbi.select_all(#{sql}, #{dict}, #{term}, #{dict}, #{term})")
			warn("returning empty result")
			[]
		end
		def retrieve_from_index(index_name, search_term)
			search_term = search_term + "%"
			sql = <<-EOQ
				SELECT DISTINCT odba_id, content 
				FROM object 
				INNER JOIN #{index_name} 
				ON odba_id=#{index_name}.target_id 
				WHERE search_term LIKE ?
			EOQ
			rows = @dbi.select_all(sql, search_term.downcase)	 
		end
		def restore_named(name)
			row = @dbi.select_one("SELECT content FROM object WHERE name = ?", 
				name)
			row.first unless row.nil?
		end
		def restore_prefetchable
			rows = @dbi.select_all <<-EOQ
				SELECT odba_id, content FROM object WHERE prefetchable = true
			EOQ
			rows unless(rows.nil?)
		end
		def store(odba_id, dump, name, prefetchable)
			sth = @dbi.prepare("SELECT update_object(?, ?, ?, ?)")
			sth.execute(odba_id, dump, name, prefetchable)
		end
		def transaction(&block)
			@dbi.transaction(&block)
		end
		def update_fulltext_index(index_name, origin_id, search_term, target_id, dict)
			sth_insert = @dbi.prepare("INSERT INTO #{index_name} (origin_id, search_term, target_id) VALUES (?, to_tsvector(?, ? ), ?)")
			sth_insert.execute(origin_id, dict, search_term, target_id)
		end
		def update_index(index_name, origin_id, search_term, target_id)
			sth_insert = @dbi.prepare <<-SQL
				INSERT INTO #{index_name} (origin_id, search_term, target_id) 
				VALUES (?, ?, ?)
			SQL
			sth_insert.execute(origin_id, search_term.downcase, target_id)
		end
=begin
		def update(odba_id, dump, name, prefetchable)
			sth = @dbi.prepare("update object set content = ?, name = ?, prefetchable = ? where odba_id = ?")
			sth.execute(dump, name, prefetchable, odba_id)
			sth.rows
		end
=end
		private
		def ensure_next_id_set
			@id_mutex.synchronize {
				if(@next_id.nil?)
					@next_id = restore_max_id
				end
			}
		end
		def restore_max_id
			row = @dbi.select_one("select MAX(odba_id) from object")
			unless(row.first.nil?)
				row.first
			else
				0
			end
		end
	end
end
