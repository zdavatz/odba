#!/usr/bin/env ruby

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
		def generate_german_dictionary
			language = "german"	
			@dbi.execute("INSERT INTO 
      pg_ts_cfg (ts_name, prs_name, locale)
			VALUES ('default_german', 'default', 'de_DE@euro')")
			sth = @dbi.prepare("INSERT INTO pg_ts_dict(SELECT 'german_ispell',dict_init,?,dict_lexize FROM pg_ts_dict WHERE dict_name = 'ispell_template')")
			aff = "AffFile=\"/var/www/oddb.org/ext/fulltext/swiss/swiss.aff\","
			dict = "DictFile=\"/var/www/oddb.org/ext/fulltext/swiss/swiss.med\","
			stop = "StopFile=\"/var/www/oddb.org/ext/fulltext/swiss/swiss.stop\""
			path = dict << aff << stop
			puts "path:"
			puts path
			sth.execute(path)
			create_dictionary_map(language)
			sql = " INSERT INTO pg_ts_dict (dict_name,dict_init,dict_initoption, dict_lexize, dict_comment) VALUES('german_stem','snb_en_init(text)','/var/www/oddb.org/ext/fulltext/swiss/swiss.stop','snb_lexize(internal,internal,integer)','german stem')"
			@dbi.execute(sql)
		end
		def create_dictionary_map(language)
			sql = "INSERT INTO pg_ts_cfgmap (ts_name, tok_alias, dict_name)VALUES ('default_#{language}', 'lhword','{#{language}_ispell,#{language}_stem}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap (ts_name, tok_alias, dict_name) VALUES ('default_#{language}', 'lpart_hword','{#{language}_ispell,#{language}_stem}')"
			@dbi.execute(sql)
			sql ="INSERT INTO pg_ts_cfgmap (ts_name, tok_alias, dict_name) VALUES ('default_#{language}', 'lword', '{#{language}_ispell,#{language}_stem}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'url', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'host', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'sfloat', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'uri', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'int', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'float', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'email', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'word', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'hword', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'nlword', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'nlpart_hword', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'part_hword', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'nlhword', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'file', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'uint', '{simple}')"
			@dbi.execute(sql)
			sql = "INSERT INTO pg_ts_cfgmap VALUES ('default_#{language}', 'version', '{simple}')"
			@dbi.execute(sql)
		end
		def generate_french_dictionary
			language = "french"
			@dbi.execute("INSERT INTO 
      pg_ts_cfg (ts_name, prs_name, locale)
			VALUES ('default_french', 'default', 'fr_FR@euro')")
			sth = @dbi.prepare("INSERT INTO pg_ts_dict(SELECT 'french_ispell',dict_init,?,dict_lexize FROM pg_ts_dict WHERE dict_name = 'ispell_template')")
			aff = "AffFile=\"/var/www/oddb.org/ext/fulltext/french/french.aff\","
			dict = "DictFile=\"/var/www/oddb.org/ext/fulltext/french/french.med\","
			stop = "StopFile=\"/var/www/oddb.org/ext/fulltext/french/french.stop\""
			path = dict << aff << stop
			sth.execute(path)
			create_dictionary_map(language)
			sql = " INSERT INTO pg_ts_dict (dict_name,dict_init,dict_initoption,dict_lexize,dict_comment) VALUES('french_stem','snb_en_init(text)','/var/www/oddb.org/ext/fulltext/french/french.stop','snb_lexize(internal,internal,integer)','french stem')"
			@dbi.execute(sql)
		end
		def add_object_connection(origin_id, target_id)
			#SELECT
			rows = @dbi.select_all("select count(origin_id) from object_connection where origin_id = ? and target_id = ?", origin_id, target_id)
			if(rows.first.first == 0)
				#INSERT
				sth = @dbi.prepare("insert into object_connection (origin_id, target_id) values (?,?)")
				sth.execute(origin_id, target_id)	
			end
		end
		def bulk_restore(bulk_fetch_ids)
			if(bulk_fetch_ids.empty?)
				[]
			else
				sql = "select odba_id, content from object where odba_id in (#{bulk_fetch_ids.join(',')})"
				@dbi.select_all(sql)
			end
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
		def db_type
			"mysql"
		end
		def drop_index(index_name)
			sth = @dbi.prepare("DROP TABLE #{index_name}")
			sth.execute
		end
		def delete_index_element(index_name, odba_id)
		sth = @dbi.prepare("delete from #{index_name} where origin_id = ?")
			sth.execute(odba_id)
		end
		def delete_persistable(odba_id)
			sth = @dbi.prepare("delete from object where odba_id = ?")
			sth.execute(odba_id)
		end
		def index_delete_origin(index_name, origin_id)
			sth = @dbi.prepare("delete from #{index_name}  where origin_id = ?")
			sth.execute(origin_id)
		end
		def index_delete_target(index_name, target_id)
			sth = @dbi.prepare("delete from #{index_name}  where target_id = ?")
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
			sth.execute(min_id, max_id, min_id, max_id)
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
			sth.execute(min_id, max_id, min_id, max_id,
				min_id, max_id, min_id, max_id)
=begin
			unless(rows.first.nil?)
				sth = @dbi.prepare("delete from object where odba_id in (#{rows.join(',')})");
				sth.execute
			end
=end
		end
		def restore(odba_id)
			#	puts "storage loading #{odba_id}"
			row = @dbi.select_one("SELECT content FROM object WHERE odba_id = ?", odba_id)
			row.first unless row.nil?
		end	
		def retrieve_connected_objects(target_id)
			@dbi.select_all("select origin_id from object_connection where target_id = ?", target_id)
		end
		def retrieve_from_fulltext_index(index_name, search_term, dict)
			search_term.gsub!(/ /,"&")
	    sql = <<-EOQ
			SELECT odba_id, content,
			max(rank(search_term, to_tsquery(?, ?))) AS relevance
			FROM object INNER JOIN #{index_name} 
			on odba_id = #{index_name}.target_id 
			WHERE search_term @@ to_tsquery(?, ?) 
			GROUP BY odba_id, content
			ORDER BY relevance DESC"
			EOQ
			@dbi.select_all(sql, dict, search_term, dict, search_term)
		end
		def retrieve_from_index(index_name, search_term)
			search_term = search_term+"%"
			rows = @dbi.select_all("select distinct odba_id, content from object inner join #{index_name} on odba_id = #{index_name}.target_id where lower(search_term) like ?", search_term.downcase)	 
		end
		def restore_named(name)
			#			puts "storage loading #{name}"
			row = @dbi.select_one("select content from object where name = ?", name)
			row.first unless row.nil?
		end
		def restore_prefetchable
			sql = "select odba_id, content from object where prefetchable = true"
			rows = @dbi.select_all(sql)
			[]
			rows unless(rows.nil?)
		end
		#it is a test method
		def search_indication(index_name, search)
			@dbi.select_all("SELECT origin_id FROM #{index_name} WHERE MATCH (search_term) AGAINST (?)", search);
		end
		def store(odba_id, dump, name, prefetchable)
			if(update(odba_id, dump, name, prefetchable) == 0)
				sth = @dbi.prepare("insert into object (odba_id, content, name, prefetchable) VALUES (?, ?, ?, ?)")
				sth.execute(odba_id, dump, name, prefetchable)
			end
		end

		def transaction(&block)
			@dbi.transaction(&block)
		end
		def update_fulltext_index(index_name, origin_id, search_term, target_id, dict)
			sth_insert = @dbi.prepare("INSERT INTO #{index_name} (origin_id, search_term, target_id) VALUES (?, to_tsvector(?, ? ), ?)")
			sth_insert.execute(origin_id, dict, search_term, target_id)
		end
		def update_index(index_name, origin_id, search_term, target_id)
			puts "updating index  with:"
			puts "*******"
			puts search_term
			puts "***********"
			sth_insert = @dbi.prepare("INSERT INTO #{index_name} (origin_id, search_term, target_id) VALUES (?, ?, ?)")
			sth_insert.execute(origin_id, search_term, target_id)
		end
		def update(odba_id, dump, name, prefetchable)
			sth = @dbi.prepare("update object set content = ?, name = ?, prefetchable = ? where odba_id = ?")
			sth.execute(dump, name, prefetchable, odba_id)
			sth.rows
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
			row = @dbi.select_one("select MAX(odba_id) from object")
			unless(row.first.nil?)
				row.first
			else
				0
			end
		end
	end
end
