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
			@store_mutex = Mutex.new
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
			sql	= "create table #{table_name} ( origin_id integer, search_term text, target_id integer )"
			@dbi.prepare(sql).execute
			#index search_term
			sql = "create index search_term_#{table_name} on #{table_name}(search_term)"
			@dbi.prepare(sql).execute
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
		def drop_index_table(name)
			sth = @dbi.prepare("drop table #{name}")
			sth.execute
		end
=begin
		def update_index(index_name, origin_id, search_term)
			sth = @dbi.prepare("update #{index_name} set search_term = ? where origin_id = ?")
			sth.execute(search_term, origin_id)
		end
=end
		def index_delete_origin(index_name, origin_id)
			sth = @dbi.prepare("delete from #{index_name}  where origin_id = ?")
			sth.execute(origin_id)
		end
		def index_delete_target(index_name, target_id)
			sth = @dbi.prepare("delete from #{index_name}  where target_id = ?")
			sth.execute(target_id)
		end
		def remove_dead_connections(min_id, max_id)
			
			sth = @dbi.prepare <<-EOQ
			DELETE FROM object 
			WHERE odba_id IN ( 
				SELECT object.odba_id FROM object_connection 
				RIGHT JOIN object ON 
					(object.odba_id BETWEEN #{min_id} AND #{max_id})
				AND ((object_connection.origin_id 
					BETWEEN #{min_id} AND #{max_id}) 
				OR (object_connection.target_id 
					BETWEEN #{min_id} AND #{max_id})) 
				AND ((object_connection.origin_id = object.odba_id) 
				OR (object_connection.target_id = object.odba_id)) 
				WHERE target_id IS null
			)
			AND odba_id BETWEEN #{min_id} AND #{max_id}
			EOQ
			sth.execute
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
				DELETE FROM object WHERE odba_id IN
				(
					SELECT object.odba_id
					FROM object_connection 
					RIGHT JOIN object 
					ON (object_connection.target_id = object.odba_id 
						OR object_connection.origin_id = object.odba_id) 
					WHERE	target_id IS NULL
					AND object.odba_id BETWEEN ? AND ?
				)
			EOQ
			sth.execute(min_id, max_id)
=begin
			unless(rows.first.nil?)
				sth = @dbi.prepare("delete from object where odba_id in (#{rows.join(',')})");
				sth.execute
			end
=end
		end
		def update_index(index_name, origin_id, search_term, target_id)
			sth_insert = @dbi.prepare("insert into #{index_name} (origin_id, search_term, target_id) values (?, ?, ?)")
			sth_insert.execute(origin_id, search_term, target_id)
		end
		def update(odba_id, dump, name, prefetchable)
			sth = @dbi.prepare("update object set content = ?, name = ?, prefetchable = ? where odba_id = ?")
			sth.execute(dump, name, prefetchable, odba_id)
			sth.rows
		end
		def restore(odba_id)
			#	puts "storage loading #{odba_id}"
			row = @dbi.select_one("select content from object where odba_id = ?", odba_id)
			row.first unless row.nil?
		end	
		def retrieve_from_index(index_name, search_term)
			search_term = search_term+"%"
			rows = @dbi.select_all("select distinct odba_id, content from object inner join #{index_name} on odba_id = #{index_name}.target_id where search_term ilike ?", search_term)	 
		end
		def retrieve_connected_objects(target_id)
			@dbi.select_all("select origin_id from object_connection where target_id = ?", target_id)
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
		def store(odba_id, dump, name, prefetchable)
			@store_mutex.synchronize {	
				if(update(odba_id, dump, name, prefetchable) == 0)
					sth = @dbi.prepare("insert into object (odba_id, content, name, prefetchable) VALUES (?, ?, ?, ?)")
					sth.execute(odba_id, dump, name, prefetchable)
				end
			}
		end
		def max_id
			ensure_next_id_set
			@next_id
		end
		def next_id
			ensure_next_id_set
			@next_id += 1
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
