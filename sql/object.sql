CREATE TABLE object (
		odba_id integer NOT NULL,
		content text,
		name text,
		prefetchable boolean,
		PRIMARY KEY(odba_id),
		UNIQUE(name)
		);
CREATE TABLE object_connection (
		origin_id integer,
		target_id integer,
		PRIMARY KEY(origin_id, target_id)
		);
CREATE INDEX target_id_index ON object_connection (target_id);
CREATE INDEX origin_id_index ON object_connection (origin_id);
