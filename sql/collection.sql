CREATE TABLE collection (
		odba_id integer NOT NULL,
		key text,
		value text,
		PRIMARY KEY(odba_id, key)
);
CREATE INDEX collection_odba_id_key_index ON collection (odba_id, key);
