CREATE TABLE collection (
		odba_id integer NOT NULL,
		key text,
		value text,
		PRIMARY KEY(odba_id, key)
);
