CREATE TABLE object (
		odba_id integer NOT NULL,
		content text,
		name text,
		prefetchable boolean,
		PRIMARY KEY(odba_id),
		UNIQUE(name)
);
