CREATE OR REPLACE FUNCTION update_object(integer, text, text, boolean) 
RETURNS BOOLEAN AS '
BEGIN
	PERFORM odba_id FROM object WHERE odba_id = $1;
	IF FOUND THEN
		UPDATE object 
		SET content = $2, name = $3, prefetchable = $4
		WHERE odba_id = $1;
	ELSE
		INSERT INTO object (odba_id, content, name, prefetchable)
		VALUES ($1, $2, $3, $4);
	END IF;
	RETURN FOUND;
END;
' LANGUAGE plpgsql;
