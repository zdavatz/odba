CREATE OR REPLACE FUNCTION update_object(integer, text, text, boolean) 
RETURNS BOOLEAN AS '
DECLARE 
	v_old_name text;
	v_new_name text;
BEGIN
	SELECT INTO v_old_name name FROM object WHERE odba_id = $1;
	IF FOUND THEN
		IF $3 IS NULL THEN
			v_new_name := v_old_name;
		ELSE
			v_new_name := $3;
		END IF;
		UPDATE object 
		SET content = $2, name = v_new_name, prefetchable = $4
		WHERE odba_id = $1;
	ELSE
		INSERT INTO object (odba_id, content, name, prefetchable)
		VALUES ($1, $2, $3, $4);
	END IF;
	RETURN FOUND;
END;
' LANGUAGE plpgsql;
