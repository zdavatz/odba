CREATE OR REPLACE FUNCTION ensure_object_connection(integer, integer) 
RETURNS BOOLEAN AS '
DECLARE
	create BOOLEAN;
BEGIN
  PERFORM * FROM object_connection 
	WHERE origin_id=$1 AND target_id=$2;
	create := NOT FOUND;
	IF create THEN
		INSERT INTO object_connection (origin_id, target_id) VALUES ($1, $2);
	END IF;
	RETURN create;
END;
' LANGUAGE plpgsql;
