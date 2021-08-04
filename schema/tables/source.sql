-- TABLE
DROP TABLE IF EXISTS source CASCADE;
CREATE TABLE source (
  source_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name TEXT NOT NULL UNIQUE,
  revision INTEGER NOT NULL,
  table_view text REFERENCES tables
);

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_source_id(source_name text) RETURNS UUID AS $$   
DECLARE
  sid UUID;
BEGIN
  select source_id into sid from source where name = source_name;

  if (sid is NULL) then
    RAISE EXCEPTION 'Unknown source: %', source_name;
  END IF;
  
  RETURN sid;
END ; 
$$ LANGUAGE plpgsql;