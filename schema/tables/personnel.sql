-- TABLE
DROP TABLE IF EXISTS personnel CASCADE;
DROP FUNCTION insert_personnel(uuid,text,text,text,text,text,text,text,text);
DROP FUNCTION update_personnel(uuid,text,text,text,text,text,text,text);
DROP FUNCTION get_personnel_id(text,text,text,text,text,text,text);

CREATE TABLE personnel (
  personnel_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  personnel_name TEXT UNIQUE NOT NULL,
  personnel_role TEXT NOT NULL,
  organization TEXT NOT NULL,
  office_phone TEXT NOT NULL,
  cell_phone TEXT NOT NULL,
  email TEXT UNIQUE NOT NULL,
  personnel_notes TEXT
);
CREATE INDEX personnel_source_id_idx ON personnel(source_id);

-- VIEW
CREATE OR REPLACE VIEW personnel_view AS
  SELECT
    p.personnel_id AS personnel_id,
    p.personnel_name  as personnel_name,
    p.personnel_role  as personnel_role,
    p.organization  as organization,
    p.office_phone  as office_phone,
    p.cell_phone  as cell_phone,
    p.email  as email,
    p.personnel_notes  as personnel_notes,

    sc.name AS source_name
  FROM
    personnel p
LEFT JOIN source sc ON p.source_id = sc.source_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_personnel (
  personnel_id UUID,
  personnel_name TEXT,
  personnel_role TEXT,
  organization TEXT,
  office_phone TEXT,
  cell_phone TEXT,
  email TEXT,
  personnel_notes TEXT,

  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
BEGIN

  IF( personnel_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO personnel_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;

  INSERT INTO personnel (
    personnel_id, personnel_name, personnel_role, organization, office_phone, cell_phone, email, personnel_notes, source_id
  ) VALUES (
    personnel_id, personnel_name, personnel_role, organization, office_phone, cell_phone, email, personnel_notes, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_personnel (
  personnel_id_in UUID,
  personnel_name_in TEXT,
  personnel_role_in TEXT,
  organization_in TEXT,
  office_phone_in TEXT,
  cell_phone_in TEXT,
  email_in TEXT,
  personnel_notes_in TEXT) RETURNS void AS $$
BEGIN

  UPDATE personnel SET (
    personnel_name, personnel_role, organization, office_phone, cell_phone, email, personnel_notes
  ) = (
    personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in
  ) WHERE
    personnel_id = personnel_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_personnel(
    personnel_id := NEW.personnel_id,
    personnel_name := NEW.personnel_name,
    personnel_role := NEW.personnel_role,
    organization := NEW.organization,
    office_phone := NEW.office_phone,
    cell_phone := NEW.cell_phone,
    email := NEW.email,
    personnel_notes := NEW.personnel_notes,

    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_personnel_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_personnel(
    personnel_id_in := NEW.personnel_id,
    personnel_name_in := NEW.personnel_name,
    personnel_role_in := NEW.personnel_role,
    organization_in := NEW.organization,
    office_phone_in := NEW.office_phone,
    cell_phone_in := NEW.cell_phone,
    email_in := NEW.email,
    personnel_notes_in := NEW.personnel_notes
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_personnel_id(
  personnel_name_in TEXT,
  personnel_role_in TEXT,
  organization_in TEXT,
  office_phone_in TEXT,
  cell_phone_in TEXT,
  email_in TEXT,
  personnel_notes_in TEXT
) RETURNS UUID AS $$
DECLARE
  pid UUID;
BEGIN

  IF personnel_notes_in IS NULL THEN
    SELECT
      personnel_id INTO pid
    FROM
      personnel p
    WHERE
    personnel_name = personnel_name_in AND
    personnel_role = personnel_role_in AND
    organization = organization_in AND
    office_phone = office_phone_in AND
    cell_phone = cell_phone_in AND
    email = email_in AND
    personnel_notes IS NULL;
  ELSE
    SELECT
      personnel_id INTO pid
    FROM
      personnel p
    WHERE
    personnel_name = personnel_name_in AND
    personnel_role = personnel_role_in AND
    organization = organization_in AND
    office_phone = office_phone_in AND
    cell_phone = cell_phone_in AND
    email = email_in AND
    personnel_notes = personnel_notes_in;
  END IF;

  IF (pid IS NULL) THEN
    RAISE EXCEPTION 'Unknown personnel: personnel_name="%" personnel_role="%" organization="%" office_phone="%" cell_phone="%" email="%" personnel_notes="%"',
    personnel_name_in, personnel_role_in, organization_in, office_phone_in, cell_phone_in, email_in, personnel_notes_in;
  END IF;

  RETURN pid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER personnel_insert_trig
  INSTEAD OF INSERT ON
  personnel_view FOR EACH ROW
  EXECUTE PROCEDURE insert_personnel_from_trig();

CREATE TRIGGER personnel_update_trig
  INSTEAD OF UPDATE ON
  personnel_view FOR EACH ROW
  EXECUTE PROCEDURE update_personnel_from_trig();
