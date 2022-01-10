-- TABLE
DROP TABLE IF EXISTS flight_session_site_dsm CASCADE;
CREATE TABLE flight_session_site_dsm (
  flight_session_site_dsm_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  source_id UUID REFERENCES source NOT NULL,
  flights_id UUID REFERENCES flights NOT NULL,
  sessions_id UUID REFERENCES sessions NOT NULL,
  dsm_id UUID REFERENCES DSM NOT NULL,
  site_id UUID REFERENCES study_sites
);
CREATE INDEX flight_session_site_dsm_source_id_idx ON flight_session_site_dsm(source_id);
CREATE INDEX flight_session_site_dsm_flights_id_idx ON flight_session_site_dsm(flights_id);
CREATE INDEX flight_session_site_dsm_sessions_id_idx ON flight_session_site_dsm(sessions_id);
CREATE INDEX flight_session_site_dsm_dsm_id_idx ON flight_session_site_dsm(dsm_id);
CREATE INDEX flight_session_site_dsm_site_id_idx ON flight_session_site_dsm(site_id);

-- VIEW
CREATE OR REPLACE VIEW flight_session_site_dsm_view AS
  SELECT
    f.flight_session_site_dsm_id AS flight_session_site_dsm_id,
    fl.flight_date AS flight_date,
    fl.pilot AS pilot,
    fl.operator AS operator,
    fl.flight_hours AS flight_hours,
    fl.start_time AS flight_start_time,
    fl.end_time AS flight_end_time,
    fl.flight_notes AS flight_notes,
    s.session_name AS session_name,
    s.start_time AS session_start_time,
    s.end_time AS session_end_time,
    s.line_count AS line_count,
    s.bad_lines AS bad_lines,
    s.session_notes AS session_notes,
    ss.site_name AS site_name,
    ss.region AS region,
    ST_AsKML(ss.site_poly)  as site_poly_kml,
    d.dsm_name AS dsm_name,
    ST_AsKML(d.extent_poly) AS extent_poly_kml,
    d.epsg AS epsg,
    d.vdatum AS vdatum,
    d.dsm_file AS dsm_file,
    d.dsm_metadata AS dsm_metadata,
    sc.name AS source_name
  FROM
    flight_session_site_dsm f
LEFT JOIN source sc ON f.source_id = sc.source_id
LEFT JOIN flights fl ON f.flights_id = fl.flights_id
LEFT JOIN sessions s ON f.sessions_id = s.sessions_id
LEFT JOIN study_sites ss ON f.site_id = ss.study_sites_id
LEFT JOIN dsm d ON f.dsm_id = d.dsm_id;

-- FUNCTIONS
CREATE OR REPLACE FUNCTION insert_flight_session_site_dsm (
  flight_session_site_dsm_id UUID,
  flight_date DATE,
  pilot TEXT,
  operator TEXT,
  flight_hours FLOAT,
  flight_start_time TIME,
  flight_end_time TIME,
  flight_notes TEXT,
  session_name TEXT,
  session_start_time TIME,
  session_end_time TIME,
  line_count FLOAT,
  bad_lines TEXT,
  session_notes TEXT,
  site_name TEXT,
  region TEXT,
  site_poly_kml TEXT,
  dsm_name TEXT,
  extent_poly_kml TEXT,
  epsg FLOAT,
  vdatum TEXT,
  dsm_file TEXT,
  dsm_metadata TEXT,
  source_name TEXT) RETURNS void AS $$
DECLARE
  source_id UUID;
  flid UUID;
  s_id UUID;
  ss_id UUID;
  d_id UUID;
BEGIN

  IF( flight_session_site_dsm_id IS NULL ) THEN
    SELECT uuid_generate_v4() INTO flight_session_site_dsm_id;
  END IF;
  SELECT get_source_id(source_name) INTO source_id;
  SELECT get_flights_id(flights_date, pilot, operator, flight_hours, flight_start_time, flight_end_time, flight_notes) INTO flid;
  SELECT get_sessions_id(session_name, session_start_time, session_end_time, line_count, bad_lines, session_notes) INTO s_id;
  SELECT get_study_sites_id(site_name, region, site_poly_kml) INTO ss_id;
  SELECT get_dsm_id(dsm_name, extent_poly_kml, epsg, vdatum, dsm_file, dsm_metadata) INTO d_id;

  INSERT INTO flight_session_site_dsm (
    flight_session_site_dsm_id, flights_id, sessions_id, dsm_id, site_id, source_id
  ) VALUES (
    flight_session_site_dsm_id, flid, s_id, d_id, ss_id, source_id
  );

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_flight_session_site_dsm (
  flight_session_site_dsm_id_in UUID,
  flight_date_in DATE,
  pilot_in TEXT,
  operator_in TEXT,
  flight_hours_in FLOAT,
  flight_start_time_in TIME,
  flight_end_time_in TIME,
  flight_notes_in TIME,
  session_name_in TEXT,
  session_start_time_in TIME,
  session_end_time_in TIME,
  line_count_in FLOAT,
  bad_lines_in TEXT,
  session_notes_in TEXT,
  site_name_in TEXT,
  region_in TEXT,
  site_poly_kml_in TEXT,
  dsm_name_in TEXT,
  extent_poly_kml_in TEXT,
  epsg_in FLOAT,
  vdatum_in TEXT,
  dsm_file_in TEXT,
  dsm_metadata_in TEXT) RETURNS void AS $$
DECLARE
  flid UUID;
  s_id UUID;
  ss_id UUID;
  d_id UUID;
BEGIN
  SELECT get_flights_id(flights_date_in, pilot_in, operator_in, flight_hours_in, flight_start_time_in, flight_end_time_in, flight_notes_in) INTO flid;
  SELECT get_sessions_id(session_name_in, session_start_time_in, session_end_time_in, line_count_in, bad_lines_in, session_notes_in) INTO s_id;
  SELECT get_study_sites_id(site_name_in, region_in, site_poly_kml_in) INTO ss_id;
  SELECT get_dsm_id(dsm_name_in, extent_poly_kml_in, epsg_in, vdatum_in, dsm_file_in, dsm_metadata_in) INTO d_id;
  UPDATE flight_session_site_dsm SET (
    flights_id, sessions_id, dsm_id, site_id
  ) = (
    flid, s_id, d_id, ss_id
  ) WHERE
    flight_session_site_dsm_id = flight_session_site_dsm_id_in;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION TRIGGERS
CREATE OR REPLACE FUNCTION insert_flight_session_site_dsm_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM insert_flight_session_site_dsm(
    flight_session_site_dsm_id := NEW.flight_session_site_dsm_id,
    flight_date := NEW.flight_date,
    pilot := NEW.pilot,
    operator := NEW.operator,
    flight_hours := NEW.flight_hours,
    flight_start_time := NEW.flight_start_time,
    flight_end_time := NEW.flight_end_time,
    flight_notes := NEW.flight_notes,
    session_name := NEW.session_name,
    session_start_time := NEW.session_start_time,
    session_end_time := NEW.session_end_time,
    line_count := NEW.line_count,
    bad_lines := NEW.bad_lines,
    session_notes := NEW.session_notes,
    site_name := NEW.site_name,
    region := NEW.region,
    site_poly_kml := NEW.site_poly_kml,
    dsm_name := NEW.dsm_name,
    extent_poly_kml := NEW.extent_poly_kml,
    epsg := NEW.epsg,
    vdatum := NEW.vdatum,
    dsm_file := NEW.dsm_file,
    dsm_metadata := NEW.dsm_metadata,
    source_name := NEW.source_name
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_flight_session_site_dsm_from_trig()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM update_flight_session_site_dsm(
    flight_session_site_dsm_id_in := NEW.flight_session_site_dsm_id,
    flight_date_in := NEW.flight_date,
    pilot_in := NEW.pilot,
    operator_in := NEW.operator,
    flight_hours_in := NEW.flight_hours,
    flight_start_time_in := NEW.flight_start_time,
    flight_end_time_in := NEW.flight_end_time,
    flight_notes_in := NEW.flight_notes,
    session_name_in := NEW.session_name,
    session_start_time_in := NEW.session_start_time,
    session_end_time_in := NEW.session_end_time,
    line_count_in := NEW.line_count,
    bad_lines_in := NEW.bad_lines,
    session_notes_in := NEW.session_notes,
    site_name_in := NEW.site_name,
    region_in := NEW.region,
    site_poly_kml_in := NEW.site_poly_kml,
    dsm_name_in := NEW.dsm_name,
    extent_poly_kml_in := NEW.extent_poly_kml,
    epsg_in := NEW.epsg,
    vdatum_in := NEW.vdatum,
    dsm_file_in := NEW.dsm_file,
    dsm_metadata_in := NEW.dsm_metadata
  );
  RETURN NEW;

EXCEPTION WHEN raise_exception THEN
  RAISE;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION GETTER
CREATE OR REPLACE FUNCTION get_flight_session_site_dsm_id(
  flight_date date,
  pilot text,
  operator text,
  flight_hours float,
  flight_start_time time,
  flight_end_time time,
  flight_notes text,
  session_name text,
  session_start_time time,
  session_end_time time,
  line_count float,
  bad_lines text,
  session_notes text,
  site_name text,
  region text,
  site_poly_kml TEXT,
  dsm_name text,
  extent_poly_kml TEXT,
  epsg float,
  vdatum text,
  dsm_file text,
  dsm_metadata text
) RETURNS UUID AS $$
DECLARE
  fid UUID;
  flid UUID;
  s_id UUID;
  d_id UUID;
  ss_id UUID;
BEGIN
  SELECT get_flights_id(flights_date, pilot, operator, flight_hours, flight_start_time, flight_end_time, flight_notes) INTO flid;
  SELECT get_sessions_id(session_name, session_start_time, session_end_time, line_count, bad_lines, session_notes) INTO s_id;
  SELECT get_study_sites_id(site_name, region, site_poly_kml) INTO ss_id;
  SELECT get_dsm_id(dsm_name, extent_poly_kml, epsg, vdatum, dsm_file, dsm_metadata) INTO d_id;
  SELECT
    flight_session_site_dsm_id INTO fid
  FROM
    flight_session_site_dsm f
  WHERE
    flights_id = flid AND
    sessions_id = s_id AND
    site_id = ss_id AND
    dsm_id = d_id;
  IF (fid IS NULL) THEN
    RAISE EXCEPTION 'Unknown flight_session_site_dsm: flights_date="%" pilot="%" operator="%" flight_hours="%" flight_start_time="%" flight_end_time="%" flight_notes="%" session_name="%"
     session_start_time="%" session_end_time="%" line_count="%" bad_lines="%" session_notes="%" site_name="%" region="%" site_poly_kml="%" dsm_name="%" extent_poly_kml="%" epsg="%" vdatum="%" dsm_file="%" dsm_metadata="%"',
    flights_date, pilot, operator, flight_hours, flight_start_time, flight_end_time, flight_notes, session_name, session_start_time, session_end_time,
    line_count, bad_lines, session_notes, site_name, region, site_poly_kml, dsm_name, extent_poly_kml, epsg, vdatum, dsm_file, dsm_metadata;
  END IF;

  RETURN fid;
END ;
$$ LANGUAGE plpgsql;

-- RULES
CREATE TRIGGER flight_session_site_dsm_insert_trig
  INSTEAD OF INSERT ON
  flight_session_site_dsm_view FOR EACH ROW
  EXECUTE PROCEDURE insert_flight_session_site_dsm_from_trig();

CREATE TRIGGER flight_session_site_dsm_update_trig
  INSTEAD OF UPDATE ON
  flight_session_site_dsm_view FOR EACH ROW
  EXECUTE PROCEDURE update_flight_session_site_dsm_from_trig();
