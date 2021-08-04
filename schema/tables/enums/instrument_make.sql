DROP TYPE if EXISTS instrument_make CASCADE;
CREATE TYPE instrument_make as ENUM ('HySpex', 'Headwall', 'Velodyne', 'Ocean Optics', 'Spectra Vista', 'ASD', 'Spectral Evolution', 'iMAR', 'Trimble', 'Emlid', 'Garmin');
