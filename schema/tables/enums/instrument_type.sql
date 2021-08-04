DROP TYPE if EXISTS instrument_type CASCADE;
CREATE TYPE instrument_type as ENUM ('GPS', 'imaging spectrometer', 'point spectrometer', 'INS', 'LiDAR');
