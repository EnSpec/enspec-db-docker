DROP TYPE if EXISTS rawdata_quality CASCADE;
CREATE TYPE rawdata_quality as ENUM ('Green', 'Yellow', 'Red');
