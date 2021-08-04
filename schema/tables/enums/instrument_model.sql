DROP TYPE if EXISTS instrument_model CASCADE;
CREATE TYPE instrument_model as ENUM ('VNIR-1800', 'SWIR-384', 'Mjolnir VS-620', 'Nano', 'Puck', 'HR-1024i', 'FieldSpec 4', 'PSR+', 'iTraceRT-F400', 'Geo7x', 'RS+');
