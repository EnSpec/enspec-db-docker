DROP TYPE if EXISTS species_code_type CASCADE;
CREATE TYPE species_code_type as ENUM ('FIA', 'Custom');
