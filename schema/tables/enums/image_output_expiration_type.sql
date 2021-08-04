DROP TYPE if EXISTS image_output_expiration_type CASCADE;
CREATE TYPE image_output_expiration_type as ENUM ('reprocess', 'delete', 'glacier storage');
