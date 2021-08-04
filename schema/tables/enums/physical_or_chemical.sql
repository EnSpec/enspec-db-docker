DROP TYPE if EXISTS physical_or_chemical CASCADE;
CREATE TYPE physical_or_chemical as ENUM ('Physical', 'Chemical');
