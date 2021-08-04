DROP TYPE if EXISTS tree_canopy_level CASCADE;
CREATE TYPE tree_canopy_level as ENUM ('Canopy(C)', 'Subcanopy(S)', 'Understory(U)', 'Ground');
