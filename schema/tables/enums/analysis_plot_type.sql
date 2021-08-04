DROP TYPE if EXISTS analysis_plot_type CASCADE;
CREATE TYPE analysis_plot_type as ENUM ('Quadrat', 'Transect', 'Tree canopy');
