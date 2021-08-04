DROP TYPE if EXISTS analysis_plot_physiognomic_type CASCADE;
CREATE TYPE analysis_plot_physiognomic_type as ENUM ('Deciduous forest', 'Coniferous forest', 'Tundra', 'Shrubland', 'Wetland', 'Grassland');
