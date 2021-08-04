#! /bin/bash

# Never run this unless you mean to.
# exit -1; -- Uncomment after database has data in it

# Note: extentions may need to be added by IT admin
SCHEMA=enspec;
psql -c "CREATE SCHEMA IF NOT EXISTS $SCHEMA;"
psql -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"
psql -c "CREATE EXTENSION IF NOT EXISTS postgis;"
psql -c "CREATE EXTENSION IF NOT EXISTS postgis_topology;"
# psql -c "alter extension \"uuid-ossp\" set schema public;"

# Note: you can make add enspec to default search path, makes it easier on your users
export PGOPTIONS=--search_path=$SCHEMA,public

# types
psql -f ./tables/enums/instrument_make.sql
psql -f ./tables/enums/instrument_model.sql
psql -f ./tables/enums/instrument_type.sql
psql -f ./tables/enums/rawdata_quality.sql
psql -f ./tables/enums/image_output_expiration_type.sql
psql -f ./tables/enums/analysis_plot_physiognomic_type.sql
psql -f ./tables/enums/analysis_plot_type.sql
psql -f ./tables/enums/tree_canopy_level.sql
psql -f ./tables/enums/sample_storage.sql
psql -f ./tables/enums/branchposition.sql
psql -f ./tables/enums/branchexposure.sql
psql -f ./tables/enums/species_code_type.sql
psql -f ./tables/enums/fresh_or_dry.sql   #for spectra table
psql -f ./tables/enums/physical_or_chemical.sql


# 3rd Party GIS DATA example
# psql -c "DROP TABLE IF EXISTS $SCHEMA.gis_table CASCADE";
# shp2pgsql ../data/gis_data.shp "$SCHEMA.gis_data" | psql
# psql -c "UPDATE $SCHEMA.gis_data set geom = ST_Transform(ST_SetSRID(geom, 3857),4326);";

# tables
psql -f ./tables/tables.sql
psql -f ./tables/source.sql

psql -f ./tables/instruments.sql
psql -f ./tables/specifications.sql;
psql -f ./tables/instrument_specification.sql;
psql -f ./tables/platform.sql
psql -f ./tables/calibration.sql
psql -f ./tables/installations.sql
psql -f ./tables/instrument_install.sql
psql -f ./tables/platform_install.sql
psql -f ./tables/flights.sql
psql -f ./tables/flight_installation.sql
psql -f ./tables/sessions.sql
psql -f ./tables/study_sites.sql
psql -f ./tables/dsm.sql
psql -f ./tables/flight_session_site_dsm.sql
psql -f ./tables/rawdata.sql
psql -f ./tables/variables.sql
psql -f ./tables/units.sql
psql -f ./tables/sessions_metadata.sql
psql -f ./tables/rawdata_metadata.sql
psql -f ./tables/boresight_offsets.sql
psql -f ./tables/boresight_rawdata_dsm.sql

psql -f ./tables/projects.sql
psql -f ./tables/site_projects.sql

psql -f ./tables/image_output.sql
psql -f ./tables/processing_events.sql
psql -f ./tables/workflow_chtc.sql
psql -f ./tables/rawdata_processing.sql
psql -f ./tables/rawdata_image_output.sql
psql -f ./tables/processing_metadata.sql
psql -f ./tables/processing_output_workflow.sql

psql -f ./tables/analysis_plot.sql
psql -f ./tables/tree_data.sql
psql -f ./tables/quadrats.sql
psql -f ./tables/samples.sql
psql -f ./tables/branch_data.sql
psql -f ./tables/species.sql
psql -f ./tables/species_code.sql
psql -f ./tables/species_speciescode.sql
psql -f ./tables/observations.sql
psql -f ./tables/radiometric_calibration_target.sql
psql -f ./tables/geometric_calibration_points.sql

psql -f ./tables/plot_quadrats.sql
psql -f ./tables/plot_tree_data.sql
psql -f ./tables/tree_branch_data.sql
psql -f ./tables/quadrats_samples.sql
psql -f ./tables/samples_species.sql
psql -f ./tables/branch_samples.sql
psql -f ./tables/tree_species.sql

psql -f ./tables/quadrat_observations.sql
psql -f ./tables/plot_observations.sql
psql -f ./tables/tree_observations.sql
psql -f ./tables/branch_observations.sql
psql -f ./tables/rct_site_spectrometer.sql
psql -f ./tables/gcp_site_device.sql
psql -f ./tables/AnalysisPlot_StudySites.sql


psql -f ./tables/spectra.sql
psql -f ./tables/measurements.sql
psql -f ./tables/labs.sql
psql -f ./tables/samples_measurements.sql
psql -f ./tables/sample_spectra.sql


psql -f ./tables/personnel.sql
psql -f ./tables/activity.sql
psql -f ./tables/plot_personnel.sql
psql -f ./tables/sessions_personnel.sql
psql -f ./tables/installations_personnel.sql
psql -f ./tables/studysites_personnel.sql
psql -f ./tables/projects_personnel.sql
psql -f ./tables/sample_personnel.sql


# Add permissions.  This grants anyone who has access to the database r/w access to tables
# psql -c "grant usage on schema $SCHEMA to public;"
# psql -c "grant all on all tables in schema $SCHEMA to public;"
# psql -c "grant execute on all functions in schema $SCHEMA to public;"
# psql -c "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA $SCHEMA TO public;"

# grant usage on schema grain to public;
# grant all on all tables in schema grain to public;
# grant execute on all functions in schema grain to public;
# GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA grain TO public;
# grant all on growing_degree_days_view  to public;
