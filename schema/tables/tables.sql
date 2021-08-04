-- TABLES
DROP TABLE IF EXISTS tables CASCADE;
CREATE TABLE tables (
  table_view TEXT PRIMARY KEY,
  uid TEXT NOT NULL,
  name TEXT NOT NULL UNIQUE,
  delete_view BOOLEAN
);

INSERT INTO tables (table_view, uid, name) VALUES ('instruments_view', 'instruments_id', 'instruments');
INSERT INTO tables (table_view, uid, name) VALUES ('specifications_view', 'specifications_id', 'specifications');
INSERT INTO tables (table_view, uid, name) VALUES ('instrument_specification_view', 'instrument_specification_id', 'instrument_specification');
INSERT INTO tables (table_view, uid, name) VALUES ('platform_view', 'platform_id', 'platform');
INSERT INTO tables (table_view, uid, name) VALUES ('calibration_view', 'calibration_id', 'calibration');

INSERT INTO tables (table_view, uid, name) VALUES ('installations_view', 'installations_id', 'installations');
INSERT INTO tables (table_view, uid, name) VALUES ('instrument_install_view', 'instrument_install_id', 'instrument_install');
INSERT INTO tables (table_view, uid, name) VALUES ('platform_install_view', 'platform_install_id', 'platform_install');

INSERT INTO tables (table_view, uid, name) VALUES ('flights_view', 'flights_id', 'flights');
INSERT INTO tables (table_view, uid, name) VALUES ('flight_installation_view', 'flight_installation_id', 'flight_installation');
INSERT INTO tables (table_view, uid, name) VALUES ('sessions_view', 'sessions_id', 'sessions');
INSERT INTO tables (table_view, uid, name) VALUES ('study_sites_view', 'study_sites_id', 'study_sites');
INSERT INTO tables (table_view, uid, name) VALUES ('dsm_view', 'dsm_id', 'dsm');
INSERT INTO tables (table_view, uid, name) VALUES ('flight_session_site_dsm_view', 'flight_session_site_dsm_id', 'flight_session_site_dsm');

INSERT INTO tables (table_view, uid, name) VALUES ('rawdata_view', 'rawdata_id', 'rawdata');
INSERT INTO tables (table_view, uid, name) VALUES ('variables_view', 'variables_id', 'variables');
INSERT INTO tables (table_view, uid, name) VALUES ('units_view', 'units_id', 'units');
INSERT INTO tables (table_view, uid, name) VALUES ('sessions_metadata_view', 'sessions_metadata_id', 'sessions_metadata');
INSERT INTO tables (table_view, uid, name) VALUES ('rawdata_metadata_view', 'rawdata_metadata_id', 'rawdata_metadata');
INSERT INTO tables (table_view, uid, name) VALUES ('boresight_offsets_view', 'boresight_offsets_id', 'boresight_offsets');
INSERT INTO tables (table_view, uid, name) VALUES ('boresight_rawdata_dsm_view', 'boresight_rawdata_dsm_id', 'boresight_rawdata_dsm');

INSERT INTO tables (table_view, uid, name) VALUES ('projects_view', 'projects_id', 'projects');
INSERT INTO tables (table_view, uid, name) VALUES ('site_projects_view', 'site_projects_id', 'site_projects');

INSERT INTO tables (table_view, uid, name) VALUES ('image_output_view', 'image_output_id', 'image_output');
INSERT INTO tables (table_view, uid, name) VALUES ('processing_events_view', 'processing_events_id', 'processing_events');
INSERT INTO tables (table_view, uid, name) VALUES ('workflow_chtc_view', 'workflow_chtc_id', 'workflow_chtc');
INSERT INTO tables (table_view, uid, name) VALUES ('rawdata_processing_view', 'rawdata_processing_id', 'rawdata_processing');
INSERT INTO tables (table_view, uid, name) VALUES ('rawdata_image_output_view', 'rawdata_image_output_id', 'rawdata_image_output');
INSERT INTO tables (table_view, uid, name) VALUES ('processing_metadata_view', 'processing_metadata_id', 'processing_metadata');
INSERT INTO tables (table_view, uid, name) VALUES ('processing_output_workflow_view', 'processing_output_workflow_id', 'processing_output_workflow');

INSERT INTO tables (table_view, uid, name) VALUES ('analysis_plot_view', 'analysis_plot_id', 'analysis_plot');
INSERT INTO tables (table_view, uid, name) VALUES ('tree_data_view', 'tree_data_id', 'tree_data');
INSERT INTO tables (table_view, uid, name) VALUES ('quadrats_view', 'quadrats_id', 'quadrats');
INSERT INTO tables (table_view, uid, name) VALUES ('samples_view', 'samples_id', 'samples');
INSERT INTO tables (table_view, uid, name) VALUES ('branch_data_view', 'branch_data_id', 'branch_data');
INSERT INTO tables (table_view, uid, name) VALUES ('species_view', 'species_id', 'species');
INSERT INTO tables (table_view, uid, name) VALUES ('species_code_view', 'species_code_id', 'species_code');
INSERT INTO tables (table_view, uid, name) VALUES ('species_speciescode_view', 'species_speciescode_id', 'species_speciescode');
INSERT INTO tables (table_view, uid, name) VALUES ('observations_view', 'observations_id', 'observations');
INSERT INTO tables (table_view, uid, name) VALUES ('radiometric_calibration_target_view', 'radiometric_calibration_target_id', 'radiometric_calibration_target');
INSERT INTO tables (table_view, uid, name) VALUES ('geometric_calibration_points_view', 'geometric_calibration_points_id', 'geometric_calibration_points');

INSERT INTO tables (table_view, uid, name) VALUES ('plot_quadrats_view', 'plot_quadrats_id', 'plot_quadrats');
INSERT INTO tables (table_view, uid, name) VALUES ('plot_tree_data_view', 'plot_tree_data_id', 'plot_tree_data');
INSERT INTO tables (table_view, uid, name) VALUES ('tree_branch_data_view', 'tree_branch_data_id', 'tree_branch_data');
INSERT INTO tables (table_view, uid, name) VALUES ('quadrats_samples_view', 'quadrats_samples_id', 'quadrats_samples');
INSERT INTO tables (table_view, uid, name) VALUES ('samples_species_view', 'samples_species_id', 'samples_species');
INSERT INTO tables (table_view, uid, name) VALUES ('branch_samples_view', 'branch_samples_id', 'branch_samples');
INSERT INTO tables (table_view, uid, name) VALUES ('tree_species_view', 'tree_species_id', 'tree_species');

INSERT INTO tables (table_view, uid, name) VALUES ('quadrat_observations_view', 'quadrat_observations_id', 'quadrat_observations');
INSERT INTO tables (table_view, uid, name) VALUES ('plot_observations_view', 'plot_observations_id', 'plot_observations');
INSERT INTO tables (table_view, uid, name) VALUES ('tree_observations_view', 'tree_observations_id', 'tree_observations');
INSERT INTO tables (table_view, uid, name) VALUES ('branch_observations_view', 'branch_observations_id', 'branch_observations');
INSERT INTO tables (table_view, uid, name) VALUES ('rct_site_spectrometer_view', 'rct_site_spectrometer_id', 'rct_site_spectrometer');
INSERT INTO tables (table_view, uid, name) VALUES ('gcp_site_device_view', 'gcp_site_device_id', 'gcp_site_device');
INSERT INTO tables (table_view, uid, name) VALUES ('AnalysisPlot_StudySites_view', 'AnalysisPlot_StudySites_id', 'AnalysisPlot_StudySites');


INSERT INTO tables (table_view, uid, name) VALUES ('spectra_view', 'spectra_id', 'spectra');
INSERT INTO tables (table_view, uid, name) VALUES ('measurements_view', 'measurements_id', 'measurements');
INSERT INTO tables (table_view, uid, name) VALUES ('labs_view', 'labs_id', 'labs');
INSERT INTO tables (table_view, uid, name) VALUES ('samples_measurements_view', 'samples_measurements_id', 'samples_measurements');
INSERT INTO tables (table_view, uid, name) VALUES ('sample_spectra_view', 'sample_spectra_id', 'sample_spectra');


INSERT INTO tables (table_view, uid, name) VALUES ('personnel_view', 'personnel_id', 'personnel');
INSERT INTO tables (table_view, uid, name) VALUES ('activity_view', 'activity_id', 'activity');
INSERT INTO tables (table_view, uid, name) VALUES ('plot_personnel_view', 'plot_personnel_id', 'plot_personnel');
INSERT INTO tables (table_view, uid, name) VALUES ('sessions_personnel_view', 'sessions_personnel_id', 'sessions_personnel');
INSERT INTO tables (table_view, uid, name) VALUES ('installations_personnel_view', 'installations_personnel_id', 'installations_personnel');
INSERT INTO tables (table_view, uid, name) VALUES ('studysites_personnel_view', 'studysites_personnel_id', 'studysites_personnel');
INSERT INTO tables (table_view, uid, name) VALUES ('projects_personnel_view', 'projects_personnel_id', 'projects_personnel');
INSERT INTO tables (table_view, uid, name) VALUES ('sample_personnel_view', 'sample_personnel_id', 'sample_personnel');


--INSERT INTO tables (table_view, uid, name) VALUES ('platform_view', 'platform_id', 'platform');
