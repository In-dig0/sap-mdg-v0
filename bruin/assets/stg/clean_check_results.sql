/* @bruin
name: stg.clean_check_results
type: pg.sql
depends:
  - setup.pipeline_run_open
  - ingestion.ingest_zip_to_raw
  - ingestion.ingest_xlsx_to_ref
description: >
  Svuota la tabella stg.check_results prima di ogni run.
  Dipende da pipeline_run_open per garantire che il run_id
  sia già stato registrato in stg.pipeline_runs.
connection: mdg_postgres
@bruin */

TRUNCATE TABLE stg.check_results;
