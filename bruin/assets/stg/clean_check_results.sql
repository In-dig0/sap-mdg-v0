/* @bruin
name: stg.clean_check_results
type: pg.sql
depends:
  - ingestion.ingest_zip_to_raw
  - ingestion.ingest_xlsx_to_ref
description: >
  Svuota la tabella stg.check_results prima di ogni run.
  Deve essere eseguito come primo step del layer stg,
  prima di tutti i check di qualità.
connection: mdg_postgres
@bruin */

TRUNCATE TABLE stg.check_results;
