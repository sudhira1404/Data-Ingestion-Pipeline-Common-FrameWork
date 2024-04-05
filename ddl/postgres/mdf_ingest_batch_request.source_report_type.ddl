ALTER TABLE ${schema}.mdf_ingest_batch_request
    ADD COLUMN source_report_type character varying COLLATE pg_catalog."default";
COMMENT ON COLUMN ${schema}.mdf_ingest_batch_request.source_report_type
    IS 'Name of the report type for the source system to retrieve data for. Eg if source is "gam" then report type can be either "actuals" or "delivery"';
