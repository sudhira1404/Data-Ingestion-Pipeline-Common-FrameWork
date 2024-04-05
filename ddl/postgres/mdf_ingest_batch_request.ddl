-- Table: ${schema}.mdf_ingest_batch_request

-- DROP TABLE ${schema}.mdf_ingest_batch_request;

CREATE TABLE ${schema}.mdf_ingest_batch_request
(
    batch_request_id character varying COLLATE pg_catalog."default" NOT NULL,
    batch_request_status character varying COLLATE pg_catalog."default",
    created_timestamp timestamp with time zone,
    source_system character varying COLLATE pg_catalog."default",
    start_date character varying COLLATE pg_catalog."default",
    end_date character varying COLLATE pg_catalog."default",
    landing_file character varying COLLATE pg_catalog."default",
    CONSTRAINT mdf_ingest_batch_request_pkey PRIMARY KEY (batch_request_id)
)

    TABLESPACE pg_default;

ALTER TABLE ${schema}.mdf_ingest_batch_request
    OWNER to ${userid};

GRANT ALL ON TABLE ${schema}.mdf_ingest_batch_request TO ${userid};

GRANT ALL ON TABLE ${schema}.mdf_ingest_batch_request TO PUBLIC;

COMMENT ON TABLE ${schema}.mdf_ingest_batch_request
    IS 'Used by MDF common ingest engine to record ingestion batch requests and track their status changes.';

COMMENT ON COLUMN ${schema}.mdf_ingest_batch_request.batch_request_id
    IS 'Unique identifier of the batch request';

COMMENT ON COLUMN ${schema}.mdf_ingest_batch_request.batch_request_status
    IS 'Current status of the batch request';

COMMENT ON COLUMN ${schema}.mdf_ingest_batch_request.created_timestamp
    IS 'Timestamp when the batch request was created';

COMMENT ON COLUMN ${schema}.mdf_ingest_batch_request.source_system
    IS 'Name of the source system which was requested as part of this batch request';

COMMENT ON COLUMN ${schema}.mdf_ingest_batch_request.start_date
    IS 'The start date requested to be extracted from the source system as part of the batch request';

COMMENT ON COLUMN ${schema}.mdf_ingest_batch_request.end_date
    IS 'The end date requested to be extracted from the source system as part of the batch request';

COMMENT ON COLUMN ${schema}.mdf_ingest_batch_request.landing_file
    IS 'The location and name of the HDFS landing file requested as part of the batch request';