
CREATE TABLE ${schema}.mdf_s3_objectsummary (
	pipeline_run_id varchar(255) NOT NULL,
	source_report_type varchar(255) NOT NULL,
  key varchar(255) NOT NULL,
	etag varchar(255) NOT NULL,
	bucketname varchar(255) NOT NULL,
	size varchar(255) NOT NULL,
	lastmodified varchar(255) NOT NULL,
	created_timestamp timestamp NOT NULL,
	created_date date NOT NULL,
	CONSTRAINT mdf_s3_objectsummary_pkey PRIMARY KEY (pipeline_run_id, source_report_type, key, etag)
);

TABLESPACE pg_default;

ALTER TABLE ${schema}.mdf_s3_objectsummary
    OWNER to ${userid};

GRANT ALL ON TABLE ${schema}.mdf_s3_objectsummary TO ${userid};

GRANT ALL ON TABLE ${schema}.mdf_s3_objectsummary TO PUBLIC;

COMMENT ON TABLE ${schema}.mdf_s3_objectsummary
    IS 'Table contains all the files downloaded and moved to HDFS.';

COMMENT ON COLUMN ${schema}.mdf_s3_objectsummary.pipeline_run_id
  IS 'Unique identifier of the batch request';

COMMENT ON COLUMN ${schema}.mdf_s3_objectsummary.source_report_type
   IS 'Name of the source system which was requested as part of this batch request';

COMMENT ON COLUMN ${schema}.mdf_s3_objectsummary.key
   IS 'S3 file or keyname ';

COMMENT ON COLUMN ${schema}.mdf_s3_objectsummary.etag
   IS 'file checksum';

COMMENT ON COLUMN ${schema}.mdf_s3_objectsummary.bucketname
   IS 'If set to "Y" ,the parameters will read from PGDB table instead of  reading from enum S3ReportTypes in ApplicationConstants';

COMMENT ON COLUMN ${schema}.mdf_s3_objectsummary.size
    IS 'Bucketname from where file is downloaded';

COMMENT ON COLUMN ${schema}.mdf_s3_objectsummary.lastmodified
   IS 'File modification date on the s3';

COMMENT ON COLUMN ${schema}.mdf_s3_objectsummary.created_timestamp
   IS 'Record creation timestamp by the current process';

COMMENT ON COLUMN ${schema}.mdf_s3_objectsummary.created_date
  IS 'Record creation date by the current process';
