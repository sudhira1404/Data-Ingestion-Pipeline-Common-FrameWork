CREATE TABLE ${schema}.salesforce
(
    report_type character varying COLLATE pg_catalog."default" NOT NULL,
    attributes character varying COLLATE pg_catalog."default",
    CONSTRAINT salesforce_pkey PRIMARY KEY (report_type)
)

    TABLESPACE pg_default;

ALTER TABLE ${schema}.salesforce
    OWNER to ${userid};

GRANT ALL ON TABLE ${schema}.salesforce TO ${userid};

GRANT ALL ON TABLE ${schema}.salesforce TO PUBLIC;

COMMENT ON TABLE ${schema}.salesforce
    IS 'Used by MDF common ingest engine to retrieve attributes for respective salesforce reports';

COMMENT ON COLUMN ${schema}.salesforce.report_type
    IS 'Unique identifier of the salesforce report';

COMMENT ON COLUMN ${schema}.salesforce.attributes
    IS 'Comma separated salesforce report attributes.';
