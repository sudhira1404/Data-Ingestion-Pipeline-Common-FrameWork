CREATE TABLE ${schema}.mdf_s3_parameter (
    source_report_type varchar(255) NOT NULL,
    actv_f varchar(255) NOT NULL,
    prefix varchar(255) NOT NULL,
    dirdownload bool NOT NULL DEFAULT false,
    xfermgrsinglefiledownload bool NOT NULL DEFAULT false,
    s3prefixsqlfunction bool NOT NULL DEFAULT false,
    comparewithprevloadedfilescheckbycurrentdate bool NOT NULL DEFAULT false,
    comparewithprevloadedfilescheck bool NOT NULL DEFAULT false,
    abortnofilefound bool NOT NULL DEFAULT false,
    fileage int4 NOT NULL DEFAULT 0,
    splitfile bool NOT NULL DEFAULT false,
    splitfilecompress bool NOT NULL DEFAULT false,
    CONSTRAINT mdf_s3_parameter_pkey PRIMARY KEY (source_report_type, actv_f)
    )
    TABLESPACE pg_default;

ALTER TABLE ${schema}.mdf_s3_parameter
    OWNER to ${userid};

GRANT ALL ON TABLE ${schema}.mdf_s3_parameter TO ${userid};

GRANT ALL ON TABLE ${schema}.mdf_s3_parameter TO PUBLIC;

COMMENT ON TABLE ${schema}.mdf_s3_parameter
    IS 'Contains the different optional S3 parameters.';

COMMENT ON COLUMN ${schema}.mdf_s3_parameter.actv_f
    IS 'If set to "Y" ,the parameters will read from PGDB table instead of  reading from enum S3ReportTypes in ApplicationConstants';

COMMENT ON COLUMN ${schema}.mdf_s3_parameter.prefix
    IS 'Is the folder or path to look for the file in the s3.';

COMMENT ON COLUMN ${schema}.mdf_s3_parameter.dirdownload
    IS 'Is a boolean.If set to true then downloads the entire directory(directory containing more than one file) using multipart download which is faster when comapred to this parameter not set';

COMMENT ON COLUMN ${schema}.mdf_s3_parameter.xfermgrsinglefiledownload
    IS 'Is a boolean If set to true then downloads a single file from the entire directory(direcotry should have just one file) using multipart download which is faster when comapred to this parameter not set';

COMMENT ON COLUMN ${schema}.mdf_s3_parameter.s3prefixsqlfunction
    IS 'The sql representing prefix or path provided here will be exeuted in PGdatabase and result will be used by the program to look for the files for download';

COMMENT ON COLUMN ${schema}.mdf_s3_parameter.comparewithprevloadedfilescheckbycurrentdate
    IS 'Is a boolean.If set to true,checks for the file entry in the table mdf_s3_objectsummary for current date.If file is present then it indicates this file was prevouisly loaded and will ignore the file from downloading from S3 bucket.To reprocess ,delete the file from the table';

COMMENT ON COLUMN ${schema}.mdf_s3_parameter.comparewithprevloadedfilescheck
    IS 'Is a boolean.If set to true,checks for the file entry in the table mdf_s3_objectsummary.If file is present then it indicates this file was prevouisly loaded and checks if etag(checksum) value of the file that is being requested to be downloaded with etag for the same file in the table. If no change , will ignore the file from downloading from S3 bucket';

COMMENT ON COLUMN ${schema}.mdf_s3_parameter.abortnofilefound
    IS 'Is a boolean.If set to true,if no file in the s3 bucket job will abort and throw an exception';

COMMENT ON COLUMN ${schema}.mdf_s3_parameter.fileage
    IS 'Is a integer value defaulted to 0.If set to 2 then for current date will go back 2 days and all files with S3 modified date greater than or equal to this will be considered';

COMMENT ON COLUMN ${schema}.mdf_s3_parameter.splitfile
    IS 'Is a boolean value defaulted to false.If set to true ,will split the file based on the param maxSizeBeforeNewFile in the yaml file and move to hdfs';


COMMENT ON COLUMN ${schema}.mdf_s3_parameter.splitfilecompress
    IS 'Is a boolean value defaulted to false.Used along with splitfile.If set to true ,will compress the file before moving to hdfs';
