DROP TABLE if exists ${schema}.gam_forecast_state;
CREATE TABLE ${schema}.gam_forecast_state
(
    pipeline_run_id character varying NOT NULL,
    report_start_date character varying NOT NULL,
    line_item_id bigint NOT NULL,
    forecast_type character varying NOT NULL,
    created_timestamp timestamp with time zone NOT NULL,
    started_timestamp timestamp with time zone,
    finished_timestamp timestamp with time zone,
    status character varying,
    response character varying,
    failure_reason character varying,
    total_attempts int default 0
    )

    TABLESPACE pg_default;

ALTER TABLE ${schema}.gam_forecast_state
    OWNER to ${userid};

GRANT ALL ON TABLE ${schema}.gam_forecast_state TO ${userid};

GRANT ALL ON TABLE ${schema}.gam_forecast_state TO PUBLIC;

COMMENT ON TABLE ${schema}.gam_forecast_state
    IS 'Used by GamLineItemForecastService to run asynchronous jobs when requesting Availability or Delivery Forecasts.';

COMMENT ON COLUMN ${schema}.gam_forecast_state.pipeline_run_id
    IS 'Unique identifier of the batch request';

COMMENT ON COLUMN ${schema}.gam_forecast_state.report_start_date
    IS 'The date of the requested Line Item report.';

COMMENT ON COLUMN ${schema}.gam_forecast_state.line_item_id
    IS 'The GAM LineItem ID in the Availability Forecast request.';

COMMENT ON COLUMN ${schema}.gam_forecast_state.created_timestamp
    IS 'Timestamp when the asynchronous request for the Forecast was initialized';

COMMENT ON COLUMN ${schema}.gam_forecast_state.started_timestamp
    IS 'Timestamp when the asynchronous request for the Forecast started';

COMMENT ON COLUMN ${schema}.gam_forecast_state.finished_timestamp
    IS 'Timestamp when the asynchronous request for the Forecast finished';

COMMENT ON COLUMN ${schema}.gam_forecast_state.status
    IS 'The status of the Forecast asynchronous request, either Initialized, Running, or Failed.';

COMMENT ON COLUMN ${schema}.gam_forecast_state.response
    IS 'The end date requested to be extracted from the source system as part of the batch request';

COMMENT ON COLUMN ${schema}.gam_forecast_state.failure_reason
    IS 'The exception that was thrown in the last attempt at fetching the Forecast';

COMMENT ON COLUMN ${schema}.gam_forecast_state.total_attempts
    IS 'The number of attempts it took to result in either a completed or failed state. Defaults to 0';