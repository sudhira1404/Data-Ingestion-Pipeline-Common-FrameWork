CREATE TABLE ${schema}.gam_line_item_delivery_state (
                                                  pipeline_run_id varchar NOT NULL, -- Unique identifier of the batch request
                                                  request_loop_id int4 NOT NULL, -- The loop number this Line Item request was made during.
                                                  line_item_id int8 NOT NULL, -- The GAM LineItem ID.
                                                  created_timestamp timestamptz NOT NULL, -- Timestamp when the GAM LineItem was downloaded from the API.
                                                  report_start_date varchar NOT NULL, -- The date of the requested Line Item report.
                                                  line_item_status varchar NOT NULL, -- The Line Item status
                                                  line_item_type varchar NOT NULL, -- The Line Item type
                                                  line_item_start_date date NOT NULL, -- The Line Item start date
                                                  line_item_end_date date NOT NULL -- The Line Item end date. If the line item is set to unlimited end date, this value will be set to 12-31-9999
)
    TABLESPACE pg_default;;

COMMENT ON TABLE ${schema}.gam_line_item_delivery_state IS 'Used by GamLineItemDeliveryService to hold all Line Items for the GamForecastOrchestrationService to use instead of making another trip to the API within the same date.';

-- Column comments

COMMENT ON COLUMN ${schema}.gam_line_item_delivery_state.pipeline_run_id IS 'Unique identifier of the batch request';
COMMENT ON COLUMN ${schema}.gam_line_item_delivery_state.request_loop_id IS 'The loop number this Line Item request was made during.';
COMMENT ON COLUMN ${schema}.gam_line_item_delivery_state.line_item_id IS 'The GAM LineItem ID.';
COMMENT ON COLUMN ${schema}.gam_line_item_delivery_state.created_timestamp IS 'Timestamp when the GAM LineItem was downloaded from the API.';
COMMENT ON COLUMN ${schema}.gam_line_item_delivery_state.report_start_date IS 'The date of the requested Line Item report.';
COMMENT ON COLUMN ${schema}.gam_line_item_delivery_state.line_item_status IS 'The Line Item status';
COMMENT ON COLUMN ${schema}.gam_line_item_delivery_state.line_item_type IS 'The Line Item type';
COMMENT ON COLUMN ${schema}.gam_line_item_delivery_state.line_item_start_date IS 'The Line Item start date';
COMMENT ON COLUMN ${schema}.gam_line_item_delivery_state.line_item_end_date IS 'The Line Item end date. If the line item is set to unlimited end date, this value will be set to 12-31-9999';

-- Permissions

ALTER TABLE ${schema}.gam_line_item_delivery_state OWNER TO ${userid};
GRANT ALL ON TABLE ${schema}.gam_line_item_delivery_state to ${userid};