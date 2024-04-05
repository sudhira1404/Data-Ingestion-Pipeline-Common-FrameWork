DROP TABLE IF EXISTS ${schema}.gam_forecast_contending_line_items;

CREATE TABLE ${schema}.gam_forecast_contending_line_items
(
    pipeline_run_id character varying NOT NULL,
    report_start_date character varying NOT NULL,
    availability_line_item_id bigint NOT NULL,
    availability_forecast_save_timestamp timestamp with time zone NOT NULL,
    contending_line_item_ids bigint[] NOT NULL,
    delivery_forecast_batch_id bigint,
    delivery_forecast_batch_generate_timestamp timestamp with time zone
)
TABLESPACE pg_default;

ALTER TABLE ${schema}.gam_forecast_contending_line_items OWNER to ${userid};

GRANT ALL ON TABLE ${schema}.gam_forecast_contending_line_items to ${userid};

COMMENT ON TABLE ${schema}.gam_forecast_contending_line_items
    IS '"Used by Gam Forecasting to save the contending line items for each availability forecast response, in order
    for delivery forecast batch requests to be made from line items that are highly competitive with each other.';

COMMENT ON COLUMN ${schema}.gam_forecast_contending_line_items.pipeline_run_id
    IS 'Unique identifier of the batch request';

COMMENT ON COLUMN ${schema}.gam_forecast_contending_line_items.report_start_date
    IS 'The date of the requested Line Item report.';

COMMENT ON COLUMN ${schema}.gam_forecast_contending_line_items.availability_line_item_id
    IS 'The GAM LineItem ID in the Availability Forecast request.';

COMMENT ON COLUMN ${schema}.gam_forecast_contending_line_items.availability_forecast_save_timestamp
    IS 'Timestamp when the availability forecast contending line items array was saved to this table.';

COMMENT ON COLUMN ${schema}.gam_forecast_contending_line_items.contending_line_item_ids
    IS 'The array of LineItem IDs of all contending line items.';

COMMENT ON COLUMN ${schema}.gam_forecast_contending_line_items.delivery_forecast_batch_id
    IS 'The batch identifier for this availability line item to help identify which line items end up being forecasted against each other during delivery forecasting';

COMMENT ON COLUMN ${schema}.gam_forecast_contending_line_items.delivery_forecast_batch_generate_timestamp
    IS 'Timestamp when the delivery forecast batch was generated';
