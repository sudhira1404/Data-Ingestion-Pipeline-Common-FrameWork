CREATE TABLE ${schema}.gam_forecastable_line_items
(
    line_item_id bigint NOT NULL,
    report_start_date character varying NOT NULL
    )

    TABLESPACE pg_default;

ALTER TABLE ${schema}.gam_forecastable_line_items
    OWNER to ${userid};

GRANT ALL ON TABLE ${schema}.gam_forecastable_line_items TO ${userid};

GRANT ALL ON TABLE ${schema}.gam_forecastable_line_items TO PUBLIC;

COMMENT ON TABLE ${schema}.gam_forecastable_line_items
    IS 'Used by GamLineItemDeliveryService to hold all Line Items for the GamForecastOrchestrationService to use instead of making another trip to the API within the same date.';

COMMENT ON COLUMN ${schema}.gam_forecastable_line_items.line_item_id
    IS 'The GAM LineItem ID.';

COMMENT ON COLUMN ${schema}.gam_forecastable_line_items.report_start_date
    IS 'The date of the requested Line Item report.';

