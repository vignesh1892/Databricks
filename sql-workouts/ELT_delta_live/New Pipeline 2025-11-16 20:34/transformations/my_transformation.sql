CREATE OR REFRESH STREAMING TABLE bronze_fight
AS
SELECT * FROM `databricks_airline_performance_data.v01.flights`;

CREATE FLOW customer_cdc_flow AS AUTO CDC INTO LIVE.dim_customer_scd2
FROM STREAM(bronze_fight)
KEYS (id)
SEQUENCE BY updated_at
COLUMNS *
STORED AS SCD TYPE 2;