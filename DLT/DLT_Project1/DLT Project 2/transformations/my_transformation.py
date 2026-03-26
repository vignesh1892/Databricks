import dlt
from pyspark.sql.functions import col, sum

#bronze layer
@dlt.table(
    name ="veh_daily_summary_bronze",
    comment ="Bronze layer",
)
def veh_daily_summary_bronze():
    return spark.read.format("delta").table("databricks_practice.dlt_output.veh_table_gold")

#silver layer
@dlt.table(
    name ="veh_daily_summary_silver",
    comment ="silver layer"
)
@dlt.expect_or_drop("validate_speed","speed>50")
def veh_daily_summary_silver():
    return dlt.read_stream("veh_daily_summary_bronze")

#gold layer

dlt.create_streaming_table(
    name ="veh_daily_summary_gold_base",
    comment =" gold scd type1 table"
)
dlt.apply_changes(
    target="veh_daily_summary_gold_base",
    source ="veh_daily_summary_silver",
    keys =["car_id"],
    sequence_by=col("event_date"),
    #apply_as_deletes=col("status") == "INACTIVE",
    stored_as_scd_type=1,
    name="veh_table_gold_daily_sum_cdc"  
)

@dlt.table(
    name="veh_table_gold_agg",
    comment="Aggregated vehicle metrics"
)
def veh_table_gold_agg():
    df =dlt.read("veh_daily_summary_gold_base")
    return (
        df.groupBy("car_id", "event_date")
          .agg(
              sum("fuel_level").alias("fuel_level"),
              sum("odometer").alias("odometer"),
              sum("last_service_km").alias("last_service_km"),
              sum("speed").alias("speed")
          )
    )
