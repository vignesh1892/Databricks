import dlt
from pyspark.sql.functions import col



# Bronze Layer
@dlt.table(
    name="veh_table_bronze",
    comment="Bronze layer of dlt"
)
def veh_table_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/databricks_practice/inputdb/vehicle/gcs/schema")
        .option("header", "true")
        .load("/Volumes/databricks_practice/inputdb/vehicle/gcs/")
    )
    return df


@dlt.append_flow(
    target="veh_table_bronze",
    name="veh_table_bronze_flow_2",
    comment="Bronze layer of dlt"
)
def veh_table_bronze_flow_2():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/databricks_practice/inputdb/vehicle/gcs/schema")
        .option("header", "true")
        .load("/Volumes/databricks_practice/inputdb/vehicle/gcs/2_input/")
    )
    return df

#Silver layer

@dlt.table(
    name ="veh_table_silver",
    comment ="Silver layer of pipeline"
)
@dlt.expect_or_fail("valid_car_id", "car_id IS NOT NULL")
def veh_table_silver():
    return dlt.read_stream("veh_table_bronze")

#gold layer scd type1

dlt.create_streaming_table(
    name ="veh_table_gold",
    comment =" gold scd type1 table"
)
dlt.apply_changes(
    target="veh_table_gold",
    source="veh_table_silver",
    keys=["car_id"],
    sequence_by=col("event_date"),
    #apply_as_deletes=col("status") == "INACTIVE",
    stored_as_scd_type=1,
    name="veh_table_gold_cdc"  
)

