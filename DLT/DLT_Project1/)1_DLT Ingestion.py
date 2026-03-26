import dlt
from pyspark.sql.functions import col



# Bronze Layer
@dlt.table(
    name="cust_table_bronze",
    comment="Bronze layer of dlt"
)
def cust_table_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/databricks_practice/inputdb/vehicle/gcs/schema")
        .option("header", "true")
        .load("/Volumes/databricks_practice/inputdb/vehicle/gcs/")
    )
    return df


@dlt.append_flow(
    target="cust_table_bronze",
    name="cust_table_bronze_flow_2",
    comment="Bronze layer of dlt"
)
def cust_table_bronze_flow_2():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/databricks_practice/inputdb/vehicle/gcs/schema")
        .option("header", "true")
        .load("/Volumes/databricks_practice/inputdb/vehicle/gcs/2_input/")
    )
    return df
