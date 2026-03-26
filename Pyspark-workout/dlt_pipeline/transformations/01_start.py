import dlt 
_order_status = spark.conf.get("custom.orderStatus","NA")
#create streaming table for orders
@dlt.table(
    table_properties ={"quality":"broonze"},
    comment ="order bronze layer"
)
def order_bronze():
    df = spark.readStream.table("dev.bronze.orders_raw")
    return df
  
'''  
#create MV for customer:
@dlt.table(
    table_properties ={"quality":"broonze"},
    comment ="Customer bronze layer"
)
def cust_bronze():
    df = spark.read.table("dev.bronze.customer_raw")
    return df
'''
#create MV for customer:
@dlt.view(
    comment ="Customer bronze view"
)
def customer_bronze_vw():
    df = spark.readStream.table("dev.bronze.customer_raw")
    return df

from pyspark.sql.functions import expr

@dlt.create_streaming_table("customer_scd1_bronze")

@dlt.apply_changes(
    target="customer_scd1_bronze",
    source = "customer_bronze_vw",
    keys =["c_custkey"],
    stored_as_scd_type =1,
    apply_as_deletes = expr("__src_action =D"),
    apply_as_truncates = expr("__src_action =T"),
    sequence_by ="__src_insert_dt"
)

@dlt.create_streaming_table("customer_scd2_bronze")

@dlt.apply_changes(
    target="customer_scd2_bronze",
    source = "customer_bronze_vw",
    keys =["c_custkey"],
    stored_as_scd_type =2,
    except_column_list=["__src_action", "__src_insert_dt"],
    sequence_by ="__src_insert_dt"
)

#create view to join order with customer
@dlt.view(
    comment ="Joined view"
)
def joined_vw():
    df_c = spark.read.table("LIVE.customer_scd2_bronze").where("__END_AT is null")
    df_o = spark.read.table("LIVE.order_bronze")
    df_join =df_c.join(df_o,how="inner",on=df_c.c_custkey == df_o.o_custkey)
    return df_join

#create MV fto add new column :
from pyspark.sql.functions import current_timestamp,count
@dlt.table(
    table_properties ={"quality":"silver"},
    comment ="Joined table"
)
def joined_silver():
    df = spark.read.table("LIVE.joined_vw").withColumn("__insert_date",current_timestamp())
    return df

##agg:
for i in _order_status.split(","):

    @dlt.table(
        table_properties ={"quality":"gold"},
        comment ="Order aggregrated table",
        name = f"oders_agg_{i}_gold"
    )
    def orders_agg_gold():
        df = spark.read.table("LIVE.joined_silver")
        df_final = df.groupBy("c_mktsegment").agg(count("o_orderkey").alias ("sum_orders")).withColumn("__insert_date",current_timestamp())
        return df_final