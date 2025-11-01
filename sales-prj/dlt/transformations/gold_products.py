import dlt

my_rules = {'rule1':'product_id is not null',
            'rule2':'product_name is not null'}

@dlt.table(name = 'DimProducts_stage')
@dlt.expect_all_or_drop(my_rules)
def dimproducts_stage():
    df = spark.readStream.table('sales_prj.gold.products')
    return df

@dlt.view
def dimproducts_view():
    df = spark.readStream.table('live.dimproducts_stage')
    return df

dlt.create_streaming_table('dimproducts')



dlt.apply_changes(
    target = 'dimproducts',
    source = 'live.dimproducts_view',
    keys = ['product_id'],
    sequence_by = 'product_id',
    stored_as_scd_type = 2
)
  

