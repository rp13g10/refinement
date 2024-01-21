import os

from pyspark.sql import functions as F, SparkSession
from pyspark.sql.types import DoubleType

from relevation import get_elevation

data_dir="/home/ross/repos/refinement/data"

sc = SparkSession.builder.appName("refinement").master("local[10]").getOrCreate()

nodes_df = sc.read.parquet(os.path.join(data_dir, "nodes"))

@F.udf(returnType=DoubleType())
def _get_elevation_for_row(lat, lon):
    return get_elevation(lat, lon)

nodes_df = nodes_df.withColumn('elevation', _get_elevation_for_row('lat', 'lon'))

nodes_df.write.partitionBy('bucket').parquet(os.path.join(data_dir, 'node_elevations'))
