from pyspark.sql import SparkSession
from pathlib import Path
import os
from src.processing import degradation_overlap

#Create a spark session


shots_dir = Path('C://GIS/gedi/data/GEDI/')

#get_shots_df

shots_df = spark.read.parquet(shots_dir.as_posix())

# using SQLContext to read parquet file

sc = spark.sparkContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# read parquet file
df = sqlContext.read.parquet('C:\GIS\gedi\data\GEDI\part-00000-a54c2db8-f5d5-4626-b21d-d77174e4a406-c000.snappy.parquet')