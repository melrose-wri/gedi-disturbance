from pathlib import Path
from src.processing import degradation_overlap
from src.data import spark_postgis
from src.data import jrc_parser
from src import constants

#Create a spark session
spark = spark_postgis.get_spark()

########################################################
#degradation_overlap.get_shots_df()
########################################################
shots_dir = Path("/home/melrose94/projects/gedi/data/shots") #TODO:Update in constants.py (change linux path)
shots_df = degradation_overlap.get_shots_df(spark, shots_dir)  #TODO: Lat and long are flipped

shots_df.show(n=2)
shots_df.select(['t1_geom', 't2_geom']).show(n=2)

########################################################
#jrc_parser.convert_shot_dates()
########################################################
shots_df = jrc_parser.convert_shot_dates(shots_df)
shots_df.createOrReplaceTempView("gedi_shots")

shots_df.select(['t1_year', 't2_year']).show(n=2)

########################################################
#filter data based on height disturbance definition #TODO
########################################################
#shots_df.count()  #34173
#shots_df = shots_df.filter(shots_df.t2_year > shots_df.t1_year)
#shots_df.count()  #28532
#shots_df.createOrReplaceTempView("gedi_shots")

########################################################
#overlay ecozone
########################################################
ecozone_dir = constants.ECOZONE_PATH

def get_ecozone_df(spark, ecozone_dir):
    ecozone_df = spark.read.parquet(ecozone_dir.as_posix())
    ecozone_df.createOrReplaceTempView("ecozones")
    ecozone_df = spark.sql(
        "SELECT *, ST_GeomFromWKB(geometry) AS geom FROM ecozones"
    )
    ecozone_df = ecozone_df.drop("geometry")
    ecozone_df.createOrReplaceTempView("ecozones")
    return ecozone_df

ecozone_df = get_ecozone_df(spark, ecozone_dir)
ecozone_df.show(n=10)

ecozone_join_query = f"""
    SELECT s.*, f.emisEcozon AS emisEcozon, f.gainEcozon AS gainEcozon, f.GEZ_TERM AS GEZ_TERM
    FROM gedi_shots as s INNER JOIN ecozones as f
    ON ST_Contains(f.geom, s.t2_geom)
"""

shots_df = spark.sql(ecozone_join_query)
shots_df.createOrReplaceTempView("gedi_shots")
shots_df.show(n=10)

########################################################
#overlay country
########################################################
GADM_PATH = constants.SHAPEFILE_PATH / "country"
gadm_dir = GADM_PATH

def get_gadm_df(spark, gadm_dir):
    gadm_df = spark.read.parquet(gadm_dir.as_posix())
    gadm_df.createOrReplaceTempView("gadm")
    gadm_df = spark.sql(
        "SELECT *, ST_GeomFromWKB(geometry) AS geom FROM gadm"
    )
    gadm_df = gadm_df.drop("geometry")
    gadm_df.createOrReplaceTempView("gadm")
    return gadm_df

gadm_df = get_gadm_df(spark, gadm_dir)
gadm_df.show(n=10)

gadm_join_query = f"""
    SELECT s.*, f.iso AS country
    FROM gedi_shots as s INNER JOIN gadm as f
    ON ST_Contains(f.geom, s.t2_geom)
"""

shots_df = spark.sql(gadm_join_query)
shots_df.createOrReplaceTempView("gedi_shots")
shots_df.show(n=10)

########################################################
#jrc_parser.get_sharding_geoms()
########################################################
degrade_shards_df = spark.createDataFrame(jrc_parser.get_sharding_geoms()) #TODO: Issues with negative longitude?
degrade_shards_df.createOrReplaceTempView("degrade_shards")

degrade_shards_df.count()  #54
degrade_shards_df.show(n=10)

########################################################
#degradation_overlap.shards_join_query
########################################################

shots_df = spark.sql(degradation_overlap.shards_join_query)
shots_df.createOrReplaceTempView("gedi_shots")

shots_df.count()  #28532
shots_df.show(n=10)