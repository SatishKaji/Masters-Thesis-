from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col,asc


spark = SparkSession.builder.appName("Sources").master("local[*]").getOrCreate()




#******************************Publication by different source types*****************************************
def publication_sources():
    
    #reading the parquet file 
    read_sources = spark.read.parquet("D:\open_alex_parquet\sources.parquet")
    # dropping duplicates if any
    filter_sources = read_sources.dropDuplicates(["id"])
    #selecting only the required columns
    sources = filter_sources.select("id","type","works_count")
    #counting the total publications and displaying the result
    sources.groupBy("type").sum("works_count")

publication_sources()






#******************************Publication by different source types per year*****************************************

def publication_source_types():
    
    #reading first parquet file 
    read_sources = spark.read.parquet("D:\open_alex_parquet\sources.parquet")
    
    #reading second parquet file 
    read_sources_years = spark.read.parquet("D:\open_alex_parquet\sources_counts_by_year.parquet")
    
    # dropping duplicates if any
    filter_sources = read_sources.dropDuplicates(["id"])
    filter_sources_years = read_sources_years.dropDuplicates(["source_id"])
    
    #selecting only the required columns
    sources = filter_sources.select("id","type")
    sources_years = filter_sources_years.select("source_id","type","works_count")
    
    #joining the dataframes
    combined = sources.join(sources_years,sources.id == sources_years.source_id ,"inner")
    
    #counting the total publications and displaying the result
    combined.groupBy("type","year").sum("works_count").orderBy(col("type"),col("year").asc())


publication_source_types()







spark.stop()