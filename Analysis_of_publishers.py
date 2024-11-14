from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, desc



spark = SparkSession.builder.appName("Publishers").master("local[*]").getOrCreate()




#******************************Publication by different publishers*****************************************


def publication_publishers():
    
    #reading the parquet file 
    read_publishers = spark.read.parquet("D:\open_alex_parquet\publishers.parquet")
    
    # dropping duplicates if any
    filter_publishers = read_publishers.dropDuplicates(["id"])
    
    #selecting only the required columns
    publishers = filter_publishers.select("id","display_name","works_count")
    
    #counting the total publications and displaying the result
    publishers.groupBy("id","display_name").sum("works_count").orderBy(col("sum(works_count)").desc()).show()
    
    
    
    
    
publication_publishers()
    
    
spark.stop()