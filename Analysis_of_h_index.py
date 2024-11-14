from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc,lit,when,row_number
from pyspark.sql.window import Window



spark = SparkSession.builder.appName("H-index").master("local[*]").config("spark.driver.memory", "14g").getOrCreate()




# ***********************************Average h-index************************************************************


def average_h_index():
    
    #reading the parquet file 
    read_h_index = spark.read.parquet("D:\open_alex_parquet\index.parquet")
    
    # dropping duplicates if any
    filter_h_index = read_h_index.dropDuplicates(["work_id","author_id"])
    
    #selecting only the required columns
    h_index = filter_h_index.select("work_id","author_id","concept_id","publication_year","cited_by_count")
    
    #filtering to include only the required years and selecting computer science as research field
    selecting_subject = h_index.select("*").filter((col("publication_year").between(0,2020))  & (col("concept_id")=="https://openalex.org/C41008148"))

    #creating a window by partioning according to authors and ordering by number of citations received
    windowSpec = Window.partitionBy("author_id").orderBy(col("cited_by_count").desc())
    
    # adding a new column with row numbers
    row_num=selecting_subject.withColumn("row_number",row_number().over(windowSpec))
    
    #calculating h-index
    h_index_count=row_num.withColumn("h-index", when((row_num.cited_by_count >= row_num.row_number),row_num.row_number ).otherwise(lit(0)))
    
    #calculating average h-index and displaying the result
    h_index_count.select("*").filter(col("h-index")>0).groupBy("concept_id").avg("h-index").show(truncate=False)
    

average_h_index()




spark.stop()

