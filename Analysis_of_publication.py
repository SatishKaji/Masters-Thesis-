from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, desc,lower,asc

spark = SparkSession.builder.appName("Publications").master("local[*]").config("spark.driver.memory", "14g").getOrCreate()




# ****************** Distribution of publications and open accessibility *****************

def distribution_and_oa():
    
    #reading first parquet file 
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    
    #selecting only the required columns
    works = filter_works.select("id","type")
    
    #displaying the distribution of publications within publication types
    works.select("type").groupBy("type").count().orderBy(col("count").desc()).show(truncate=False)
    
    #reading second parquet file 
    read_works_open_access = spark.read.parquet("D:\open_alex_parquet\works_open_access.parquet")
    
    #dropping duplicates if any
    filter_works_open_access = read_works.dropDuplicates(["work_id"])
    
    #selecting only the required columns
    works_open_access = filter_works_open_access.select("work_id","is_oa")
    
    #joining the dataframes
    joined_works = works.join(works_open_access,works.id ==  works_open_access.work_id,"inner") 
    
    #displaying the distribution of openly accessible publications within publication types
    joined_works.select("type").filter(lower(col('is_oa'))=="true").groupBy("type").count().orderBy(col("count").desc()).show(truncate=False)
    
distribution_and_oa()



# ****************Distribution of retracted publications***************************************

def distribution_and_retracted():
    
    #reading parquet file 
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    
    #selecting only the required columns
    works = filter_works.select("id","type","is_retracted")
    
    #displaying the distribution of retracted publications within publication types
    works.select("type").filter(lower(col('is_retracted'))=="true").groupBy("type").count().orderBy(col("count").desc()).show(truncate=False)

distribution_and_retracted()




# ****************************comparative analysis 1981-2001 and 2002-2022***************************

def comparative_analysis():
    
    #reading parquet file 
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    
    #selecting only the required columns
    works = filter_works.select("id","type","publication_year")
    
    #Filtering first by publication years between 1980 to 2001
    works_40_yrs = works.select("type").filter(col("publication_year").between(1981,2001))\
                   .groupBy("type").count().withColumnRenamed('count',"Total_count(1981-2001)" ).orderBy(col("Total_count(1981-2001)").asc())
                   
    #displaying the result               
    works_40_yrs.show(truncate=False)
    
    #filtering by publication years between 2002 to 2022
    works_20_yrs = works.select("type").filter(col("publication_year").between(2002,2022))\
                   .groupBy("type").count().withColumnRenamed('count',"Total_count(2002-2022)" ).orderBy(col("Total_count(2002-2022)").asc())
                   
    #displaying the result               
    works_20_yrs.show(truncate=False)
            
comparative_analysis()




# **************Publication per year************************

def publication_per_year():

    #reading parquet file 
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    
    #selecting only the required columns
    works = filter_works.select("id","publication_year")
    
    #filtering by publication years between 1992 to 2022 
    yearly_publication = works.select("publication_year").filter(col("publication_year").between(1992,2022))\
                        .groupBy("publication_year").count().withColumnRenamed('count',"count per year" ).orderBy(col("publication_year").asc())
                        
    #displaying the results              
    yearly_publication.show(truncate=False)

publication_per_year()



#*************************Openly accessible publications vs other publications**************************
def oa_vs_others():
    
    #reading first parquet file
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #reading second parquet file 
    read_works_oa = spark.read.parquet("D:\open_alex_parquet\works_open_access.parquet")
    
    #dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    filter_works_oa = read_works_oa.dropDuplicates(["work_id"])
    
    #selecting only the required columns
    works_oa = filter_works_oa.select("work_id","is_oa")
    works = filter_works.select("id","publication_year")
    
    #filtering by publication years between 1992 to 2022 
    select_years = works.select("id","publication_year").filter(col("publication_year").between(1992,2022))
    
    #joining the data frames
    combined = select_years.join(works_oa,select_years.id ==  works_oa.work_id,"inner")
    
    #for openly accessible works
    total_works_oa = combined.select("publication_year").filter(lower(col('is_oa'))=="true")\
                    .groupBy("publication_year").count().orderBy(col("publication_year").asc())
    
    #displaying the results for openly accessible works
    total_works_oa.show(truncate=False)
    
    #for works not openly accessible
    total_works_non_oa = combined.select("publication_year").filter(lower(col('is_oa'))=="false")\
                        .groupBy("publication_year").count().orderBy(col("publication_year").asc())
                        
    #displaying the results for works not openly accessible
    total_works_non_oa.show(truncate=False)

oa_vs_others()


spark.stop()