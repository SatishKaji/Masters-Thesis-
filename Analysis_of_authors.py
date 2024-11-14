from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count,countDistinct,asc



spark = SparkSession.builder.appName("Authors").master("local[*]").config("spark.driver.memory", "14g").getOrCreate()




# ***************************unique Authors per year*******************************

def unique_authors():
    
    #reading first parquet file 
    read_authorship = spark.read.parquet("D:\open_alex_parquet\works_authorships.parquet")
    
    #reading second parquet file 
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    filter_authorship = read_authorship.dropDuplicates(["work_id","author_id"])
    
    #selecting only the required columns
    works = filter_works.select("id","publication_year")
    authors = filter_authorship.select("work_id","author_id")
    
    #filtering by publication years between 1992 to 2022 
    select_years = works.select("id","publication_year").filter(col("publication_year").between(1992,2022))
    
    #joining the dataframes
    combined = authors.join(select_years,authors.work_id == select_years.id ,"inner")
    
    #displaying the total number of unique authors per year
    combined.select("*").groupBy("publication_year").agg(countDistinct('author_id')).orderBy(col("publication_year").asc()).show(truncate=False)
    
unique_authors()




#******************************Total Authors per year*****************************

def total_authors():
    
    #reading first parquet file 
    read_authorship = spark.read.parquet("D:\open_alex_parquet\works_authorships.parquet")
    
    #reading second parquet file 
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    filter_authorship = read_authorship.dropDuplicates(["work_id","author_id"])
    
    #selecting only the required columns
    authors = filter_authorship.select("work_id","author_id")
    works = filter_works.select("id","publication_year")
    
    #filtering by publication years between 1992 to 2022 and displaying the results
    select_years = works.select("id","publication_year").filter(col("publication_year").between(1992,2022))
    
    #joining the dataframes
    combined = authors.join(select_years,authors.work_id == select_years.id ,"inner")
    
    #displaying the total number of authors per year
    combined.select("*").groupBy("publication_year").agg(count('author_id')).orderBy(col("publication_year").asc()).show(truncate=False)

total_authors()


#********************Publications single-authored vs coauthored************************************

def publication_single_vs_multiple():
    
    #reading first parquet file 
    read_authorship = spark.read.parquet("D:\open_alex_parquet\works_authorships.parquet")
    
    #reading second parquet file 
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    filter_authorship = read_authorship.dropDuplicates(["work_id","author_id"])
    
    #selecting only the required columns
    authors = filter_authorship.select("work_id","author_id")
    works = filter_works.select("id","publication_year")
    
    #filtering by publication years between 1992 to 2022 and displaying the results
    select_years = works.select("id","publication_year").filter(col("publication_year").between(1992,2022))
    
    #counting the number of authors for each publication
    count_authors = authors.groupBy("work_id").count()
    
    #Single-authored publications
    #limiting author's count to 1 for single authored publications
    limited_authors = count_authors.select("*").filter(col("count")==1).withColumnRenamed('count',"total authors" )
    
    #joining the dataframes
    combined = limited_authors.join(select_years,limited_authors.work_id == select_years.id ,"inner")
    
    #Total single-authored publications
    works_single_authored=combined.groupby("publication_year").count().orderBy(col("publication_year").asc())
    works_single_authored.show(truncate=False)
    
    #Coauthored publications
    #limiting author's count to more than 1 for coauthored publications
    limited_authors = count_authors.select("*").filter(col("count")>1).withColumnRenamed('count',"total authors" )
    
    #joining the dataframes
    combined = limited_authors.join(select_years,limited_authors.work_id == select_years.id ,"inner")
    
    #Total coauthored publications
    works_couthored =combined.groupby("publication_year").count().orderBy(col("publication_year").asc())
    works_couthored.show(truncate=False)


publication_single_vs_multiple()



spark.stop()