from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lower,count,asc
from pyspark.sql.types import FloatType,DecimalType


spark = SparkSession.builder.appName("Citation").master("local[*]").config("spark.driver.memory", "14g").getOrCreate()






 # *************************Citations per year***********************
 
def citations_per_year():
    
    #reading parquet file 
    read_citation = spark.read.parquet("D:\open_alex_parquet\citation.parquet")
    
    #dropping duplicates if any
    filter_citations = read_citation.dropDuplicates(["work_id","referenced_work_id"])
    
    #selecting only the required columns
    citation = filter_citations.select("work_id","publication_year","referenced_work_id")
    
    #filtering by publication years between 1992 to 2022 and displaying the results
    citation.filter((col("publication_year").between(1992,2022))).groupby("publication_year").count().orderBy(col("publication_year").asc()).show(truncate=False)
    
citations_per_year()



#*****************************Citations on openly accessible publications vs other publications******************************************

def citations_oa_vs_others():
    
    #reading first parquet file 
    read_citation = spark.read.parquet("D:\open_alex_parquet\citation.parquet")
    
    #reading second parquet file 
    read_works_oa = spark.read.parquet("D:\open_alex_parquet\works_open_access.parquet")
    
    #dropping duplicates if any
    filter_citations = read_citation.dropDuplicates(["work_id","referenced_work_id"])
    filter_works_oa = read_works_oa.dropDuplicates(["work_id"])
    
    #selecting only the required columns
    citation = filter_citations.select("work_id","publication_year","referenced_work_id")
    works_oa = filter_works_oa.select("work_id","is_oa")
    
    #filtering by publication years between 1992 to 2022 
    citation_years = citation.filter((col("publication_year").between(1992,2022)))
    
    #joining the data frames
    combined = citation_years.join(works_oa,citation_years.referenced_work_id ==  works_oa.work_id,"inner") 
    
    #citations for openly accessible works
    citations_works_oa=combined.filter((lower(col('is_oa'))=="true")).groupby("publication_year").count().orderBy(col("publication_year").asc())
    
    #displaying the result for openly accessible works
    citations_works_oa.show(truncate=False)
    
    #citations for works not openly accessible
    citations_works_non_oa=combined.filter((lower(col('is_oa'))=="false")).groupby("publication_year").count().orderBy(col("publication_year").asc())
    
    #displaying the result for works not openly accessible
    citations_works_non_oa.show(truncate=False)


citations_oa_vs_others()





#********************Citations on single-authored vs coauthored publications************************************

def citation_single_vs_multiple():
    
    #reading first parquet file 
    read_authorship = spark.read.parquet("D:\open_alex_parquet\works_authorships.parquet")
    
    #reading second parquet file 
    read_citation = spark.read.parquet("D:\open_alex_parquet\citation.parquet")
    
    #dropping duplicates if any
    filter_citations = read_citation.dropDuplicates(["work_id","referenced_work_id"])
    filter_authorship = read_authorship.dropDuplicates(["work_id","author_id"])
    
    #selecting only the required columns
    authors = filter_authorship.select("work_id","author_id")
    citation = filter_citations.select("work_id","publication_year","referenced_work_id")
    
    #filtering by publication years between 1992 to 2022 and displaying the results
    citation_years = citation.filter((col("publication_year").between(1992,2022)))
    
    #counting the number of authors for each publication
    count_authors = authors.groupBy("work_id").count()
    
    #Citations on single-authored publications 
    #limiting author's count to 1 for single authored publications
    limited_authors = count_authors.select("*").filter(col("count")==1).withColumnRenamed('count',"total authors" )
    
    #joining the dataframes
    combined = limited_authors.join(citation_years,limited_authors.work_id == citation_years.referenced_work_id ,"inner")
    
    #Total citations on single-authored publications
    citations_single_authored=combined.groupby("publication_year").count().orderBy(col("publication_year").asc())
    citations_single_authored.show(truncate=False)
    
    #Citations on coauthored publications
    #limiting author's count to more than 1 for coauthored publications
    limited_authors = count_authors.select("*").filter(col("count")>1).withColumnRenamed('count',"total authors" )
    
    #joining the dataframes
    combined = limited_authors.join(citation_years,limited_authors.work_id == citation_years.referenced_work_id ,"inner")
    
    #Total citations on coauthored publications
    citations_couthored =combined.groupby("publication_year").count().orderBy(col("publication_year").asc())
    citations_couthored.show(truncate=False)

citation_single_vs_multiple()






spark.stop()