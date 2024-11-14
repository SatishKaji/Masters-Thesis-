from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col,lower,lit,when,asc


spark = SparkSession.builder.appName("Self-citation").master("local[*]").config("spark.driver.memory", "14g").getOrCreate()






#******************************Self-citation analysis***********************************************

def self_citation():
    
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
    
    #joining the dataframes
    combined = citation_years.join(authors,citation_years.referenced_works == authors.work_id ,"inner") 
    
    #Counting self-citations
    self_citation_counts=combined.withColumn("self_citation_count", when((combined.author_id == combined.referenced_author_id), lit(1)).otherwise(lit(0)))
    
    #Displaying the results
    self_citation_counts.groupBy("publication_year").sum("self_citation_count").orderBy(col("publication_year").asc()).show()


self_citation()
    




#****************************************Self-citations main authors vs coauthors*******************************

def self_citation_authorship():
    
    #reading first parquet file 
    read_authorship = spark.read.parquet("D:\open_alex_parquet\works_authorships.parquet")
    
    #reading second parquet file 
    read_citation = spark.read.parquet("D:\open_alex_parquet\self_citation.parquet")
    
    # dropping duplicates if any
    filter_citations = read_citation.dropDuplicates(["work_id","referenced_work_id"])
    filter_authorship = read_authorship.dropDuplicates(["work_id","author_id"])
    
    #selecting only the required columns
    authors = filter_authorship.select("work_id","author_id")
    citation = filter_citations.select("work_id","author_id","author_position","referenced_work_id")
    
    #filtering only the main authors and the publication years between 1992 to 2022 
    main_authors = citation.filter( (lower(col("author_position"))=="first") & ((col("publication_year").between(1992,2022))))
    
    #joining the dataframes
    combined = main_authors.join(authors,main_authors.referenced_works == authors.work_id ,"inner") 
    
    #Counting self-citations
    self_citation_counts=combined.withColumn("self_citation_count", when((combined.author_id == combined.referenced_author_id), lit(1)).otherwise(lit(0)))
    
    #Displaying the results
    self_citation_counts.show()
    
    #filtering only the coauthors and the publication years between 1992 to 2022 
    coauthors = citation.filter((lower(col("author_position"))!="first") & ((col("publication_year").between(1992,2022))))
    
    #joining the dataframes
    combined = coauthors.join(authors,coauthors.referenced_works == authors.work_id ,"inner") 
    
    #Counting self-citations
    self_citation_counts=combined.withColumn("self_citation_count", when((combined.author_id == combined.referenced_author_id), lit(1)).otherwise(lit(0)))
    
    #Displaying the results
    self_citation_counts.show()    


self_citation_authorship()




spark.stop()




