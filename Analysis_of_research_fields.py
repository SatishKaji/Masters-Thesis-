from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc,count,asc



spark = SparkSession.builder.appName("Research fields").master("local[*]").config("spark.driver.memory", "14g").getOrCreate()




#****************************************Domninant research fields according to publication count*****************************************

def research_field_publications():
     
    #reading first parquet file 
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #reading second parquet file 
    read_works_concepts = spark.read.parquet("D:\open_alex_parquet\works_concepts.parquet")
    
    # dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    filter_works_concepts = read_works_concepts.dropDuplicates(["work_id","concept_id"])
    
    #selecting only the required columns
    works = filter_works.select("id","publication_year")
    works_concepts = filter_works_concepts.select("work_id","concept_id","display_name")
    
    #filtering the publication years between 1992 to 2022 
    select_years = works.select("id","publication_year").filter(col("publication_year").between(1992,2022))
    
    #joining the dataframes
    combined = works_concepts.join(select_years,works_concepts.work_id == select_years.id ,"inner")
    
    #counting the total publications and displaying the result
    combined.groupBy("concept_id","display_name").count().orderBy(col("count").desc()).show()
    
    
research_field_publications()





#********************************Publication per year in the top 5 research fields********************

def publication_top_research_field():
    
    
    #reading first parquet file 
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #reading second parquet file 
    read_works_concepts = spark.read.parquet("D:\open_alex_parquet\works_concepts.parquet")
    
    # dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    filter_works_concepts = read_works_concepts.dropDuplicates(["work_id","concept_id"])
    
    #selecting only the required columns
    works = filter_works.select("id","publication_year")
    works_concepts = filter_works_concepts.select("work_id","concept_id","display_name")
    
    #making a list of top 5 dominant research fields
    concepts_list=["https://openalex.org/C17744445","https://openalex.org/C71924100",
                   "https://openalex.org/C41008148","https://openalex.org/C121332964","https://openalex.org/C86803240"]
    
    #filtering the publication years between 1992 to 2022 
    select_years = works.select("id","publication_year").filter(col("publication_year").between(1992,2022))
    
    #selecting only the top 5 concepts
    top_concepts = works_concepts.select("work_id","concept_id").filter(col("concept_id").isin(concepts_list))
    
    #joining the dataframes
    combined = works_concepts.join(select_years,works_concepts.work_id == select_years.id ,"inner")
    
    #counting the total publications and displaying the result
    combined.groupBy("concept_id","publication_year").count().orderBy(col("concept_id"),col("publication_year").asc()).show()



publication_top_research_field()





#*********************************Dominant research field according to citation count**********************************

def research_field_citations():

    #reading first parquet file 
    read_works_concepts = spark.read.parquet("D:\open_alex_parquet\works_concepts.parquet")
    
    #reading second parquet file 
    read_citation = spark.read.parquet("D:\open_alex_parquet\citation.parquet")
    
    # dropping duplicates if any
    filter_works_concepts = read_works_concepts.dropDuplicates(["work_id","concept_id"])
    filter_citations = read_citation.dropDuplicates(["work_id","referenced_work_id"])
    
    #selecting only the required columns
    citation = filter_citations.select("work_id","publication_year","referenced_work_id")
    works_concepts = filter_works_concepts.select("work_id","concept_id","display_name")
    
    #filtering the publication years between 1992 to 2022 
    citation_years = citation.filter(col("publication_year").between(1992,2022))
    
    #joining the dataframes
    combined = citation_years.join(works_concepts,citation_years.referenced_work_id == works_concepts.work_id ,"inner")
    
    #counting the total publications and displaying the result
    combined.groupBy("concept_id","display_name").count().orderBy(col("count").desc()).show()
    

research_field_citations()




#********************************Total citations per year in the top 5 research field******************


def citation_top_research_field():
    
    #reading first parquet file 
    read_works_concepts = spark.read.parquet("D:\open_alex_parquet\works_concepts.parquet")
    
    #reading second parquet file 
    read_citation = spark.read.parquet("D:\open_alex_parquet\citation.parquet")
    
    # dropping duplicates if any
    filter_works_concepts = read_works_concepts.dropDuplicates(["work_id","concept_id"])
    filter_citations = read_citation.dropDuplicates(["work_id","referenced_work_id"])
    
    #selecting only the required columns
    citation = filter_citations.select("work_id","publication_year","referenced_work_id")
    works_concepts = filter_works_concepts.select("work_id","concept_id","display_name")
    
    #making a list of top 5 dominant research fields
    concepts_list=["https://openalex.org/C185592680","https://openalex.org/C71924100",
                   "https://openalex.org/C41008148","https://openalex.org/C121332964","https://openalex.org/C86803240"]
    
    #filtering the publication years between 1992 to 2022 
    citation_years = citation.filter(col("publication_year").between(1992,2022))
    
    #selecting only the top 5 concepts
    top_concepts = works_concepts.select("work_id","concept_id").filter(col("concept_id").isin(concepts_list))
    
    #joining the dataframes
    combined = citation_years.join(works_concepts,citation_years.referenced_work_id == works_concepts.work_id ,"inner")
    
    #counting the total publications and displaying the result
    combined.groupBy("concept_id","publication_year").count().orderBy(col("concept_id"),col("publication_year").asc()).show()


citation_top_research_field()



spark.stop()
