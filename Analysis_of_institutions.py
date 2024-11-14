from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc,count,asc


spark = SparkSession.builder.appName("Institutions").master("local[*]").config("spark.driver.memory", "14g").getOrCreate()



#*****************************Proportion of publications by different institutions*****************************

def publication_institutions():
    
    
    #reading first parquet file 
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #reading second parquet file 
    read_authorship = spark.read.parquet("D:\open_alex_parquet\works_authorships.parquet")
    
    #reading third parquet file 
    read_institutions =spark.read.parquet("D:\open_alex_parquet\institutions.parquet")
    
    # dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    filter_authorship = read_authorship.dropDuplicates(["work_id","institution_id"])
    filter_institution = read_institutions.dropDuplicates(["id"])
    
    #selecting only the required columns
    works = filter_works.select("id","publication_year")
    authorship = filter_authorship.select("work_id","institution_id")
    institution = filter_institution.select("id","type")
    
    #filtering the publication years between 1992 to 2022 
    select_years = works.select("id","publication_year").filter((col("publication_year").between(1992,2022)))
    
    #filtering to exclude NULL values from column "institution_id"
    filter_null_values = authorship.select("work_id","institution_id").filter((col("institution_id").isNotNull()))
    
    #joining the dataframes
    joined_works_authorship = select_years.join(filter_null_values,select_years.id ==  filter_null_values.work_id,"inner")
    
    #performing second join
    joined_works_institution = joined_works_authorship.join(institution,joined_works_authorship.institution_id ==  institution.id,"inner")
    
    #counting the total publications and displaying the result
    joined_works_institution.groupBy("type").count().orderBy(col("type").desc()).show()


publication_institutions()








#**************************************Publication per year by dominant institutions******************************************

def publication_dominant_institutions():
    
    #reading first parquet file 
    read_works = spark.read.parquet("D:\open_alex_parquet\works.parquet")
    
    #reading second parquet file 
    read_authorship = spark.read.parquet("D:\open_alex_parquet\works_authorships.parquet")
    
    #reading third parquet file 
    read_institutions =spark.read.parquet("D:\open_alex_parquet\institutions.parquet")
    
    # dropping duplicates if any
    filter_works = read_works.dropDuplicates(["id"])
    filter_authorship = read_authorship.dropDuplicates(["work_id","institution_id"])
    filter_institution = read_institutions.dropDuplicates(["id"])
    
    #selecting only the required columns
    works = filter_works.select("id","publication_year")
    authorship = filter_authorship.select("work_id","institution_id")
    institution = filter_institution.select("id","type")
    
    #filtering the publication years between 1992 to 2022 
    select_years = works.select("id","publication_year").filter((col("publication_year").between(1992,2022)))
    
    #filtering to exclude NULL values from column "institution_id"
    filter_null_values = authorship.select("work_id","institution_id").filter((col("institution_id").isNotNull()))
    
    #making a list of top 5 institutions
    list_institution =["education","facility","healthcare","company","government" ]
    
    #filtering to include only the top 5 institutions
    filter_institutions = institution.select("id","type").filter((col("type").isin(list_institution)))
    
    #joining the dataframes
    joined_works_authorship = select_years.join(filter_null_values,select_years.id ==  filter_null_values.work_id,"inner")
    
    #performing second join
    joined_works_institution = joined_works_authorship.join(filter_institutions,joined_works_authorship.institution_id ==  filter_institutions.id,"inner")
    
    #counting the total publications and displaying the result
    joined_works_institution.select("*").groupBy("type","publication_year").count().orderBy(col("type"),col("publication_year").asc()).show()
    
    
publication_dominant_institutions()
    
    





spark.stop()
