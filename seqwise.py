from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# Initialize Spark session
spark = SparkSession.builder.appName("GooglePlayStoreAnalysis").getOrCreate()

# Read the dataset
data_path = "path/to/your/dataset.csv"
Dataset=spark.read.option("header","True").option("inferSchema",True).csv("dbfs:/SampleFiles/google_play_dataset_by_tapivedotcom.csv")

#Data Cleaning
Dataset=Dataset.withColumn("check_fields",lit(""))

Dataset=Dataset.withColumn("check_fields", when(col("free").isin([0,1,'TRUE','FALSE']), col("check_fields")).otherwise(concat(col("check_fields"), lit(",free")))).withColumn("free",trim(col("free")).cast("int"))
Dataset = Dataset.withColumn("check_fields", when(col("genre").rlike("^[A-Za-z& ]+$"), col("check_fields")).otherwise(concat(col("check_fields"), lit(",genre"))))
Dataset = Dataset.withColumn("check_fields", when(col("genreId").rlike("^[A-Za-z_ ]+$"), col("check_fields")).otherwise(concat(col("check_fields"), lit(",genreId"))))
Dataset = Dataset.withColumn("check_fields", when(col("price").rlike("^[0-9 ]+(?:\.[0-9]+)?$"),col("check_fields")).otherwise(concat(col("check_fields"), lit(",price")))).withColumn("price",trim(col("price")).cast("double"))
Dataset = Dataset.withColumn("check_fields", when(col("ratings").rlike("^[0-9 ]+$"),col("check_fields")).otherwise(concat(col("check_fields"), lit(",ratings")))).withColumn("ratings",trim(col("ratings")).cast("int"))
Dataset = Dataset.withColumn("check_fields", when(col("len screenshots").rlike("^[0-9 ]+$"),col("check_fields")).otherwise(concat(col("check_fields"), lit(",len screenshots")))).withColumn("len screenshots",trim(col("len screenshots")).cast("int"))
Dataset=Dataset.withColumn("check_fields", when(col("adSupported").isin([0,1,'TRUE','FALSE']), col("check_fields")).otherwise(concat(col("check_fields"), lit(",adSupported")))).withColumn("adSupported",trim(col("adSupported")).cast("int"))
Dataset=Dataset.withColumn("check_fields", when(col("containsAds").isin([0,1,'TRUE','FALSE']), col("check_fields")).otherwise(concat(col("check_fields"), lit(",containsAds")))).withColumn("containsAds",trim(col("containsAds")).cast("int"))
Dataset=Dataset.withColumn("check_fields", when(col("reviews").rlike("^[0-9 ]+$"), col("check_fields")).otherwise(concat(col("check_fields"), lit(",reviews")))).withColumn("reviews",trim(col("reviews")).cast("int"))
Dataset=Dataset.withColumn("check_fields", when(col("releasedMonth").isin(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']), col("check_fields")).otherwise(concat(col("check_fields"), lit(",releasedMonth"))))


CleanDataSet=Dataset.filter("trim(check_fields) = ''")

#Analysis
#1)Genre and its count
genre_count_combination = (CleanDataSet.groupBy("genre").agg(count("*").alias("count"))).selectExpr("CONCAT('Genre=', genre) AS genre","count")
output_path = "path/to/Genre_count.csv"
genre_count_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

#2)Genre and its install count
genre_installs_combination = (CleanDataSet.withColumn("Installs", (col("minInstalls") / 10000).cast("int") * 10000).groupBy("genre","Installs").agg(count("*").alias("count"))).selectExpr("CONCAT('Genre=', genre) AS genre","CONCAT('Installs=[', Installs, '-', Installs + 10000, ']') AS Installs")
output_path = "path/to/Genre_install_count.csv"
genre_installs_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

#3)Genre and its year and install count
Temp_df=CleanDataSet.withColumn("Installs", (col("minInstalls") / 10000).cast("int") * 10000).withColumn("binnedYear", floor((col("releasedYear"))/5)*5+1)
genre_installs_year_combination = (Temp_df.groupBy("genre","Installs","binnedYear").agg(count("*").alias("count"))).selectExpr("CONCAT('Genre=', genre) AS genre","CONCAT('Installs=[', Installs, '-', Installs + 10000, ']') AS Installs","CONCAT('Year=[', binnedYear, '-', binnedYear + 4, ']') AS year_range")
output_path = "path/to/Genre_year_install_count.csv"
genre_installs_year_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")


#4)Genre and its year and scores install count
Temp_df=CleanDataSet.withColumn("Installs", (col("minInstalls") / 10000).cast("int") * 10000)
.withColumn("binnedYear", floor((col("releasedYear"))/5)*5+1)
.withColumn("binnedscores", (col("score") / 0.5).cast("int") * 0.5)
genre_installs_year_combination = (Temp_df.groupBy("genre","Installs","binnedYear").agg(count("*").alias("count")))
.selectExpr("CONCAT('Genre=', genre) AS genre","CONCAT('Installs=[', Installs, '-', Installs + 10000, ']') AS Installs"
,"CONCAT('Year=[', binnedYear, '-', binnedYear + 4, ']') AS year_range"
,"CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores")
output_path = "path/to/Genre_year_install_scores_count.csv"
genre_installs_year_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")


#5 genre and ad support
genre_adsupport=CleanDataSet.groupby("genre","adSupported").agg(count("*").alias("count")).selectExpr(
"CONCAT('Genre=', genre) AS genre",
"adSupported","count"
)
output_path = "path/to/Genre_adsupported.csv"
genre_adsupport.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

#6 adsupported and price
adSupported_price=CleanDataSet
.withColumn("binnedPrice", (col("price") / 5).cast("int") * 5)
.groupBy("binnedPrice","adSupported").agg(count("*").alias("count").selectExpr(
"CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
"adSupported","count"
)

#7-10score and review and genre

tempDf=CleanDataSet.withColumn("binnedscores", (col("score") / 0.5).cast("int") * 0.5)
.withColumn("Binnedreview", (col("review") / 100).cast("int") * 100)
.withColumn("binnedYear", floor((col("Year"))/5)*5+1)
.withColumn("binnedPrice", (col("price") / 5).cast("int") * 5)
.groupBy("binnedscores","Binnedreview","genre","binnedYear","binnedPrice","adSupported")

score_review=tempDf.selectExpr(
"CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores")
,"CONCAT('review=[', binnedreview, '-', binnedreview + 100, ']') AS review_range"
)

score_review_price=tempDf.selectExpr(
"CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
,"CONCAT('review=[', binnedreview, '-', binnedreview + 100, ']') AS review_range"
,"CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores")
)

score_review_genre=tempDf.selectExpr(
"CONCAT('Genre=', genre) AS genre",
,"CONCAT('review=[', binnedreview, '-', binnedreview + 100, ']') AS review_range"
,"CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores")
)


##########

can get developer and genere
can find the co relation between developer and developer_id and if co relation is greater than 0.8.
can consider the develoepr id and get the those details.

ex:
cleaneddeveloperid=CleanDataSet.select("*",corr("developerId", "minInstalls") as corr).filter("corr>0.7").drop("corr")

developer and genere.

developer_genere=cleaneddeveloperid.groupBy("develoepr,"genere","adSupported").agg(count("*").alias("count")).selectExpr(
"CONCAT('develoepr=', genre) AS genre",
"CONCAT('develoepr=', develoepr) AS develoepr",
"count"
)

#developer who is mointizeing and no of apps.
developer_genere=cleaneddeveloperid.groupBy("develoepr,"genere","adSupported").agg(count("*").alias("count")).selectExpr(
"CONCAT('develoepr=', develoepr) AS develoepr",
"CONCAT('adSupported=', adSupported) AS adSupported",
"count"
)
#############
filter on free we get another comibination.
above 13 stats * 2= 26 stats.



spark.stop()
