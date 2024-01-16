from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# Initialize Spark session
spark = SparkSession.builder.appName("GooglePlayStoreAnalysis").getOrCreate()

# Read the dataset
data_path = "path/to/your/dataset.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Apply binning functions

df = df.withColumn("binnedYear", floor((col("Year"))/5)*5+1)
df = df.withColumn("binnedscores", (col("score") / 0.5).cast("int") * 0.5)
df = df.withColumn("binnedPrice", (col("price") / 5).cast("int") * 5)
filtered_df = (df.groupBy("binnedYear", "binnedscores", "binnedPrice").agg(count("*").alias("count")))

# Format the output as specified
output_df = filtered_df.groupBy("binnedYear", "minInstalls").agg(count("*").alias("count"),sum("minInstalls").alias("minInstalls")).selectExpr("CONCAT('Installs=[', minInstalls, ']') AS combination",
                           "CONCAT('Year=[', binnedYear, '-', binnedYear + 4, ']') AS year_range",)


output_path = "path/to/year_install.csv"
output_df.coalesce(1).write.option("header", "true").csv(output_path, sep=";")


output_df = filtered_df.selectExpr("CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS combination",
                           "CONCAT('Year=[', binnedYear, '-', binnedYear + 4, ']') AS year_range")

output_path = "path/to/year_scores.csv"
output_df.coalesce(1).write.option("header", "true").csv(output_path, sep=";")


#Price and scores Combination
price_scores_combination = (filtered_df
                            .selectExpr("CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
                                        "CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores_range"))

output_path = "path/to/price_score.csv"
price_scores_combination .coalesce(1).write.option("header", "true").csv(output_path, sep=";")


#Genre and Installs Combination
genre_installs_combination = (filtered_df
                              .selectExpr("CONCAT('Genre=', genre) AS genre",
                                          "CONCAT('Installs=[', minInstalls, ']') AS installs_range"))
output_path = "path/to/genre_instals.csv"
genre_installs_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

#scores and Installs Combination
scores_installs_combination = (filtered_df
                                .selectExpr("CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores_range",
                                            "CONCAT('Installs=[', minInstalls, ']') AS installs_range"))

output_path = "path/to/score_instals.csv"
scores_installs_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")



#Price and Genre Combination
price_genre_combination = (filtered_df
                           .selectExpr("CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
                                       "CONCAT('Genre=', genre) AS genre"))

output_path = "path/to/price_genre.csv"
price_genre_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

#Price, Genre, and scores Combination
price_genre_scores_combination = (filtered_df
                                   .selectExpr("CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
                                               "CONCAT('Genre=', genre) AS genre",
                                               "CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores_range"))

output_path = "path/to/price_genre_score.csv"
price_genre_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

# Year, Genre, and Installs Combination
year_genre_installs_combination = (filtered_df
                                   .selectExpr("CONCAT('Year=[', binnedYear, '-', binnedYear + 4, ']') AS year_range",
                                               "CONCAT('Genre=', genre) AS genre",
                                               "CONCAT('Installs=[', minInstalls, ']') AS installs_range"))
output_path = "path/to/year_genre_install.csv"
year_genre_installs_combination .coalesce(1).write.option("header", "true").csv(output_path, sep=";")


#scores, Price, and Installs Combination
scores_price_installs_combination = (filtered_df
                                      .selectExpr("CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores_range",
                                                  "CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
                                                  "CONCAT('Installs=[', minInstalls, ']') AS installs_range"))

output_path = "path/to/score_price_install.csv"
scores_price_installs_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

# Year, Price, Genre, and scores Combination
combined_combination = (filtered_df
                        .selectExpr("CONCAT('Year=[', binnedYear, '-', binnedYear + 4, ']') AS year_range",
                                    "CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
                                    "CONCAT('Genre=', genre) AS genre",
                                    "CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores_range"))

# Write the final output to a CSV file
output_path = "path/to/year_price_genre_score.csv"
combined_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")


free_filtered_df=df.filter("free=0").groupBy("binnedYear", "binnedscores", "binnedPrice").agg(count("*").alias("count"))
##############

output_df = free_filtered_df.groupBy("binnedYear", "minInstalls").agg(count("*").alias("count"),sum("minInstalls").alias("minInstalls")).selectExpr("CONCAT('Installs=[', minInstalls, ']') AS combination",
                           "CONCAT('Year=[', binnedYear, '-', binnedYear + 4, ']') AS year_range",)


output_path = "path/to/free_year_install.csv"
output_df.coalesce(1).write.option("header", "true").csv(output_path, sep=";")


output_df = free_filtered_df.selectExpr("CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS combination",
                           "CONCAT('Year=[', binnedYear, '-', binnedYear + 4, ']') AS year_range")

output_path = "path/to/free_year_scores.csv"
output_df.coalesce(1).write.option("header", "true").csv(output_path, sep=";")


#Price and scores Combination
price_scores_combination = (free_filtered_df
                            .selectExpr("CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
                                        "CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores_range"))

output_path = "path/to/free_price_score.csv"
price_scores_combination .coalesce(1).write.option("header", "true").csv(output_path, sep=";")


#Genre and Installs Combination
genre_installs_combination = (free_filtered_df
                              .selectExpr("CONCAT('Genre=', genre) AS genre",
                                          "CONCAT('Installs=[', minInstalls, ']') AS installs_range"))
output_path = "path/to/free_genre_instals.csv"
genre_installs_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

#scores and Installs Combination
scores_installs_combination = (free_filtered_df
                                .selectExpr("CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores_range",
                                            "CONCAT('Installs=[', minInstalls, ']') AS installs_range"))

output_path = "path/to/free_score_instals.csv"
scores_installs_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")



#Price and Genre Combination
price_genre_combination = (free_filtered_df
                           .selectExpr("CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
                                       "CONCAT('Genre=', genre) AS genre"))

output_path = "path/to/free_price_genre.csv"
price_genre_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

#Price, Genre, and scores Combination
price_genre_scores_combination = (free_filtered_df
                                   .selectExpr("CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
                                               "CONCAT('Genre=', genre) AS genre",
                                               "CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores_range"))

output_path = "path/to/free_price_genre_score.csv"
price_genre_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

# Year, Genre, and Installs Combination
year_genre_installs_combination = (free_filtered_df
                                   .selectExpr("CONCAT('Year=[', binnedYear, '-', binnedYear + 4, ']') AS year_range",
                                               "CONCAT('Genre=', genre) AS genre",
                                               "CONCAT('Installs=[', minInstalls, ']') AS installs_range"))
output_path = "path/to/free_year_genre_install.csv"
year_genre_installs_combination .coalesce(1).write.option("header", "true").csv(output_path, sep=";")


#scores, Price, and Installs Combination
scores_price_installs_combination = (free_filtered_df
                                      .selectExpr("CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores_range",
                                                  "CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
                                                  "CONCAT('Installs=[', minInstalls, ']') AS installs_range"))

output_path = "path/to/free_score_price_install.csv"
scores_price_installs_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

# Year, Price, Genre, and scores Combination
combined_combination = (free_filtered_df
                        .selectExpr("CONCAT('Year=[', binnedYear, '-', binnedYear + 4, ']') AS year_range",
                                    "CONCAT('Price=[', binnedPrice, '-', binnedPrice + 5, ']') AS price_range",
                                    "CONCAT('Genre=', genre) AS genre",
                                    "CONCAT('scores=[', binnedscores, '-', binnedscores + 0.5, ']') AS scores_range"))

# Write the final output to a CSV file
output_path = "path/to/free_year_price_genre_score.csv"
combined_combination.coalesce(1).write.option("header", "true").csv(output_path, sep=";")

spark.stop()
