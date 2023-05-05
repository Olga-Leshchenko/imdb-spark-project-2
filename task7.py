from pyspark.sql.functions import *
def get_top_movies_by_decade(basics_df, ratings_df, limit=10):
    basics_df = basics_df.withColumn("decade", floor(col("startYear") / 10) * 10)
    title_df = basics_df.join(ratings_df, "tconst", "inner")
    #title_df.show()
    decade_ratings_df = title_df.groupBy("decade").agg(avg("averageRating").alias("averageRating"))
    print('7. get_top_movies_by_decade')
    for decade in decade_ratings_df.select("decade").orderBy("decade").collect():
        decade_value = decade["decade"]
        decade_df = title_df.filter(col("decade") == decade_value).orderBy(desc("averageRating")).limit(limit)
        #print(f"Decade: {decade_value}s")
        #decade_df.select("primaryTitle", "averageRating").show(truncate=False)

    decade_ratings_df.write.format('csv').option('header', 'true').mode('overwrite').save(
        'C:\\Users\\Olga\\PycharmProjects\\imdb-spark-project-2\\Result\\Task7.csv')