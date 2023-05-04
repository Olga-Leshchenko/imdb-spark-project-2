from pyspark.sql.functions import *
from pyspark.sql.window import Window

def get_top_adult_movies(akas_df, basics_df, ratings_df, orderBy=None):
    adult_basics_df = basics_df.filter(col("isAdult") == 1)
    akas_df = akas_df.withColumnRenamed("titleId", "tconst")
    adult_movies_df = adult_basics_df.join(akas_df, "tconst", "inner") \
        .join(ratings_df, "tconst", "inner")

    # Групуємо дані за регіоном та обчислюємо 20 фільмів з найвищим рейтингом
    window_spec = Window.partitionBy("region").orderBy(col("averageRating").desc())
    top_movies_df = adult_movies_df.withColumn("rank", rank().over(window_spec)).filter(col("rank") <= 10)
    #print('5. get_top_adult_movies')
    for region in top_movies_df.select("region").distinct().collect():
        region_name = region.region
        region_df = top_movies_df.filter(col("region") == region_name).orderBy("rank")
        #print(f"Region: {region_name}")
        #region_df.select('primaryTitle', 'originalTitle', 'averageRating').show()
    top_movies_df.write.write.format('csv').option('header', 'true').mode('overwrite').save(
        'C:\\Users\\Olga\\PycharmProjects\\imdb-spark-project\\Result\\Task5.csv')