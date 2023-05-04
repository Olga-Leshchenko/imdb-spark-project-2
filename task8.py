from pyspark.sql.functions import *
from pyspark.sql.window import Window
def get_top_movies_by_genre(basics_df, ratings_df, limit=10):
    movies_df = basics_df.join(ratings_df, "tconst", "inner")
    movies_df.show()
    window_spec = Window.partitionBy("genres").orderBy(col("averageRating").desc())
    ranked_movies_df = movies_df.select(col("tconst"), col("primaryTitle"), col("genres"), col("averageRating"),
                                        dense_rank().over(window_spec).alias("rank"))
    #print('8. get_top_movies_by_genre')
    #ranked_movies_df.show()
    for genre in ranked_movies_df.select("genres").distinct().collect():
        genre_name = genre.genres
        genre_df = ranked_movies_df.filter(col("genres") == genre_name).filter(col("rank") <= limit)
        #print(f"Genre: {genre_name}")
        #genre_df.show(truncate=False)
    ranked_movies_df.write.format('csv').option('header', 'true').mode('overwrite').save('C:\\Users\\Olga\\PycharmProjects\\imdb-spark-project\\Result\\Task8.csv')

