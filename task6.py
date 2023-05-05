from pyspark.sql.functions import *
def count_episodes_per_series(basics_df, episode_df, limit=50):
    series_df = basics_df.join(episode_df, "tconst", "inner")
    series_df = series_df.drop("endYear", "isAdult", "titleType", "startYear", "runtimeMinutes", "parentTconst")
    series_df = series_df.filter(col("seasonNumber").cast("int").isNotNull()) \
        .filter(col("episodeNumber").cast("int").isNotNull())

    series_df = series_df.withColumn("numEpisodes", col("seasonNumber") * col("episodeNumber"))
    series_df = series_df.groupBy("tconst", "primaryTitle") \
        .agg(sum("numEpisodes").alias("totalEpisodes")) \
        .orderBy(desc("totalEpisodes")) \
        .limit(limit)

    #print('6. Count_episodes_per_series')
    #series_df.show()
    series_df.write.write.format('csv').option('header', 'true').mode('overwrite').save(
        'C:\\Users\\Olga\\PycharmProjects\\imdb-spark-project-2\\Result\\Task6.csv')