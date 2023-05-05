from pyspark.sql.functions import *

def get_movies_over_two_hours(basics_df):
    movies_df = basics_df.filter((col("titleType") == "movie") & (col("runtimeMinutes") >= 120))
    print('3. Get_movies_over_two_hours')
    # movies_df.show()
    #movies_df.select('primaryTitle', 'originalTitle').show()
    movies_df.write.format('csv').option('header', 'true').mode('overwrite').save(
        'C:\\Users\\Olga\\PycharmProjects\\imdb-spark-project-2\\Result\\Task3.csv')