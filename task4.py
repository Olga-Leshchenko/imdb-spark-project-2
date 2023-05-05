from pyspark.sql.functions import *

def join_principals_and_names(principals_df, name_df):
    joined_df = principals_df.join(name_df, "nconst", "inner")
    filtered_df = joined_df.filter(col("characters").isNotNull())
    result_df = filtered_df.select("primaryName", "category", "characters")
    #print('4. join_principals_and_names')
    #result_df.show()
    result_df.write.write.write.format('csv').option('header', 'true').mode('overwrite').save(
        'C:\\Users\\Olga\\PycharmProjects\\imdb-spark-project-2\\Result\\Task4.csv')