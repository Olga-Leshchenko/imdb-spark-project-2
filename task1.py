from pyspark.sql.functions import *

def filter_akas_by_region(akas_df):
    ukr_akas_df = akas_df.filter(col("region") == "UA")
    print('1. Filter_akas_by_region')
    #ukr_akas_df.show()
    ukr_akas_df.write.write.write.format('csv').option('header', 'true').mode('overwrite').save(
        'C:\\Users\\Olga\\PycharmProjects\\imdb-spark-project-2\\Result\\Task1.csv')