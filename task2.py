from pyspark.sql.functions import *

def get_names_of_people_born_in_19th_century(name_df):
    people_df = name_df.filter((col("birthYear") >= 1800) & (col("birthYear") < 1900))
    print('2. Get_names_of_people_born_in_19th_century')
    # people_df.show()
    #people_df.select(col('primaryName')).show()
    people_df.write.write.write.format('csv').option('header', 'true').mode('overwrite').save(
        'C:\\Users\\Olga\\PycharmProjects\\imdb-spark-project-2\\Result\\Task2.csv')