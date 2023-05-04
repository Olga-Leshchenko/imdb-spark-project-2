from pyspark.sql import SparkSession

def load_dataframes():
    # ініціалізуємо SparkSession
    spark = SparkSession.builder.appName("Task").getOrCreate()

    # завантажуємо датафрейм title.akas.tsv.gz
    path = "E:\\Olga\\1 Shevchenka\\Python Corses\\DataSet\\title.akas.tsv.gz"
    akas_df = spark.read.option("header", True).option("sep", "\t").csv(path)
    print('датафрейм title.akas.tsv.gz')
    #akas_df.show(truncate=False)

    # завантажуємо датафрейм name.basics.tsv.gz
    path1 = "E:\\Olga\\1 Shevchenka\\Python Corses\\DataSet\\name.basics.tsv.gz"
    name_df = spark.read.option("header", True).option("sep", "\t").csv(path1)
    print('датафрейм name.basics.tsv.gz')
    #name_df.show(truncate=False)

    # завантажуємо датафрейм title.basics.tsv.gz
    path3 = "E:\\Olga\\1 Shevchenka\\Python Corses\\DataSet\\title.basics.tsv.gz"
    basics_df = spark.read.option("header", True).option("sep", "\t").csv(path3)
    print('датафрейм title.basics.tsv.gz')
    #basics_df.show(truncate=False)

    # завантажуємо датафрейм title.principals.tsv.gz
    path4 = "E:\\Olga\\1 Shevchenka\\Python Corses\\DataSet\\title.principals.tsv.gz"
    principals_df = spark.read.option("header", True).option("sep", "\t").csv(path4)
    print('датафрейм title.principals.tsv.gz')
    #rincipals_df.show(truncate=False)

    # завантажуємо датафрейм title.ratings.tsv.gz
    path5 = "E:\\Olga\\1 Shevchenka\\Python Corses\\DataSet\\title.ratings.tsv.gz"
    ratings_df = spark.read.option("header", True).option("sep", "\t").csv(path5)
    print('датафрейм title.ratings.tsv.gz')
    #ratings_df.show(truncate=False)

    # завантажуємо датафрейм title.episode.tsv.gz
    path6 = "E:\\Olga\\1 Shevchenka\\Python Corses\\DataSet\\title.episode.tsv.gz"
    episode_df = spark.read.option("header", True).option("sep", "\t").csv(path6)
    print('датафрейм title.episode.tsv.gz')
    #episode_df.show(truncate=False)

    return akas_df, name_df, basics_df, principals_df, ratings_df, episode_df
