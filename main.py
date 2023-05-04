from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from load_dataframes import load_dataframes
from task1 import filter_akas_by_region
from task2 import get_names_of_people_born_in_19th_century
from task3 import get_movies_over_two_hours
from task4 import join_principals_and_names
from task5 import get_top_adult_movies
from task6 import count_episodes_per_series
from task7 import get_top_movies_by_decade
from task8 import get_top_movies_by_genre

def main():
    akas_df, name_df, basics_df, principals_df, ratings_df, episode_df = load_dataframes()
    filter_akas_by_region(akas_df)
    get_names_of_people_born_in_19th_century(name_df)
    get_movies_over_two_hours(basics_df)
    join_principals_and_names(principals_df, name_df)
    result_df = get_top_adult_movies(akas_df, basics_df, ratings_df)
    #write_region_movies_to_file(result_df)
    count_episodes_per_series(basics_df, episode_df, limit=50)
    get_top_movies_by_decade(basics_df, ratings_df, limit=10)
    get_top_movies_by_genre(basics_df, ratings_df, limit=10)

if __name__ == '__main__':
    main()
