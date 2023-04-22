# Get information about how many adult movies/series etc. there are per
# region. Get the top 100 of them from the region with the biggest count to
# the region with the smallest one.

import pyspark.sql.functions as f
from pyspark.sql import Window
import pyspark.sql.types as t

import imdb.pipeline.columns as c
from imdb.ioutil import load
from imdb.pipeline.functions import clean_title_basics, clean_title_ratings
from imdb.pipeline.schemas import title_basics_schema, title_ratings_schema

TOP_TEN = "top_ten"
DECADE = "decade"
PERIOD = 10


def task7(title_basics_path="resources/title.basics.tsv.gz",
          title_ratings_path="resources/title.ratings.tsv.gz",
          limit=None):
    basics_df = load_title_basics(title_basics_path, limit)
    ratings_df = load_title_ratings(title_ratings_path, limit)
    df = basics_df.join(ratings_df.select(c.tr_tconst, c.tr_averageRating), c.tb_tconst)
    df = df.withColumn(DECADE, f.floor(f.col(c.tb_startYear) / PERIOD).cast(t.IntegerType()))
    window = Window.partitionBy(DECADE).orderBy(f.desc(c.tr_averageRating))
    df = df.withColumn(TOP_TEN, f.row_number().over(window)).where(f.col(TOP_TEN) <= 10)
    df.show()
    return df


def load_title_basics(path, limit):
    df = load(path, schema=title_basics_schema, limit=limit)
    df = clean_title_basics(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


def load_title_ratings(path, limit):
    df = load(path, schema=title_ratings_schema, limit=limit)
    df = clean_title_ratings(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


if __name__ == "__main__":
    task7(limit=1000)  # .show()
