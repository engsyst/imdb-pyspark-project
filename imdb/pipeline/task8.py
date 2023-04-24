# Get information about how many adult movies/series etc. there are per
# region. Get the top 100 of them from the region with the biggest count to
# the region with the smallest one.

import pyspark.sql.functions as f
from pyspark.sql import Window

import imdb.pipeline.columns as c
from imdb.pipeline.functions import load_title_ratings, load_title_basics

TOP_TEN = "top_ten"
DECADE = "decade"
PERIOD = 10


def task8(title_basics_path="resources/title.basics.tsv.gz",
          title_ratings_path="resources/title.ratings.tsv.gz",
          limit=None):
    basics_df = load_title_basics(title_basics_path, limit)
    df = basics_df.withColumn(c.tb_genres, f.explode(f.split(f.col(c.tb_genres), ",", limit=-1)))

    ratings_df = load_title_ratings(title_ratings_path, limit)
    df = df.join(ratings_df.select(c.tr_tconst, c.tr_averageRating), c.tb_tconst)
    window = (Window
              .orderBy(f.desc(c.tb_genres), c.tb_titleType, f.desc(c.tr_averageRating))
              .partitionBy(c.tb_genres, c.tb_titleType)
              )
    df = df.withColumn(TOP_TEN, f.row_number().over(window)).where(f.col(TOP_TEN) <= 10)
    df.show(150)
    return df


if __name__ == "__main__":
    task8(limit=10000)  # .show()
