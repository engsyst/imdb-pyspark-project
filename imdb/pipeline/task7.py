# Get 10 titles of the most popular movies/series etc. by each decade.
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

import imdb.pipeline.columns as c
from imdb.ioutil import save
from imdb.pipeline.functions import load_title_ratings, load_title_basics

TOP_TEN = "top_ten"
DECADE = "decade"
PERIOD = 10


def task7(title_basics_path="resources/title.basics.tsv.gz",
          title_ratings_path="resources/title.ratings.tsv.gz",
          limit=None):
    basics_df = load_title_basics(title_basics_path, limit)
    basics_df = basics_df.filter(f.col(c.tb_startYear).isNotNull())
    ratings_df = load_title_ratings(title_ratings_path, limit)
    df = basics_df.join(ratings_df.select(c.tr_tconst, c.tr_averageRating), c.tb_tconst)
    df = df.withColumn(DECADE, f.floor(f.col(c.tb_startYear) / PERIOD).cast(t.IntegerType()))
    window = Window.partitionBy(DECADE).orderBy(f.desc(c.tr_averageRating))
    df = df.withColumn(TOP_TEN, f.row_number().over(window)).where(f.col(TOP_TEN) <= 10)
    df.show()
    save(df, "task7")
    return df


if __name__ == "__main__":
    task7()  # .show()
