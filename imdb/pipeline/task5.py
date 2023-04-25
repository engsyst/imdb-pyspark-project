# Get information about how many adult movies/series etc. there are per
# region. Get the top 100 of them from the region with the biggest count to
# the region with the smallest one.
import pyspark.sql.functions as f
from pyspark.sql import Window

import imdb.pipeline.columns as c
from imdb.ioutil import save
from imdb.pipeline.functions import load_title_akas, load_title_basics, prepare_less_data_task5, load_title_ratings

TOP_N = "top_n"


def task5(title_akas_path="resources/title.akas.tsv.gz",
          title_basics_path="resources/title.basics.tsv.gz",
          title_ratings_path="resources/title.ratings.tsv.gz",
          top=100, limit=None):
    akas_df = load_title_akas(title_akas_path, limit)
    titles_df = load_title_basics(title_basics_path, limit)
    ratings_df = load_title_ratings(title_ratings_path, limit)

    # Get result
    titles_df = titles_df.filter((f.col(c.tb_isAdult) == 1) & f.col(c.tb_titleType).isNotNull())
    akas_df = akas_df.filter(f.col(c.ta_region).isNotNull())
    r_window = (Window.partitionBy(c.ta_region).orderBy(c.ta_region))
    t_window = (Window.partitionBy(c.ta_region, c.tb_titleType).orderBy(c.ta_region, c.tb_titleType))
    df = akas_df.join(titles_df, f.col(c.tb_tconst) == f.col(c.ta_titleId))
    df = df.withColumn(c.adult_per_region, f.count(f.col(c.tb_tconst)).over(r_window))
    df = df.withColumn(c.adult_per_title_type, f.count(f.col(c.tb_tconst)).over(t_window))
    # it does not support order by future fields
    df = df.orderBy(f.desc(c.adult_per_region), f.desc(c.adult_per_title_type))
    df = df.join(ratings_df, c.tb_tconst)

    window = (Window.partitionBy(c.ta_region, c.tb_titleType)
              .orderBy(f.desc(c.adult_per_region), f.desc(c.adult_per_title_type), f.desc(c.tr_averageRating)))
    df = df.withColumn(TOP_N, f.row_number().over(window))
    df = df.orderBy(f.desc(c.adult_per_region), f.desc(c.adult_per_title_type), f.desc(c.tr_averageRating))
    df = df.where(f.col(TOP_N) <= top)
    # df.explain()
    # df.show(150)
    save(df, "task5")
    return df


if __name__ == "__main__":
    # prepare_less_data_task5()
    task5(top=100)  # .show()

# Region1 countInR1 video countOfVideo titleV1
# ...
# Region1 countInR1 video countOfVideo titleV100
# ...
# Region1 countInR1 series countOfSeries titleS1
# ...
# Region1 countInR1 series countOfSeries titleS100
# ...
# Region2 countInR2 video countOfVideo titleV1
# ...
# Region2 countInR2 video countOfVideo titleV100
# ...
# Region2 countInR2 series countOfSeries titleV1
# ...
# Region2 countInR2 series countOfSeries titleV100
