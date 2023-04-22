# Get information about how many adult movies/series etc. there are per
# region. Get the top 100 of them from the region with the biggest count to
# the region with the smallest one.
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

import imdb.pipeline.columns as c
from imdb.ioutil import load
from imdb.pipeline.functions import apply_with_columns_func
from test.pipline.test_dataset import test_data_complaince


# title.basics.tsv.gz
# title.akas.tsv.gz
# title.principals.tsv.gz
# name.basics.tsv.gz

def task5(limit=None):
    akas_df = load_akas(limit)
    titles_df = load_titles(limit)

    # Get result
    window = Window.partitionBy(c.ta_region)  # .orderBy(f.desc(c.adult_per_region))
    akas_df = akas_df.filter(f.col(c.ta_region).isNotNull())
    titles_df = titles_df.filter(f.col(c.tb_isAdult) == 1)
    df = akas_df.join(titles_df, f.col(c.tb_tconst) == f.col(c.ta_titleId)) \
        .filter(f.col(c.ta_region).isNotNull()) \
        .withColumn(c.adult_per_region, f.count(f.col(c.ta_region)).over(window)) \
        .orderBy(f.desc(c.adult_per_region))
    df.explain()
    df.show(100)
    return df


def load_akas(limit):
    df = load("resources/title.akas.tsv.gz", limit=limit)  # , schema=names_schema)
    # Check bad data
    # test_data_complaince(df, c.nb_primaryName, t.StringType(), "\\N")
    df.show(truncate=False)
    # Apply correct schema
    df = apply_with_columns_func(df, df.columns,
                                 lambda cl: f.when(f.col(cl) == "\\N", None)
                                 .otherwise(f.col(cl)))
    df = apply_with_columns_func(df, [c.ta_ordering, c.ta_isOriginalTitle],
                                 lambda cl: f.col(cl)
                                 .cast(t.IntegerType()))
    df.printSchema()
    # df.show(truncate=False)
    return df


def load_titles(limit):
    df = load("resources/title.basics.tsv.gz", limit=limit)  # , schema=names_schema)
    # Check bad data
    # test_data_complaince(df, c.nb_primaryName, t.StringType(), "\\N")
    df.show(truncate=False)
    # Apply correct schema
    df = apply_with_columns_func(df, df.columns,
                                 lambda cl: f.when(f.col(cl) == "\\N", None)
                                 .otherwise(f.col(cl)))
    df = apply_with_columns_func(df, [c.tb_startYear, c.tb_endYear, c.tb_runtimeMinutes],
                                 lambda cl: f.col(cl)
                                 .cast(t.IntegerType()))
    df.printSchema()
    # df.show(truncate=False)
    return df


if __name__ == "__main__":
    task5()  # .show()
