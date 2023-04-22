# Get information about how many adult movies/series etc. there are per
# region. Get the top 100 of them from the region with the biggest count to
# the region with the smallest one.
import pyspark.sql.functions as f
from pyspark.sql import Window

import imdb.pipeline.columns as c
from imdb.ioutil import load
from imdb.pipeline.functions import clean_title_akas, clean_title_basics
from imdb.pipeline.schemas import title_akas_schema, title_basics_schema


# title.basics.tsv.gz
# title.akas.tsv.gz
# title.principals.tsv.gz
# name.basics.tsv.gz

def task5(title_akas_path="resources/title.akas.tsv.gz",
          title_bacis_path="resources/title.basics.tsv.gz", limit=None):
    akas_df = load_akas(title_akas_path, limit)
    titles_df = load_title_basics(title_bacis_path, limit)

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


def load_akas(path, limit):
    df = load(path, schema=title_akas_schema, limit=limit)
    df = clean_title_akas(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


def load_title_basics(path, limit):
    df = load(path, schema=title_basics_schema, limit=limit)  # , schema=names_schema)
    df = clean_title_basics(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


if __name__ == "__main__":
    task5()  # .show()
