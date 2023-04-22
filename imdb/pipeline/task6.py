# Get information about how many adult movies/series etc. there are per
# region. Get the top 100 of them from the region with the biggest count to
# the region with the smallest one.

import pyspark.sql.functions as f

import imdb.pipeline.columns as c
from imdb.ioutil import load
from imdb.pipeline.functions import clean_title_basics, clean_title_akas, clean_title_episode
from imdb.pipeline.schemas import title_basics_schema, title_akas_schema, title_episode_schema


def task6(title_basics_path="resources/title.basics.tsv.gz",
          title_episode_path="resources/title.episode.tsv.gz",
          limit=None):
    basics_df = load_title_basics(title_basics_path, limit)
    episode_df = load_title_episode(title_episode_path, limit)
    grouped_episode_df = episode_df.groupBy(c.te_parentTconst).count().orderBy(f.desc("count"))
    # to reduce time filter before join
    df = grouped_episode_df \
        .join(basics_df.filter(f.col(c.tb_titleType) == "tvSeries"), f.col(c.te_parentTconst) == f.col(c.tb_tconst)) \
        .orderBy(f.desc("count"), c.tb_originalTitle)
    df.show()
    return df


def load_title_basics(path, limit):
    df = load(path, schema=title_basics_schema, limit=limit)  # , schema=names_schema)
    df = clean_title_basics(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


def load_title_akas(path, limit):
    df = load(path, schema=title_akas_schema, limit=limit)
    df = clean_title_akas(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


def load_title_episode(path, limit):
    df = load(path, schema=title_episode_schema, limit=limit)
    df = clean_title_episode(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


if __name__ == "__main__":
    task6()  # .show()
