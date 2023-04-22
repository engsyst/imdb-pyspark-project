# Get names of people, corresponding movies/series and characters they played in those films.
import pyspark.sql.functions as f

import imdb.pipeline.columns as c
from imdb.ioutil import load
from imdb.pipeline.functions import clean_name_basics, clean_title_principals, clean_title_basics
from imdb.pipeline.schemas import names_schema


# title.basics.tsv.gz
# title.akas.tsv.gz
# title.principals.tsv.gz
# name.basics.tsv.gz

def task4(names_path="resources/name.basics.tsv.gz",
          title_principals_path="resources/title.principals.tsv.gz",
          title_basics_path="resources/title.basics.tsv.gz",
          limit=None):
    names_df = load_names(names_path, limit)
    principals_df = load_principals(title_principals_path, limit)
    titles_df = load_titles(title_basics_path, limit)

    # Get result
    df = titles_df.join(principals_df, c.tb_tconst).join(names_df, c.tp_nconst).orderBy(f.desc(c.tb_tconst))
    return df


def load_names(path, limit):
    df = load(path, schema=names_schema, limit=limit)
    clean_name_basics(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


def load_principals(path, limit):
    df = load(path, limit=limit)  # )  # , schema=principals_schema
    df = clean_title_principals(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


def load_titles(path, limit):
    df = load(path, schema=names_schema, limit=limit)
    df = clean_title_basics(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


if __name__ == "__main__":
    task4(limit=10000)  # .show()
