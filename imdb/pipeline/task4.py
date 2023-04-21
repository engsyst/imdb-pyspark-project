# Get names of people, corresponding movies/series and characters they played in those films.
import pyspark.sql.functions as f
import pyspark.sql.types as t

import imdb.pipeline.columns as c
from imdb.ioutil import load
from imdb.pipeline.functions import apply_with_columns_func, test_data_complaince


# title.basics.tsv.gz
# title.akas.tsv.gz
# title.principals.tsv.gz
# name.basics.tsv.gz

def task4(limit=None):
    names_df = load_names(limit)
    principals_df = load_principals(limit)
    titles_df = load_titles(limit)

    # Get result
    df = titles_df.join(principals_df, c.tb_tconst).join(names_df, c.tp_nconst).orderBy(f.desc(c.tb_tconst))
    return df


def load_names(limit):
    names_df = load("resources/name.basics.tsv.gz", limit=limit)  # , schema=names_schema)
    # Check bad data
    # test_data_complaince(names_df, c.nb_primaryName, t.StringType(), "\\N")
    names_df.show(truncate=False)
    # Apply correct schema
    names_df = apply_with_columns_func(names_df, names_df.columns,
                                       lambda cl: f.when(f.col(cl) == "\\N", None)
                                       .otherwise(f.col(cl)))
    names_df = apply_with_columns_func(names_df, [c.nb_birthYear, c.nb_deathYear],
                                       lambda cl: f.col(cl)
                                       .cast(t.IntegerType()))
    names_df.printSchema()
    # names_df.show(truncate=False)
    return names_df


def load_principals(limit):
    df = load("resources/title.principals.tsv.gz", limit=limit)  # , schema=names_schema)
    # Check bad data
    # test_data_complaince(df, c.nb_primaryName, t.StringType(), "\\N")
    df.show(truncate=False)
    # Apply correct schema
    df = apply_with_columns_func(df, df.columns,
                                 lambda cl: f.when(f.col(cl) == "\\N", None)
                                 .otherwise(f.col(cl)))
    df = apply_with_columns_func(df, [c.tp_ordering],
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
    df = apply_with_columns_func(df, [c.tb_isAdult, c.tb_startYear, c.tb_endYear, c.tb_runtimeMinutes],
                                 lambda cl: f.col(cl)
                                 .cast(t.IntegerType()))
    df.printSchema()
    # df.show(truncate=False)
    return df


if __name__ == "__main__":
    task4(limit=10000)  # .show()
