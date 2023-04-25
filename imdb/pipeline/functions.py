from pyspark.shell import spark
from pyspark.sql import DataFrame, functions as f, types as t

from imdb.ioutil import load, save
from imdb.pipeline import columns as c
from imdb.pipeline.schemas import names_schema, title_principals_schema, title_akas_schema, title_episode_schema, \
    title_ratings_schema, title_basics_schema


def apply_with_columns_func(df: DataFrame, columns, func):
    action_map = {column: func(column) for column in columns}
    return df.withColumns(action_map)


def apply_select_func(df: DataFrame, columns, func):
    action_list = [func(column) for column in columns]
    print(type(action_list))
    return df.select(action_list)


def clean_all_columns(df: DataFrame):
    # Apply correct schema
    df = apply_with_columns_func(df, df.columns, lambda cl: f.when(f.col(cl) == "\\N", None)
                                 .otherwise(f.col(cl)))
    return df


def clean_name_basics(df: DataFrame):
    # Apply correct schema
    df = clean_all_columns(df)
    return df


def clean_title_akas(df: DataFrame):
    df = clean_all_columns(df)
    return df


def clean_title_basics(df: DataFrame):
    # Apply correct schema
    df = clean_all_columns(df)
    # df = apply_with_columns_func(df, [c.tb_runtimeMinutes], lambda cl: f.col(cl).cast(t.IntegerType()))
    return df


def clean_title_principals(df: DataFrame):
    # Apply correct schema
    df = clean_all_columns(df)
    df = apply_with_columns_func(df, [c.tp_ordering],
                                 lambda cl: f.col(cl)
                                 .cast(t.IntegerType()))
    return df


def clean_title_episode(df: DataFrame):
    # Apply correct schema
    df = clean_all_columns(df)
    return df


def clean_title_ratings(df: DataFrame):
    # Apply correct schema
    df = clean_all_columns(df)
    return df


def load_name_basics(path, limit):
    df = load(path, schema=names_schema, limit=limit)
    clean_name_basics(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


def load_title_basics(path, limit):
    df = load(path, schema=title_basics_schema, limit=limit)
    df = clean_title_basics(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df


def load_title_principals(path, limit):
    df = load(path, schema=title_principals_schema, limit=limit)  # )  #
    df = clean_title_principals(df)
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


def load_title_ratings(path, limit):
    df = load(path, schema=title_ratings_schema, limit=limit)
    df = clean_title_ratings(df)
    # df.printSchema()
    # df.show(truncate=False)
    return df
