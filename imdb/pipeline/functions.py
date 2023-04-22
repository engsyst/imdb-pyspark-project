from pyspark.pandas import DataFrame
from pyspark.sql import DataFrame, functions as f, types as t

from imdb.pipeline import columns as c


def apply_with_columns_func(df: DataFrame, columns, func):
    action_map = {column: func(column) for column in columns}
    return df.withColumns(action_map)


def count_nulls(df, *col_names):
    nulls_df = df.select(f.count(f.when(f.col(c.ta_title).isNull(), 1)),
                         f.count(f.when(f.col(c.ta_region).isNull(), 1)),
                         f.count(f.when(f.col(c.ta_language).isNull(), 1)),
                         f.count(f.when(f.col(c.ta_types).isNull(), 1)),
                         f.count(f.when(f.col(c.ta_attributes).isNull(), 1)),
                         f.count(f.when(f.col(c.ta_isOriginalTitle).isNull(), 1))
                         )


def clean_title_akas(df: DataFrame):
    df = apply_with_columns_func(df, df.columns, lambda cl: f.when(f.col(cl) == "\\N", None)
                                 .otherwise(f.col(cl)))
    return df


def clean_name_basics(df: DataFrame):
    # Apply correct schema
    df = apply_with_columns_func(df, df.columns, lambda cl: f.when(f.col(cl) == "\\N", None).otherwise(f.col(cl)))
    return df


def clean_title_basics(df: DataFrame):
    # Apply correct schema
    df = apply_with_columns_func(df, df.columns, lambda cl: f.when(f.col(cl) == "\\N", None).otherwise(f.col(cl)))
    df = apply_with_columns_func(df, [c.tb_runtimeMinutes], lambda cl: f.col(cl).cast(t.IntegerType()))
    return df


def clean_title_principals(df: DataFrame):
    # Apply correct schema
    df = apply_with_columns_func(df, df.columns,
                                 lambda cl: f.when(f.col(cl) == "\\N", None)
                                 .otherwise(f.col(cl)))
    df = apply_with_columns_func(df, [c.tp_ordering],
                                 lambda cl: f.col(cl)
                                 .cast(t.IntegerType()))
    return df
