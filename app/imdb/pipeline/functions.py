from pyspark.sql import DataFrame


def apply_with_columns_func(df: DataFrame, columns, func):
    action_map = {column: func(column) for column in columns}
    return df.withColumns(action_map)
