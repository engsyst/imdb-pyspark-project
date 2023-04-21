# Get titles of all movies that last more than 2 hours.
import pyspark.sql.functions as f
import pyspark.sql.types as t

import imdb.pipeline.columns as c
from imdb.ioutil import load
from imdb.pipeline.functions import apply_with_columns_func, test_data_complaince


def task3(path="resources/title.basics.tsv.gz", limit=None):
    df = load(path, limit=limit)  # , schema=names_schema)

    # Check bad data
    test_data_complaince(df, c.tb_runtimeMinutes, t.IntegerType(), "\\N")

    # Apply correct schema
    df = apply_with_columns_func(df, df.columns, lambda cl: f.when(f.col(cl) == "\\N", None).otherwise(f.col(cl)))
    df = apply_with_columns_func(df, [c.tb_runtimeMinutes], lambda cl: f.col(cl).cast(t.IntegerType()))

    # Get result
    df = df.filter(f.col(c.tb_runtimeMinutes) >= 120)
    return df


if __name__ == "__main__":
    task3(path="resources/title.basics.tsv").show()
