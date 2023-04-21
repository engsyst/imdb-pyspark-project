# Get the list of people’s names, who were born in the 19th century.
import pyspark.sql.functions as f
import pyspark.sql.types as t

import imdb.pipeline.columns as c
from imdb.ioutil import load
from imdb.pipeline.functions import apply_with_columns_func, test_data_complaince


def task2(path="resources/name.basics.tsv.gz", limit=None):
    df = load(path, limit=limit)  # , schema=names_schema

    # Check bad data
    test_data_complaince(df, c.nb_birthYear, t.IntegerType(), "\\N", show_details=True)

    # Apply correct schema
    df = apply_with_columns_func(df, df.columns, lambda cl: f.when(f.col(cl) == "\\N", None).otherwise(f.col(cl)))

    # Get result
    df = df.filter((f.col(c.nb_birthYear) >= 1800) & (f.col(c.nb_birthYear) < 1900))
    return df


if __name__ == "__main__":
    task2(limit=None).show()
