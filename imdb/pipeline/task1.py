# Get all titles of series/movies etc. that are available in Ukrainian.
import pyspark.sql.functions as f
import pyspark.sql.types as t

import imdb.pipeline.columns as c
from imdb.ioutil import load
from imdb.pipeline.functions import apply_with_columns_func, test_data_complaince


def task1(path="resources/title.akas.tsv.gz", limit=None):
    df = load(path, limit)  # , schema=title_akas_schema

    # Check bad data
    test_data_complaince(df, c.ta_region, t.StringType(), "\\N", show_details=True)

    # Apply correct schema
    df = apply_with_columns_func(df, df.columns, lambda cl: f.when(f.col(cl) == "\\N", None).otherwise(f.col(cl)))

    # Get result
    df = df.filter(f.col(c.ta_region) == "UA")
    return df


if __name__ == "__main__":
    task1(limit=None).show()
