# Get all titles of series/movies etc. that are available in Ukrainian.
import pyspark.sql.functions as f

import imdb.pipeline.columns as c
from imdb.ioutil import load
from imdb.pipeline.functions import clean_title_akas
from imdb.pipeline.schemas import title_akas_schema


def task1(path="resources/title.akas.tsv.gz", limit=None):
    df = load(path, schema=title_akas_schema, limit=limit)  #
    df = clean_title_akas(df)
    # Get result
    df = df.filter(f.col(c.ta_region) == "UA")
    return df


if __name__ == "__main__":
    task1(limit=None).show()
