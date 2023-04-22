# Get titles of all movies that last more than 2 hours.
import pyspark.sql.functions as f

import imdb.pipeline.columns as c
from imdb.ioutil import load
from imdb.pipeline.functions import clean_title_basics


def task3(path="resources/title.basics.tsv.gz", limit=None):
    df = load(path, limit=limit)  # , schema=names_schema)
    df = clean_title_basics(df)

    # Get result
    df = df.filter(f.col(c.tb_runtimeMinutes) >= 120)
    return df


if __name__ == "__main__":
    task3(path="resources/title.basics.tsv").show()
