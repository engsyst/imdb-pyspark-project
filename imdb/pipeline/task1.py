# Get all titles of series/movies etc. that are available in Ukrainian.
import pyspark.sql.functions as f

import imdb.pipeline.columns as c
from imdb.ioutil import save
from imdb.pipeline.functions import load_title_akas


def task1(path="resources/title.akas.tsv.gz", limit=None):
    df = load_title_akas(path, limit=limit)

    # Get result
    df = df.filter(f.col(c.ta_region) == "UA")
    save(df, "task1")
    return df


if __name__ == "__main__":
    task1().show()
