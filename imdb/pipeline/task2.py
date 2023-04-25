# Get the list of people’s names, who were born in the 19th century.
import pyspark.sql.functions as f

import imdb.pipeline.columns as c
from imdb.ioutil import save
from imdb.pipeline.functions import load_name_basics


def task2(path="resources/name.basics.tsv.gz", limit=None):
    df = load_name_basics(path, limit)

    # Get result
    df = df.filter((f.col(c.nb_birthYear) >= 1800) & (f.col(c.nb_birthYear) < 1900))
    save(df, "task2")
    return df


if __name__ == "__main__":
    task2(limit=10000).show()
