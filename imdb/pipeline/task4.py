# Get names of people, corresponding movies/series and characters they played in those films.
import pyspark.sql.functions as f

import imdb.pipeline.columns as c
from imdb.ioutil import save
from imdb.pipeline.functions import load_name_basics, load_title_basics, load_title_principals


def task4(names_path="resources/name.basics.tsv.gz",
          title_principals_path="resources/title.principals.tsv.gz",
          title_basics_path="resources/title.basics.tsv.gz",
          limit=None):
    names_df = load_name_basics(names_path, limit)
    principals_df = load_title_principals(title_principals_path, limit)
    titles_df = load_title_basics(title_basics_path, limit)

    # Get result
    df = (titles_df.join(principals_df, c.tb_tconst)
          .join(names_df, c.tp_nconst)
          .orderBy(f.desc(c.tb_tconst))
          )
    save(df, "task4")
    return df


if __name__ == "__main__":
    task4(limit=10000)  # .show()
