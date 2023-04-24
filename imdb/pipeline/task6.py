# Get information about how many episodes in each TV Series. Get the top
# 50 of them starting from the TV Series with the biggest quantity of
# episodes.
import pyspark.sql.functions as f

import imdb.pipeline.columns as c
from imdb.ioutil import save
from imdb.pipeline.functions import load_title_basics, \
    load_title_episode


def task6(title_basics_path="resources/title.basics.tsv.gz",
          title_episode_path="resources/title.episode.tsv.gz",
          limit=None):
    basics_df = load_title_basics(title_basics_path, limit)
    episode_df = load_title_episode(title_episode_path, limit)
    grouped_episode_df = episode_df.groupBy(c.te_parentTconst).count().orderBy(f.desc("count"))
    # to reduce time filter before join
    df = (grouped_episode_df
          .join(basics_df.filter(f.col(c.tb_titleType) == "tvSeries"),
                f.col(c.te_parentTconst) == f.col(c.tb_tconst))
          .orderBy(f.desc("count"), c.tb_originalTitle)
          )
    df.show()
    save(df, "task6")
    return df


if __name__ == "__main__":
    task6()  # .show()
