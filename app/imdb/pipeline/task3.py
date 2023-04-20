# Get all titles of series/movies etc. that are available in Ukrainian.
import logging

import pyspark.sql.functions as f
import pyspark.sql.types as t

import app.imdb.pipeline.columns as c
from app.imdb.ioutil import load
from app.imdb.pipeline.functions import apply_with_columns_func

log = logging.getLogger("app.imdb.pipeline.task3")


def task3():
    path = "../../../resources/title.basics.tsv"
    # path = "resources/title.basics.tsv.gz"
    df = load(path)  # , schema=names_schema)
    # + no schema ---------------------------------+-----------------------------------------------+--------------------------------------------------+---------------------------------------------------+---------------------------------------------+-----------------------------------------------+---------------------------------------------+----------------------------------------------------+--------------------------------------------+
    # |count(CASE WHEN (tconst IS NULL) THEN 1 END)|count(CASE WHEN (titleType IS NULL) THEN 1 END)|count(CASE WHEN (primaryTitle IS NULL) THEN 1 END)|count(CASE WHEN (originalTitle IS NULL) THEN 1 END)|count(CASE WHEN (isAdult IS NULL) THEN 1 END)|count(CASE WHEN (startYear IS NULL) THEN 1 END)|count(CASE WHEN (endYear IS NULL) THEN 1 END)|count(CASE WHEN (runtimeMinutes IS NULL) THEN 1 END)|count(CASE WHEN (genres IS NULL) THEN 1 END)|
    # +--------------------------------------------+-----------------------------------------------+--------------------------------------------------+---------------------------------------------------+---------------------------------------------+-----------------------------------------------+---------------------------------------------+----------------------------------------------------+--------------------------------------------+
    # |                                           0|                                              0|                                                 0|                                                  0|                                            1|                                        1324454|                                      9683058|                                             6905863|                                      441446|
    # +--------------------------------------------+-----------------------------------------------+--------------------------------------------------+---------------------------------------------------+---------------------------------------------+-----------------------------------------------+---------------------------------------------+----------------------------------------------------+--------------------------------------------+
    # log.info(f"title.basics schema: {df.count()}")
    # df.printSchema()

    # Check bad data
    test_df = (df.filter((f.col(c.tb_runtimeMinutes) != "\\N") &
                         f.col(c.tb_runtimeMinutes).cast(t.IntegerType()).isNull()))
    if not test_df.isEmpty():
        log.warning(f"Found wrong rows: {test_df.count()}")
        test_df.show()

    # Apply correct schema
    df = apply_with_columns_func(df, df.columns, lambda cl: f.when(f.col(cl) == "\\N", None).otherwise(f.col(cl)))
    df = apply_with_columns_func(df, [c.tb_runtimeMinutes], lambda cl: f.col(cl).cast(t.IntegerType()))

    # Get result
    df = df.filter(f.col(c.tb_runtimeMinutes) >= 120)
    return df


if __name__ == "__main__":
    task3().show()
