# Get all titles of series/movies etc. that are available in Ukrainian.
from app.imdb.ioutil import load
import app.imdb.pipeline.columns as c
import pyspark.sql.functions as f
import pyspark.sql.types as t

from app.imdb.pipeline.functions import apply_with_columns_func
from app.imdb.pipeline.schemas import names_schema


def task2():
    path = "../../../resources/name.basics.tsv"
    # path = "resources/name.basics.tsv.gz"
    df = load(path, schema=names_schema)
    df.printSchema()
    print(f"Total rows: {df.count()}")

    # Check bad data
    test_df = (df.filter((f.col(c.nb_birthYear) != "\\N") &
                         f.col(c.nb_birthYear).cast(t.IntegerType()).isNull()))
    test_df.show()
    print(f"Wrong rows count: {test_df.count()}")

    df = apply_with_columns_func(df, df.columns, lambda cl: f.when(f.col(cl) == "\\N", None).otherwise(f.col(cl)))
    df.show()
    # nulls_df = df.select(f.count(f.when(f.col(c.nb_nconst).isNull(), 1)),
    #                      f.count(f.when(f.col(c.nb_primaryName).isNull(), 1)),
    #                      f.count(f.when(f.col(c.nb_birthYear).isNull(), 1)),
    #                      f.count(f.when(f.col(c.nb_deathYear).isNull(), 1)),
    #                      f.count(f.when(f.col(c.nb_primaryProfession).isNull(), 1)),
    #                      f.count(f.when(f.col(c.nb_knownForTitles).isNull(), 1))
    #                      )
    # nulls_df.show()
    df.filter((f.col(c.nb_birthYear) >= 1800) & (f.col(c.nb_birthYear) < 1900)).show()


if __name__ == "__main__":
    task2()
