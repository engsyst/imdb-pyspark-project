# Get all titles of series/movies etc. that are available in Ukrainian.
from app.imdb.ioutil import load
import app.imdb.pipeline.columns as c
import pyspark.sql.functions as f
import pyspark.sql.types as t

from app.imdb.pipeline.functions import apply_func
from app.imdb.pipeline.schemas import title_akas_schema


def task1():
    path = "../../../resources/title.akas.tsv.gz"
    # path = "resources/title.akas.tsv.gz"
    df = load(path, schema=title_akas_schema)
    df.printSchema()
    df = apply_func(df, df.columns, lambda cl: f.when(f.col(cl) == "\\N", None).otherwise(f.col(cl)))
    df.show()
    # nulls_df = df.select(f.count(f.when(f.col(c.ta_title).isNull(), 1)),
    #                      f.count(f.when(f.col(c.ta_region).isNull(), 1)),
    #                      f.count(f.when(f.col(c.ta_language).isNull(), 1)),
    #                      f.count(f.when(f.col(c.ta_types).isNull(), 1)),
    #                      f.count(f.when(f.col(c.ta_attributes).isNull(), 1)),
    #                      f.count(f.when(f.col(c.ta_isOriginalTitle).isNull(), 1))
    #                      )
    # nulls_df.show()
    df.filter(f.col(c.ta_region) == "UA").show()


if __name__ == "__main__":
    task1()
