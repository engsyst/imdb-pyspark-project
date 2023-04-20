import pyspark.sql.types as t

title_akas_schema = t.StructType([
    t.StructField("titleId", t.StringType(), nullable=False),
    t.StructField("ordering", t.StringType(), nullable=False),
    t.StructField("title", t.StringType(), nullable=False),
    t.StructField("region", t.StringType(), nullable=True),
    t.StructField("language", t.StringType(), nullable=True),
    t.StructField("types", t.StringType(), nullable=True),
    t.StructField("attributes", t.StringType(), nullable=True),
    # Column `attributes` has a data type of array<string>, which is not supported by CSV.
    t.StructField("isOriginalTitle", t.IntegerType(), nullable=True),
])

names_schema = t.StructType([
    t.StructField("nconst", t.StringType(), nullable=False),
    t.StructField("primaryName", t.StringType(), nullable=False),
    t.StructField("birthYear", t.IntegerType(), nullable=True),
    t.StructField("deathYear", t.IntegerType(), nullable=True),
    t.StructField("primaryProfession", t.StringType(), nullable=True),
    t.StructField("knownForTitles", t.StringType(), nullable=True),
])
