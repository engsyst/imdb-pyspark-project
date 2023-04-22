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

title_basics_schema = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=True),
    t.StructField("titleType", t.StringType(), nullable=True),
    t.StructField("primaryTitle", t.StringType(), nullable=True),
    t.StructField("originalTitle", t.StringType(), nullable=True),
    t.StructField("isAdult", t.StringType(), nullable=True),
    t.StructField("startYear", t.IntegerType(), nullable=True),
    t.StructField("endYear", t.IntegerType(), nullable=True),
    t.StructField("runtimeMinutes", t.IntegerType(), nullable=True),
    t.StructField("genres", t.StringType(), nullable=True),
])

title_principals_schema = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=True),
    t.StructField("titleType", t.StringType(), nullable=True),
    t.StructField("primaryTitle", t.StringType(), nullable=True),
    t.StructField("originalTitle", t.StringType(), nullable=True),
    t.StructField("isAdult", t.StringType(), nullable=True),
    t.StructField("startYear", t.IntegerType(), nullable=True),
    t.StructField("endYear", t.IntegerType(), nullable=True),
    t.StructField("runtimeMinutes", t.IntegerType(), nullable=True),
    t.StructField("genres", t.StringType(), nullable=True),
])

title_episode_schema = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=True),
    t.StructField("parentTconst", t.StringType(), nullable=True),
    t.StructField("seasonNumber", t.IntegerType(), nullable=True),
    t.StructField("episodeNumber", t.IntegerType(), nullable=True),
])
