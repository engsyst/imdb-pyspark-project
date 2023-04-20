from pyspark import SparkConf
from pyspark.sql import SparkSession


def load(path, schema=None, sep="\t", header=True, limit=None):
    session = (SparkSession.builder
               .config(conf=SparkConf())
               .master("local")
               .appName("imdb")
               .getOrCreate()
               )
    if limit is not None:
        return session.read.csv(path, schema=schema, sep=sep, header=header, nullValue="\\N").limit(limit)
    else:
        return session.read.csv(path, schema=schema, sep=sep, header=header)
