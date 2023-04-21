import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession


def load(path, schema=None, sep="\t", header=True, limit=None):
    eprint(f"Params: schema={schema}, sep={sep}, header={header}, limit={limit}")
    session = (SparkSession.builder
               .config(conf=SparkConf())
               .master("local")
               .appName("imdb")
               .getOrCreate()
               )
    if limit is None:
        return session.read.csv(path, sep=sep, header=header)  # schema=schema,
    else:
        return session.read.csv(path, sep=sep, header=header).limit(limit)  # schema=schema,


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
