import sys

from py4j.java_gateway import java_import
from pyspark import SparkConf
from pyspark.shell import spark, sc
from pyspark.sql import SparkSession, DataFrame

BASE_PATH = "resources/out/"
TEMP_PATH = "resources/temp/"

session = (SparkSession.builder
           .config(conf=SparkConf())
           .master("local")
           .appName("imdb")
           .getOrCreate()
           )


def load(path, schema=None, sep="\t", header=True, limit=None):
    eprint(f"Params: schema={schema}, sep={sep}, header={header}, limit={limit}")
    if limit is None:
        return session.read.csv(path, sep=sep, header=header)  # schema=schema,
    else:
        return session.read.csv(path, sep=sep, header=header).limit(limit)  # schema=schema,


def save(df: DataFrame, file_name: str, sep="\t", header=True):
    df.write.csv(TEMP_PATH + file_name, mode="overwrite", sep=sep, header=header, encoding="UTF-8")

    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = TEMP_PATH + file_name + "/"
    file = fs.globStatus(sc._jvm.Path(path + "part*"))[0].getPath().getName()
    print(file)
    fs.rename(sc._jvm.Path(path + file), sc._jvm.Path(BASE_PATH + file_name + ".tsv"))
    fs.delete(sc._jvm.Path(TEMP_PATH + file_name), True)


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

