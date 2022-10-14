import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder.appName("ADS project 2")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

if __name__ == "__main__":
    readF = sys.argv[1]
    writeF = sys.argv[2]

    if readF.endswith("csv"):
        readF = spark.read.csv(readF, sep="|", header=True)
        readF.write.parquet(writeF)
    else:
        f = spark.read.parquet(readF)
        f.write.parquet(writeF)
