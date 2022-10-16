import sys
import os
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

    transactions = [sys.argv[1] + "/" + file for file in os.listdir(sys.argv[1]) if "transactions" in file]
    consumer_fraud = [sys.argv[1] + "/" + file for file in os.listdir(sys.argv[1]) if "consumer" in file and "fraud" in file]
    consumer_detail = [sys.argv[1] + "/" + file for file in os.listdir(sys.argv[1]) if "consumer" in file and "detail" in file]
    tbl_consumer = [sys.argv[1] + "/" + file for file in os.listdir(sys.argv[1]) if "consumer" in file and "tbl" in file]
    tbl_merchant = [sys.argv[1] + "/" + file for file in os.listdir(sys.argv[1]) if "merchant" in file and "tbl" in file]


    # preprocess "consumer_fraud"
    ls_consumer_fraud = []
    for i in consumer_fraud:
        if i.endswith("csv"):
            f = spark.read.csv(i, sep=",", header=True)
        else:
            f = spark.read.parquet(i)
        ls_consumer_fraud.append(f)

    total_consumer_fraud = ls_consumer_fraud[0]
    if len(ls_consumer_fraud) > 1:
        for j in range(len(ls_consumer_fraud)-1):
            total_consumer_fraud = total_consumer_fraud.union(ls_consumer_fraud[j+1])

    total_consumer_fraud.write.parquet(sys.argv[2]+"/consumer_fraud")

    # preprocess "tbl_merchant"
    ls_merchant = []
    for i in tbl_merchant:
        if i.endswith("csv"):
            f = spark.read.csv(i, sep="|", header=True)
        else:
            f = spark.read.parquet(i)
        
        # separate "tags" into "products", "revenue_level", "take_rate"
        preprocessed_f = f.withColumn("split_tags", split(col("tags"), ",")) \
            .withColumn("take_rate", element_at(col("split_tags"), -1)) \
            .withColumn("revenue_level", element_at(col("split_tags"), -2)) \
            .withColumn("products", slice(reverse(col("split_tags")), 3, 100)) \
            .withColumn("products", reverse(col("products")))

        # remove the special characters of "revenue_level" and "take_rate"
        preprocessed_f = preprocessed_f.withColumn("revenue_level", regexp_replace("revenue_level", "[^a-e]", "")) \
                                                            .withColumn("take_rate", regexp_replace("take_rate", "[^0-9.]", ""))

        # standardize the values in "products" and vectorize "products" into the bags of word
        preprocessed_f = preprocessed_f.withColumn("products", concat_ws(",", col("products"))) \
                                                            .withColumn("products", regexp_replace("products", "[^A-Za-z0-9]", " ")) \
                                                            .withColumn("products", regexp_replace("products", "\s+", " ")) \
                                                            .withColumn("products", regexp_replace("products", "(^\s+)|(\s+$)", "")) \
                                                            .withColumn("products", lower(col("products")))

        preprocessed_f = preprocessed_f.withColumnRenamed("name", "merchant_name")
        preprocessed_f = preprocessed_f.select("merchant_name", "products", "revenue_level",
                                                                    preprocessed_f.take_rate.cast("double"), "merchant_abn")

        tags = preprocessed_f.rdd.map(lambda x: x[1]).collect()
        tags_unique = " ".join(tags).split()
        tags_unique = list(set(tags_unique))

        tags_tok = tags
        for i in range(len(tags)):
            tags_tok[i] = tags_tok[i].split()
            cur = [tags_tok[i][0]]
            for j in range(1, len(tags_tok[i])):
                if tags_tok[i][j-1] != "except":
                    if tags_tok[i][j] not in ["and", "except", "other", "shops", "services"]:
                        cur += [tags_tok[i][j]]
            tags_tok[i] = cur

        tags_tok_unique = []
        for i in tags_tok:
            if i not in tags_tok_unique:
                tags_tok_unique += [i]

        import collections
        categories = [
            'home and technology', 'home and technology', 'fashion and accessories', 'fashion and accessories', 'books and music',
            'art and gifts', 'home and technology', 'home and technology', 'home and technology', 'art and gifts',
            'outdoors', 'art and gifts', 'outdoors', 'books and music', 'books and music',
            'outdoors', 'art and gifts', 'outdoors', 'books and music', 'fashion and accessories',
            'fashion and accessories', 'fashion and accessories', 'books and music', 'outdoors', 'home and technology'
        ]

        tag_to_cat = {" ".join(tags_tok_unique[i]): categories[i] for i in range(len(categories))}
        cat_list = [tag_to_cat[" ".join(i)] for i in tags_tok]

        collections.Counter([i for i in cat_list]).most_common()

        from pyspark.sql.types import *
        @udf(ArrayType(StringType()))
        def tokenise(tag):
            tag = tag.split()
            cur = [tag[0]]
            for i in range(1, len(tag)):
                if tag[i-1] != "except":
                    if tag[i] not in ["and", "except", "other", "shops", "services"]:
                        cur += [tag[i]]
            return cur

        @udf(StringType())
        def categorise(tag):
            tag = " ".join(tag)
            return tag_to_cat[tag]

        preprocessed_tbl_merchant_token = preprocessed_f.withColumn("tag", tokenise(col("products")))
        preprocessed_tbl_merchant_cat = preprocessed_tbl_merchant_token.withColumn("category", categorise(col("tag")))
        preprocessed_tbl_merchant_cat = preprocessed_tbl_merchant_cat.select(*(preprocessed_tbl_merchant_cat.columns[:5]),
                                                                        concat_ws(' ', 'tag').alias('tag'), "category")

        ls_merchant.append(preprocessed_tbl_merchant_cat)

    total_merchant = ls_merchant[0]
    if len(ls_merchant) > 1:
        for j in range(len(ls_merchant)-1):
            total_merchant = total_merchant.union(ls_merchant[j+1])
    


    # preprocess "transactions"
    ls_transactions = []
    for i in transactions:
        if i.endswith("csv"):
            f = spark.read.csv(i, sep="|", header=True)
        else:
            f = spark.read.parquet(i)

        # separate pickup datetime into date, year, month and day
        preprocessed_transact = f.withColumn('order_year', year(col('order_datetime')))
        preprocessed_transact = preprocessed_transact.withColumn('order_month', month(col('order_datetime')))
        preprocessed_transact = preprocessed_transact.withColumn('order_day', dayofmonth(col('order_datetime')))
        preprocessed_transact = preprocessed_transact.withColumnRenamed('merchant_abn', 'merchant_abn_repeat')

        ls_transactions.append(preprocessed_transact)

    total_transactions = ls_transactions[0]
    if len(ls_transactions) > 1:
        for j in range(len(ls_transactions)-1):
            total_transactions = total_transactions.union(ls_transactions[j+1])



    # preprocess "tbl_consumer"
    ls_tbl_consumer = []
    for i in tbl_consumer:
        if i.endswith("csv"):
            f = spark.read.csv(i, sep="|", header=True)
        else:
            f = spark.read.parquet(i)

        preprocessed_tbl_consumer = f.withColumnRenamed("name", "consumer")
        preprocessed_tbl_consumer = preprocessed_tbl_consumer.withColumnRenamed("address", "consumer_address")
        preprocessed_tbl_consumer = preprocessed_tbl_consumer.withColumnRenamed("state", "consumer_state")
        preprocessed_tbl_consumer = preprocessed_tbl_consumer.withColumnRenamed("postcode", "consumer_postcode")
        preprocessed_tbl_consumer = preprocessed_tbl_consumer.withColumnRenamed("gender", "consumer_gender")

        ls_tbl_consumer.append(preprocessed_tbl_consumer)

    total_tbl_consumer = ls_tbl_consumer[0]
    if len(ls_tbl_consumer) > 1:
        for j in range(len(ls_tbl_consumer)-1):
            total_tbl_consumer = total_tbl_consumer.union(ls_tbl_consumer[j+1])



    # preprocess "tbl_consumer"
    ls_consumer_detail = []
    for i in consumer_detail:
        if i.endswith("csv"):
            f = spark.read.csv(i, sep="|", header=True)
        else:
            f = spark.read.parquet(i)

        preprocessed_consumer = f.withColumnRenamed("user_id", "user_id_repeat")
        preprocessed_consumer = preprocessed_consumer.withColumnRenamed("consumer_id", "consumer_id_repeat")

        ls_consumer_detail.append(preprocessed_consumer)

    total_consumer_detail = ls_consumer_detail[0]
    if len(ls_tbl_consumer) > 1:
        for j in range(len(ls_consumer_detail)-1):
            total_consumer_detail = total_consumer_detail.union(ls_consumer_detail[j+1])



    # join tables
    total_merchant.createOrReplaceTempView('merchant')
    total_transactions.createOrReplaceTempView('transact')
    total_tbl_consumer.createOrReplaceTempView('tbl_consumer')
    total_consumer_detail.createOrReplaceTempView('consumer')

    join = spark.sql("""
    SELECT 
        *
    FROM 
        merchant
    INNER JOIN
        transact
    ON 
        transact.merchant_abn_repeat = merchant.merchant_abn
    INNER JOIN
        consumer
    ON
        transact.user_id = consumer.user_id_repeat
    INNER JOIN
        tbl_consumer
    ON 
        consumer.consumer_id_repeat = tbl_consumer.consumer_id
    ORDER BY
        revenue_level DESC
    """)

    join = join.drop("merchant_abn_repeat", "consumer_id_repeat", "user_id_repeat", "merchant_abn", "consumer_id", "order_id")

    join.write.parquet(sys.argv[2]+"/internal_dataset")