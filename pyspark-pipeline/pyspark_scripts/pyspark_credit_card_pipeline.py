import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, from_json, sum as spark_sum, when, lit, split, udf, trim, regexp_replace
from pyspark.sql.functions import date_format, from_unixtime, to_timestamp, year, current_date
from pyspark.sql.functions import aes_encrypt, aes_decrypt, base64, unbase64
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os

def explode_json(df, col_name, schema):
    return df.withColumn(col_name, from_json(col(col_name), schema)).select("*", f'{col_name}.*').drop(col_name)

def main():
    conf = (
        pyspark.SparkConf().setAppName('cc_credit_card_pipeline')
        .set("spark.executor.memory", "16g") # Adjust as needed
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # --- Stage 1: Load and Transform Data (from cc_credit_card.ipynb) ---
    raw_df = spark.read.json('../../raw_cc_credit_card/raw_cc_credit_card') # Path adjusted for script location

    transformed_df = raw_df.select('*')

    # Date transformation
    transformed_df = transformed_df.withColumn(
        "trans_date_trans_time",
        date_format(to_timestamp(col('trans_date_trans_time')), "yyyy-MM-dd HH:mm:ss.SSS")
    )
    transformed_df = transformed_df.withColumn(
        "merch_last_update_time",
        date_format(from_unixtime(col("merch_last_update_time") / 1000000), "yyyy-MM-dd HH:mm:ss.SSS")
    )
    transformed_df = transformed_df.withColumn(
        "merch_eff_time",
        date_format(from_unixtime(col("merch_eff_time") / 1000000), "yyyy-MM-dd HH:mm:ss.SSS")
    )

    # Define schemas for JSON explosion
    personal_detail_schema = StructType([
        StructField("person_name", StringType()),
        StructField("gender", StringType()),
        StructField("address", StringType()),
        StructField("lat", StringType()),
        StructField("long", StringType()),
        StructField("city_pop", StringType()),
        StructField("job", StringType()),
        StructField("dob", StringType()),
    ])

    address_schema = StructType([
        StructField("street", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("zip", StringType())
    ])

    # Explode JSON columns
    transformed_df = explode_json(transformed_df, 'personal_detail', personal_detail_schema)
    transformed_df = explode_json(transformed_df, 'address', address_schema)

    # Split person_name
    transformed_df = transformed_df.withColumn('first', split(transformed_df['person_name'], '\\W+').getItem(0))
    transformed_df = transformed_df.withColumn('last', split(transformed_df['person_name'], '\\W+').getItem(1))
    transformed_df = transformed_df.drop('person_name')

    # Trim columns
    trim_columns_list = ['merchant', 'job', 'street', 'city', 'state']
    for column_name in trim_columns_list:
        transformed_df = transformed_df.withColumn(column_name, trim(col(column_name)))

    # Handle nulls in cc_bic and merch_zipcode
    transformed_df = transformed_df.withColumn("cc_bic", when(col("cc_bic").isin("Null", "NA", ""), lit(None)).otherwise(col("cc_bic")))
    transformed_df = transformed_df.fillna('undefined', subset=['cc_bic', 'merch_zipcode'])


    # Cast data types
    transformed_df = transformed_df.withColumn("amt", col("amt").cast(DoubleType())) \
                                .withColumn("lat", col("lat").cast(DoubleType())) \
                                .withColumn("long", col("long").cast(DoubleType())) \
                                .withColumn("city_pop", col("city_pop").cast(IntegerType())) \
                                .withColumn("merch_lat", col("merch_lat").cast(DoubleType())) \
                                .withColumn("merch_long", col("merch_long").cast(DoubleType())) \
                                .withColumn("is_fraud", col("is_fraud").cast(IntegerType())) \
                                .withColumn("zip", col("zip").cast(StringType())) \
                                .withColumn("merch_zipcode", col("merch_zipcode").cast(StringType()))

    # Select final columns for curated data (before encryption)
    curated_df = transformed_df.select(
        col("Unnamed: 0"),
        col("trans_date_trans_time"),
        col("cc_num"),
        col("merchant"),
        col("category"),
        col("amt"),
        col("first"),
        col("last"),
        col("gender"),
        col("street"),
        col("city"),
        col("state"),
        col("zip"),
        col("lat"),
        col("long"),
        col("city_pop"),
        col("job"),
        col("dob"),
        col("trans_num"),
        col("merch_lat"),
        col("merch_long"),
        col("is_fraud"),
        col("merch_zipcode"),
        col("merch_last_update_time"),
        col("merch_eff_time"),
        col("cc_bic")
    )

    # --- Stage 2: PII Encryption (from cc_credit_card_data_serving.ipynb) ---
    encrypted_data_df = curated_df.withColumn("age", year(current_date()) - year(col("dob")))

    # Encryption key handling
    key_path = '../decryption.key' # Path adjusted
    if os.path.exists(key_path):
        with open(key_path, "rb") as f:
            key = f.read()
    else:
        key = os.urandom(32) # AES-256 key
        with open(key_path, "wb") as f:
            f.write(key)

    list_of_columns_to_encrypt = [
        "cc_num",
        "first",
        "last",
        "street",
        "dob"
    ]

    for column_to_encrypt in list_of_columns_to_encrypt:
        encrypted_data_df = encrypted_data_df.withColumn(
            column_to_encrypt,
            base64(aes_encrypt(col(column_to_encrypt).cast("binary"), lit(key)))
        )

    # --- Stage 3: Write Final Output ---
    # Output path relative to the project root for Airflow execution
    output_path = '../../transformed_cc_credit_card/cc_credit_card_transformed_airflow.parquet'

    encrypted_data_df.write.partitionBy("cc_bic", "category", "gender").mode('overwrite').parquet(output_path)

    print(f"Successfully processed data and saved to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
