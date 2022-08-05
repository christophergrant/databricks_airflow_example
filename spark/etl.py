import pyspark
import argparse
from delta import configure_spark_with_delta_pip


def main(n):
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.range(n).write.format("delta").mode("overwrite").save(f"/tmp/cgrant/{n}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--arg1', type=str, required=True)
    args = parser.parse_args()
    main(args.arg1)
