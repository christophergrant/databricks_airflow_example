import pyspark
import argparse
from delta import configure_spark_with_delta_pip


def main(arg1, arg2):
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    def create_path(n):
        return f"/tmp/cgrant/{n}"
    df1, df2 = spark.read.format("delta").load(create_path(arg1)), spark.read.format("delta").load(create_path(arg2))
    final = df1.join(df2, "id").select("id")
    final.write.format("delta").mode("overwrite").save(f"/tmp/cgrant/agg")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--arg1', type=str, required=True)
    parser.add_argument('--arg2', type=str, required=True)
    args = parser.parse_args()
    main(args.arg1, args.arg2)
