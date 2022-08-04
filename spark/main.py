import pyspark
import argparse

def main(n):
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print(spark.range(n).count())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--arg1', type=str, required=True)
    args = parser.parse_args()
    main(args.arg1)
