import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """
    #TODO Ваш код
    data_flights = spark.read.parquet(flights_path)
    df = data_flights.groupBy(data_flights['ORIGIN_AIRPORT']) \
        .agg(F.round(F.avg(data_flights['DEPARTURE_DELAY']), 6).alias('avg_delay'),
             F.min(data_flights['DEPARTURE_DELAY']).alias('min_delay'),
             F.max(data_flights['DEPARTURE_DELAY']).alias('max_delay'),
             F.round(F.corr('DEPARTURE_DELAY', 'DAY_OF_WEEK'), 6).alias("corr_delay2day_of_week")) \
        .filter(F.col('max_delay') > 1000) \
        .select(F.col('ORIGIN_AIRPORT'),
                F.col('avg_delay'),
                F.col('min_delay'),
                F.col('max_delay'),
                F.col('corr_delay2day_of_week')) \
        .orderBy(F.col('max_delay').desc())

    return df.write.parquet(result_path)


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob3').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)
