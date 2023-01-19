import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, airlines_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param result_path: путь с результатами преобразований
    """
    #TODO Ваш код
    df_flights = spark.read.parquet(flights_path) \
        .withColumn('airline_issue',
                    F.when(F.col('CANCELLATION_REASON') == 'A', 1).otherwise(0)) \
        .withColumn('weather_issue',
                    F.when(F.col('CANCELLATION_REASON') == 'B', 1).otherwise(0)) \
        .withColumn('nas_issue',
                    F.when(F.col('CANCELLATION_REASON') == 'C', 1).otherwise(0)) \
        .withColumn('security_issue',
                    F.when(F.col('CANCELLATION_REASON') == 'D', 1).otherwise(0))
    df_airlines = spark.read.parquet(airlines_path) \
        .withColumnRenamed('AIRLINE', 'AIRLINE_NAME')

    df = df_flights \
        .join(df_airlines, on=df_airlines['IATA_CODE'] == df_flights['AIRLINE']) \
        .groupBy(F.col('AIRLINE_NAME')) \
        .agg(F.count('TAIL_NUMBER').alias('correct_count'),
             F.sum('DIVERTED').alias('diverted_count'),
             F.sum('CANCELLED').alias('cancelled_count'),
             F.round(F.avg('DISTANCE'), 6).alias('avg_distance'),
             F.round(F.avg('AIR_TIME'), 6).alias('avg_air_time'),
             F.sum('airline_issue').alias('airline_issue_count'),
             F.sum('weather_issue').alias('weather_issue_count'),
             F.sum('nas_issue').alias('nas_issue_count'),
             F.sum('security_issue').alias('security_issue_count')) \
        .select(F.col('AIRLINE_NAME'),
                F.col('correct_count'),
                F.col('diverted_count'),
                F.col('cancelled_count'),
                F.col('avg_distance'),
                F.col('avg_air_time'),
                F.col('airline_issue_count'),
                F.col('weather_issue_count'),
                F.col('nas_issue_count'),
                F.col('security_issue_count')) \
        .orderBy(F.col('AIRLINE_NAME'))

    return df.write.parquet(result_path)


def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob5').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet',
                        help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet',
                        help='Please set airlines datasets path.')
    parser.add_argument('--result_path', type=str, default='result',
                        help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)
