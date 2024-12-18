import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, mean, to_date, locate, date_format, sum, col, expr, lpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

BIGQUERY_DATASET = 'pyspark_practice'
GCS_BUCKET = 'dataproc-data-centi'
SOURCE_PATH = 'gs://dataproc-data-centi/pyspark_practice/source'
execution_mode = 'local'

def get_session() -> SparkSession:
    global execution_mode

    ss = SparkSession.builder.appName("Simple PySpark Practice"). \
        getOrCreate()
    #        config("spark.jars",
    #           "https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.41.0.jar,https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"). \


    if execution_mode == 'local':
        ss.conf.set("google.cloud.auth.service.account.enable", "true")
        ss.conf.set("google.cloud.auth.service.account.json.keyfile", "/opt/tkns/centi-data-engineering-f007bbb116ab.json")

    ss.conf.set("temporaryGcsBucket", GCS_BUCKET)

    return ss


def clean_up(data: DataFrame):
    # drop columns
    cols_not_needed = ['ZIP CODE', 'LATITUDE', 'LONGITUDE', 'LOCATION',
                       'ON STREET NAME', 'CROSS STREET NAME', 'OFF STREET NAME',
                       'COLLISION_ID']

    df = data.drop(*cols_not_needed)
    df = df.filter(col('CRASH DATE').rlike('[0-9]{2}/[0-9]{2}/[0-9]{4}'))

    return df


def explore_data(mode: str):
    global execution_mode

    execution_mode = mode
    session = get_session()

    df = session.read.format('gcs'). \
        csv(path=f'{SOURCE_PATH}/Motor_Vehicle_Collisions_-_Crashes_HEAD.csv',
            header=True, inferSchema=True)

    df = clean_up(df)

    df.show(n=100)

    df.describe().show()

    df.select('CONTRIBUTING FACTOR VEHICLE 1').distinct().sort('CONTRIBUTING FACTOR VEHICLE 1'). \
        write.mode('overwrite').text(
            'outputs/contributing_factor_vehicle_1.txt'
        )
    df.select('CONTRIBUTING FACTOR VEHICLE 4').distinct().sort('CONTRIBUTINlG FACTOR VEHICLE 4'). \
        write.mode('overwrite').text(
            'outputs/contributing_factor_vehicle_4.txt'
        )
    df.select('VEHICLE TYPE CODE 1').distinct().sort('VEHICLE TYPE CODE 1'). \
        write.mode('overwrite').text(
            'outputs/vehicle_type_code_1.txt'
        )
    df.select('VEHICLE TYPE CODE 5').distinct().sort('VEHICLE TYPE CODE 5'). \
        write.mode('overwrite').text(
            'outputs/vehicle_type_code_5.txt'
        )

    df.printSchema()


def store_unused_agg(data: DataFrame):
    unused_columns = [
        'VEHICLE TYPE CODE 1', 'VEHICLE TYPE CODE 2', 'VEHICLE TYPE CODE 3',
        'VEHICLE TYPE CODE 4', 'VEHICLE TYPE CODE 5']

    df = data.groupBy(*unused_columns).agg(
        count('*').alias('count')
    )

    df.write.format('bigquery'). \
        option('table', f'{BIGQUERY_DATASET}.unused_data'). \
        mode('overwrite'). \
        save()

    return data.drop(*unused_columns)


def calculate_metrics(data: DataFrame):
    # crashes by hour of day
    crashes_per_hour = data.withColumn('position', locate(':', col('CRASH TIME'))). \
        withColumn('hour_of_day', expr('substring(`CRASH TIME`, 1, position - 1)')). \
        withColumn('hour_of_day', lpad(col('hour_of_day'), 2, '0'))

    crashes_per_hour = crashes_per_hour.groupBy('hour_of_day').agg(count('*').alias('count'))
    crashes_per_hour.write.format('bigquery'). \
        option('table', f'{BIGQUERY_DATASET}.crashes_per_hour'). \
        mode('overwrite'). \
        save()

    # crashes by day of week
    crashes_per_dayofweek = data.withColumn('day_of_week', date_format('CRASH DATE' ,'E'))
    crashes_per_dayofweek = crashes_per_dayofweek.groupBy('day_of_week').agg(count('*').alias('count'))
    crashes_per_dayofweek.write.format('bigquery'). \
        option('table', f'{BIGQUERY_DATASET}.crashes_per_dayofweek'). \
        mode('overwrite'). \
        save()

    # persons injured or killed by contributing factor
    metrics_by_contributing_factor = data.groupBy('CONTRIBUTING FACTOR VEHICLE 1', 'CONTRIBUTING FACTOR VEHICLE 2'). \
        agg(
            count('*').alias('count'),
            sum('NUMBER OF PERSONS INJURED').alias('persons_injured'),
            mean('NUMBER OF PERSONS INJURED').alias('persons_injured_avg'),
            sum('NUMBER OF PERSONS KILLED').alias('persons_killed'),
            mean('NUMBER OF PERSONS KILLED').alias('persons_killed_avg')
            )
    metrics_by_contributing_factor.write.format('bigquery'). \
        option('table', f'{BIGQUERY_DATASET}.metrics_by_contributing_factor'). \
        mode('overwrite'). \
        save()


def show_metrics(data: DataFrame):
    session = get_session()

    dim_date = session.read.format('bigquery'). \
        option('table', 'pyspark_practice.dim_date'). \
        load()

    dim_date.createOrReplaceTempView('dimDate')

    data.createOrReplaceTempView('facts')
    season_metrics = session.sql(
        '''SELECT 
            season,
            COUNT(*) AS crashes,
            SUM(`NUMBER OF PERSONS INJURED`) AS persons_injured,
            SUM(`NUMBER OF PERSONS KILLED`) AS persons_killed
        FROM facts f LEFT JOIN dimDate d
            ON f.`CRASH DATE` = d.full_date
        GROUP BY season
        '''
    )

    """
    enriched = data.join(other=dim_date, on=(data['CRASH DATE'] == dim_date['full_date']), how='outer').   \
        select('season', 'NUMBER OF PERSONS INJURED', 'NUMBER OF PERSONS KILLED')

    season_metrics = enriched.groupBy('season').agg(
        count('*').alias('crashes'),
        sum('NUMBER OF PERSONS INJURED').alias('persons_injured'),
        sum('NUMBER OF PERSONS KILLED').alias('persons_killed')
    )
    """

    season_metrics.show()


def pipeline(mode: str, source_file: str):
    global execution_mode

    print(f"Working in mode: {mode}")
    print(f"The source file is: {source_file}")

    execution_mode = mode
    session = get_session()

    csv_schema = StructType([
        StructField(name='CRASH DATE', dataType=StringType()),
        StructField(name='CRASH TIME', dataType=StringType()),
        StructField(name='BOROUGH', dataType=StringType()),
        StructField(name='ZIP CODE', dataType=StringType()),
        StructField(name='LATITUDE', dataType=FloatType()),
        StructField(name='LONGITUDE', dataType=FloatType()),
        StructField(name='LOCATION', dataType=StringType()),
        StructField(name='ON STREET NAME', dataType=StringType()),
        StructField(name='CROSS STREET NAME', dataType=StringType()),
        StructField(name='OFF STREET NAME', dataType=StringType()),
        StructField(name='NUMBER OF PERSONS INJURED', dataType=IntegerType()),
        StructField(name='NUMBER OF PERSONS KILLED', dataType=IntegerType()),
        StructField(name='NUMBER OF PEDESTRIANS INJURED', dataType=IntegerType()),
        StructField(name='NUMBER OF PEDESTRIANS KILLED', dataType=IntegerType()),
        StructField(name='NUMBER OF CYCLIST INJURED', dataType=IntegerType()),
        StructField(name='NUMBER OF CYCLIST KILLED', dataType=IntegerType()),
        StructField(name='NUMBER OF MOTORIST INJURED', dataType=IntegerType()),
        StructField(name='NUMBER OF MOTORIST KILLED', dataType=IntegerType()),
        StructField(name='CONTRIBUTING FACTOR VEHICLE 1', dataType=StringType()),
        StructField(name='CONTRIBUTING FACTOR VEHICLE 2', dataType=StringType()),
        StructField(name='CONTRIBUTING FACTOR VEHICLE 3', dataType=StringType()),
        StructField(name='CONTRIBUTING FACTOR VEHICLE 4', dataType=StringType()),
        StructField(name='CONTRIBUTING FACTOR VEHICLE 5', dataType=StringType()),
        StructField(name='COLLISION_ID', dataType=StringType()),
        StructField(name='VEHICLE TYPE CODE 1', dataType=StringType()),
        StructField(name='VEHICLE TYPE CODE 2', dataType=StringType()),
        StructField(name='VEHICLE TYPE CODE 3', dataType=StringType()),
        StructField(name='VEHICLE TYPE CODE 4', dataType=StringType()),
        StructField(name='VEHICLE TYPE CODE 5', dataType=StringType())
    ])
    df = session.read.format('gcs'). \
        csv(path=f'{SOURCE_PATH}/{source_file}',
            header=True, inferSchema=False,
            schema=csv_schema)

    # clean up
    df = clean_up(df)
    df = store_unused_agg(df)

    # format
    df = df.withColumn('CRASH DATE', to_date('CRASH DATE', format='MM/dd/yyyy'))

    # store cleaned data
    df.write.format('bigquery'). \
        option('table', f'{BIGQUERY_DATASET}.vehicle_collisions'). \
        mode('overwrite'). \
        save()

    # get metrics
    calculate_metrics(df)
    show_metrics(df)

    df.show()
    df.printSchema()


if __name__ == '__main__':
    #explore_data()
    pipeline(sys.argv[1], sys.argv[2])
