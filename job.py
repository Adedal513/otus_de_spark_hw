from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg


CRIMES_PATH = 'data/crime.csv'
OFFENSE_CODES_PATH = 'data/offense_codes.csv'
PARQUET_OUTPUT_PATH = 'output/'


spark = SparkSession.builder \
    .appName('crime_datamart') \
    .getOrCreate()


crimes_df = spark.read.csv(
    CRIMES_PATH,
    header=True,
    inferSchema=True
)

# Проверка наличия дубликатов и очистка данных
crimes_df = crimes_df.dropDuplicates()

# Загрузка справочника кодов и удаление дубликатов
codes_data = spark.read.csv(
    OFFENSE_CODES_PATH,
    header=True,
    inferSchema=True
)
codes_data = codes_data.dropDuplicates(["CODE"])

# Присоединение справочника к основным данным
crime_data = crimes_df.join(
    codes_data,
    crimes_df.OFFENSE_CODE == codes_data.CODE,
    "left_outer"
)

# Создание витрины
crime_data.createOrReplaceTempView("crime_data_view")


crimes_total = spark.sql("""
    SELECT
        district,
        COUNT(*) AS crimes_total
    FROM crime_data_view
    GROUP BY district
""")

crimes_monthly = spark.sql("""
    SELECT
        district,
        percentile_approx(count, 0.5) AS crimes_monthly
    FROM (
        SELECT
            district,
            COUNT(INCIDENT_NUMBER) AS count,
            YEAR AS year,
            MONTH AS month
        FROM crime_data_view
        GROUP BY district, YEAR, MONTH
    ) AS monthly_counts
    GROUP BY district
""")

frequent_crime_types = spark.sql("""
    SELECT
        district,
        CONCAT_WS(", ", frequent_crime_types[0], frequent_crime_types[1], frequent_crime_types[2]) AS frequent_crime_types
    FROM (
        SELECT
            district,
            collect_list(crime_type) AS frequent_crime_types
        FROM (
            SELECT
                district,
                NAME as crime_type,
                COUNT(*) AS count
            FROM crime_data_view
            GROUP BY district, NAME
            ORDER BY count DESC
        )
        GROUP BY district
    )
""")

average_coordinates = spark.sql("""
    SELECT
        district,
        AVG(Lat) AS lat,
        AVG(Long) AS lng
    FROM crime_data_view
    GROUP BY district
""")

result = crimes_total.join(crimes_monthly, "district") \
                     .join(frequent_crime_types, "district") \
                     .join(average_coordinates, "district")


result.write.parquet(PARQUET_OUTPUT_PATH, mode="overwrite")

spark.stop()