from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, lag, row_number, round


spark = SparkSession.builder.appName("netology").master('local[2]').getOrCreate()

data = (
    spark.read
    .option('header', True)
    .option('sep', ',')
    .option('inferSchema', True)
    .csv('data/owid-covid-data.csv')
)

# 1) 15 стран с наибольшим процентом переболевших на 31 марта 2021
(
    data
    .filter((col('date') == '2021-03-31'))
    .select(
        'iso_code', 'location', round((col('total_cases') / col('population')) * 100, 2).alias('cases_per_population'))
    .sort(col('cases_per_population').desc())
    .show(15)
)
# +--------+-------------+--------------------+
# |iso_code|     location|cases_per_population|
# +--------+-------------+--------------------+
# |     AND|      Andorra|               15.54|
# |     MNE|   Montenegro|               14.52|
# |     CZE|      Czechia|               14.31|
# |     SMR|   San Marino|               13.94|
# |     SVN|     Slovenia|               10.37|
# |     LUX|   Luxembourg|                9.85|
# |     ISR|       Israel|                9.63|
# |     USA|United States|                 9.2|
# |     SRB|       Serbia|                8.83|
# |     BHR|      Bahrain|                8.49|
# |     PAN|       Panama|                8.23|
# |     PRT|     Portugal|                8.06|
# |     EST|      Estonia|                8.02|
# |     SWE|       Sweden|                7.97|
# |     LTU|    Lithuania|                7.94|
# +--------+-------------+--------------------+

# 2) TOP 10 стран с макс. кол-вом новых случаев за последнюю неделю марта 2021
window = Window.partitionBy("location").orderBy(col("new_cases").desc())
(
    data
    .filter((col('date') >= '2021-03-22') & (col('date') <= '2021-03-28') & (~col('iso_code').like('OWID_%')))
    .withColumn('rn', row_number().over(window))
    .filter(col('rn') == 1)
    .select('iso_code', 'date', 'location', 'new_cases')
    .sort(col('new_cases').desc())
    .show(10)
)
# +--------+----------+-------------+---------+
# |iso_code|      date|     location|new_cases|
# +--------+----------+-------------+---------+
# |     BRA|2021-03-25|       Brazil| 100158.0|
# |     USA|2021-03-24|United States|  86960.0|
# |     IND|2021-03-28|        India|  68020.0|
# |     FRA|2021-03-24|       France|  65392.0|
# |     POL|2021-03-26|       Poland|  35145.0|
# |     TUR|2021-03-27|       Turkey|  30021.0|
# |     ITA|2021-03-22|        Italy|  24501.0|
# |     DEU|2021-03-24|      Germany|  23757.0|
# |     PER|2021-03-25|         Peru|  19206.0|
# |     UKR|2021-03-26|      Ukraine|  18226.0|
# +--------+----------+-------------+---------+

# 3) Изменение числа случаев по отношению к пред.дню в России за последнюю неделю марта
window = Window.partitionBy("location").orderBy(col("date"))
(
    data
    .withColumn('cases_prev_day', lag('new_cases', 1).over(window))
    .filter((col('date') >= '2021-03-22') & (col('date') <= '2021-03-28') & (col('iso_code') == 'RUS'))
    .select('iso_code', 'date', 'location', 'new_cases', 'cases_prev_day', (col('new_cases') - col('cases_prev_day')).alias('cases_delta'))
    .sort(col('date').desc())
    .show(20)
)
# +--------+----------+--------+---------+--------------+-----------+
# |iso_code|      date|location|new_cases|cases_prev_day|cases_delta|
# +--------+----------+--------+---------+--------------+-----------+
# |     RUS|2021-03-28|  Russia|   8979.0|        8783.0|      196.0|
# |     RUS|2021-03-27|  Russia|   8783.0|        9073.0|     -290.0|
# |     RUS|2021-03-26|  Russia|   9073.0|        9128.0|      -55.0|
# |     RUS|2021-03-25|  Russia|   9128.0|        8769.0|      359.0|
# |     RUS|2021-03-24|  Russia|   8769.0|        8369.0|      400.0|
# |     RUS|2021-03-23|  Russia|   8369.0|        9195.0|     -826.0|
# |     RUS|2021-03-22|  Russia|   9195.0|        9215.0|      -20.0|
# +--------+----------+--------+---------+--------------+-----------+

spark.stop()
