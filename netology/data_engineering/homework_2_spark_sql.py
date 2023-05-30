from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, lag, row_number


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
        'iso_code', 'location', (col('total_cases') / col('population')).alias('cases_per_population'))
    .sort(col('cases_per_population').desc())
    .show(15)
)
# +--------+-------------+--------------------+
# |iso_code|     location|cases_per_population|
# +--------+-------------+--------------------+
# |     AND|      Andorra| 0.15543907331909662|
# |     MNE|   Montenegro| 0.14523725364693293|
# |     CZE|      Czechia| 0.14308848404077998|
# |     SMR|   San Marino|  0.1393717956273204|
# |     SVN|     Slovenia| 0.10370805779121203|
# |     LUX|   Luxembourg| 0.09847342390123583|
# |     ISR|       Israel| 0.09625106044786802|
# |     USA|United States| 0.09203010995860707|
# |     SRB|       Serbia| 0.08826328557933491|
# |     BHR|      Bahrain| 0.08488860079114566|
# |     PAN|       Panama| 0.08228739065460762|
# |     PRT|     Portugal| 0.08058699735120368|
# |     EST|      Estonia|  0.0802268157965955|
# |     SWE|       Sweden| 0.07969744347858805|
# |     LTU|    Lithuania| 0.07938864728274825|
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

window = Window.partitionBy("location").orderBy(col("date"))
(
    data
    .filter((col('date') >= '2021-03-22') & (col('date') <= '2021-03-28') & (col('iso_code') == 'RUS'))
    .withColumn('cases_prev_day', lag('new_cases', 1).over(window))
    .select('iso_code', 'date', 'location', 'new_cases', 'cases_prev_day', (col('new_cases') - col('cases_prev_day')).alias('cases_delta'))
    .show(20)
)
# +--------+----------+--------+---------+--------------+-----------+
# |iso_code|      date|location|new_cases|cases_prev_day|cases_delta|
# +--------+----------+--------+---------+--------------+-----------+
# |     RUS|2021-03-22|  Russia|   9195.0|          null|       null|
# |     RUS|2021-03-23|  Russia|   8369.0|        9195.0|     -826.0|
# |     RUS|2021-03-24|  Russia|   8769.0|        8369.0|      400.0|
# |     RUS|2021-03-25|  Russia|   9128.0|        8769.0|      359.0|
# |     RUS|2021-03-26|  Russia|   9073.0|        9128.0|      -55.0|
# |     RUS|2021-03-27|  Russia|   8783.0|        9073.0|     -290.0|
# |     RUS|2021-03-28|  Russia|   8979.0|        8783.0|      196.0|
# +--------+----------+--------+---------+--------------+-----------+

spark.stop()
