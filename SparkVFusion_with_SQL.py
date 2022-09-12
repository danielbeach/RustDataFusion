from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder.appName('SparkVFusion').master('local[6]').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
t1 = datetime.now()
df = spark.read.csv("data/*csv", header='true')
df.createOrReplaceTempView('trips')

trans = spark.sql("""
				SELECT COUNT('transaction_id') as cnt,
				    date_part('year', CAST(started_at as TIMESTAMP)) as year,
				    date_part('month', CAST(started_at as TIMESTAMP)) as month,
				    date_part('day', CAST(started_at as TIMESTAMP)) as day,
				    start_station_name
				FROM trips
				WHERE date_part('year', CAST(started_at as TIMESTAMP)) = 2022
				GROUP BY date_part('year', CAST(started_at as TIMESTAMP)),
				  date_part('month', CAST(started_at as TIMESTAMP)),
				    date_part('day', CAST(started_at as TIMESTAMP)),
				    start_station_name
	""")

trans.show()

t2 = datetime.now()
print("it took {x}".format(x=t2-t1))
