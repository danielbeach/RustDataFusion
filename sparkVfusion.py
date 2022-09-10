from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder.appName('SparkVFusion').master('local[6]').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
t1 = datetime.now()
df = spark.read.csv("data/*csv", header='true')

trans = df.groupBy('member_casual').agg(F.count('ride_id'))

trans.show()

t2 = datetime.now()
print("it took {x}".format(x=t2-t1))