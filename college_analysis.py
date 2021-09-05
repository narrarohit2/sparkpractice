from pyspark.sql import SparkSession
from pyspark.sql.functions import col
sc = SparkSession.builder.appName('sparkpractice').getOrCreate()
df = sc.read.format("csv").option('header','true').option('inferschema','true').load("files/ForbesAmericasTopColleges2019.csv")
df.printSchema()
df.show(50)
df.filter(col("state") == 'DC')
df.select('Rank','Name','City','State').show()

