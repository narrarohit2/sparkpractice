from pyspark.sql import SparkSession
from pyspark.sql.functions import col
sc = SparkSession.builder.master("local").appName('sparkpractice').getOrCreate()
df = sc.read.format("csv").option('header','true').option('inferschema','true').load("files/ForbesAmericasTopColleges2019.csv")
df.printSchema()
df.show(50)
df2 = df
df2.filter(col("state") == 'DC')
df2.select('Rank','Name','City','State').show()
df.show()
df.groupBy('state').count().show()
