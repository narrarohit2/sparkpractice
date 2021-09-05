from pyspark.sql import SparkSession

sc = SparkSession.builder.appName('sparkpractice').getOrCreate()
df = sc.read.format("csv").option('header','true').option('inferschema','true').load("files/ForbesAmericasTopColleges2019.csv")
df.printSchema()
df.show(20)
df.printSchema()
df.describe()





