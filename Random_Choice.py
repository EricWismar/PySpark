'''
Sourced from https://stackoverflow.com/questions/66135534/how-to-update-a-dataframe-in-pyspark-with-random-values-from-another-dataframe

Given a data frame of:

df_a 

+-----+
|Name |
+-----+
|    a|
|    b|
|    c|
+-----+
#And another dataframe of:

df_b

+-----+
|Zip  |
+-----+
|06905|
|06901|
|06902|
+-----+
Add a new column to df_a from a random selection from df_b
'''

from pyspark.sql import functions as F

df_result = df_a.crossJoin(
    df_b.agg(F.collect_list("Zip").alias("Zip"))
).withColumn(
    "Zip",
    F.expr("shuffle(Zip)[0]")
)
