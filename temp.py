feature_schema = StructType([StructField("OP_CARRIER", StringType(), True),
StructField("ORIGIN", StringType(), True)])

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf,substring

convert_time_to_hour = udf(lambda x: x if len(x) == 4 else "0{}".format(x),StringType())

args = {"feature":"AA,ICT"}#,DFW,1135,85,11,328"}

#def predict(args):
flight_features = args["feature"].split(",")
features = spark.createDataFrame([
  (
    flight_features[0],
    flight_features[1])],schema=feature_schema
  )

l = [('Alice', '')]
spark.createDataFrame(l,['name','']).show()


#features = features.withColumn('CRS_DEP_HOUR', substring(convert_time_to_hour("CRS_DEP_TIME"),0,2).cast('float'))
#  result = model.transform(features).collect()[0].prediction
#  return {"result" : result}
