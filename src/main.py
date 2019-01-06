## Project : Network Anomaly Detection using spark
## Date Of Start : 05/01/2019
## Date Of completion:


## importing the libraries

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoderEstimator
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt

# creating the spark session
spark = SparkSession.builder.appName("NetworkAnomaly").master('spark://namenode:7077').getOrCreate()

# our data is present in hdfs so firstly we will load it.
data_without_header = spark.read.option("inferSchema", True).option('header', False).csv(
    'hdfs://namenode:8020/networkanomaly/data.txt')

data = data_without_header.toDF("duration", "protocol_type", "service", "flag",
                                "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
                                "hot", "num_failed_logins", "logged_in", "num_compromised",
                                "root_shell", "su_attempted", "num_root", "num_file_creations",
                                "num_shells", "num_access_files", "num_outbound_cmds",
                                "is_host_login", "is_guest_login", "count", "srv_count",
                                "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
                                "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
                                "dst_host_count", "dst_host_srv_count",
                                "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
                                "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
                                "dst_host_serror_rate", "dst_host_srv_serror_rate",
                                "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
                                "label")
# this will show the data
data.show(10)

# first we will check the different number of labels and count of those.

df_show = data.select('label').groupBy('label').count().orderBy(['count'], ascending=False).toDF("label", "count")

# visualizing data using seaborn
sns.set(style="white")
sns.relplot(x="count", y="label", size="count", sizes=(40, 500), alpha=.5, palette="muted", data=df_show.toPandas())
plt.show()
df_show.toPandas().hist(bins=[1000, 2000, 4000, 6000, 8000, 10000, 15000, 20000, 25000])
plt.show()

# As K means can be applied only on numeric data. at this moment we will remove them
# Data
df_num = data.drop("protocol_type", "service", "flag").cache()
col = df_num.columns
# FeatureVector
assembler = VectorAssembler(inputCols=col[:-1], outputCol='featureVector')

# model
kmeans = KMeans(predictionCol="cluster", k=2, featuresCol='featureVector')

# pipeline to process it
pipeline = Pipeline(stages=[assembler, kmeans])
pipModel = pipeline.fit(df_num)
prediction = pipModel.transform(df_num)
prediction.select("cluster", "label").groupBy("cluster", "label").count().orderBy("cluster", "label",
                                                                                  ascending=True).show(25)

# ## Coice of k
cost = np.zeros(6)
i = 0
for k in range(20, 140, 20):
    kmea = KMeans().setK(k).setSeed(1).setFeaturesCol("featureVector")
    model = kmea.fit(prediction.sample(False, 0.1, seed=42))
    cost[i] = model.computeCost(prediction)
    i += 1
print(cost)

## visualizing the value of k with cost
fig, ax = plt.subplots(1, 1)
ax.plot(range(20, 140, 20), cost, color='r')
ax.set_xlabel('k')
ax.set_ylabel('cost')
plt.show()

# Training Model using K= 100:
# passing parameter through pipeline
paramMap = {kmeans.k: 100}
pipeline = Pipeline(stages=[assembler, kmeans])
pipModel = pipeline.fit(df_num, paramMap)
prediction = pipModel.transform(df_num)
prediction.select("cluster", "label").groupBy("cluster", "label").count().orderBy("cluster", "label",
                                                                                  ascending=True).show(25)


## converting catogerical variable into vectors
def catToVec(inputCon):
    indexer = StringIndexer(inputCol=inputCon, outputCol=inputCon + "_indexed")
    encoder = OneHotEncoderEstimator(inputCols=inputCon + "_indexed", outputCols=inputCon + "_vec")
    pipHot = Pipeline(stages=[indexer, encoder])
    return (pipHot, inputCon + "_vec")


res = catToVec(df_show.select("label"))

print(res)
# print(prediction.columns)
# print(prediction.show(10))
spark.stop()
