## Project : Network Anomaly Detection using spark
## Date Of Start : 05/01/2019
## Date Of completion:


## importing the libraries

from pyspark.sql import SparkSession
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

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
sns.relplot(x="count", y="label" , size= "count",sizes=(40, 500), alpha=.5, palette="muted", data=df_show.toPandas())
plt.show()
df_show.toPandas().hist(bins=[1000, 2000, 4000, 6000, 8000, 10000, 15000, 20000, 25000])
plt.show()



spark.stop()
