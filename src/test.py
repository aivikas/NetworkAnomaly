# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName("NetworkAnomaly_test").master('spark://namenode:7077').getOrCreate()
#
# # our data is present in hdfs so firstly we will load it.
# data_without_header = spark.read.option("inferSchema", True).option('header', False).csv(
#     'hdfs://namenode:8020/networkanomaly/data.txt')
#
# data = data_without_header.toDF("duration", "protocol_type", "service", "flag",
#                                 "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
#                                 "hot", "num_failed_logins", "logged_in", "num_compromised",
#                                 "root_shell", "su_attempted", "num_root", "num_file_creations",
#                                 "num_shells", "num_access_files", "num_outbound_cmds",
#                                 "is_host_login", "is_guest_login", "count", "srv_count",
#                                 "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
#                                 "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
#                                 "dst_host_count", "dst_host_srv_count",
#                                 "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
#                                 "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
#                                 "dst_host_serror_rate", "dst_host_srv_serror_rate",
#                                 "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
#                                 "label")
#
# col = data.columns
# print(col[:-1])
#
# spark.stop()
import numpy as np
cost = np.zeros(5)
j = 0
for i in range(20, 120, 20):
    cost[j] = i
    j+=1

for i in range(0,5):
    print(cost[i])
print(cost)
print(cost)