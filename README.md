# Network Anomaly Detection using Spark

In this project we are going to detect the anomaly in network trafic using the mllib k means spark.
The inherent problem of anomaly detection is, as its name implies, that of finding unusual things.

Anomaly detection is often used to find fraud, detect network attacks, or discover problems in servers or other sensor-equipped machinery. In these cases, it’s important to be able to find new types of anomalies that have never been seen before—new forms of fraud, intrusions, and failure modes for servers.

Unsupervised learning techniques are useful in these cases because they can learn what input data normally looks like, and therefore detect when new data is unlike past data. Such new data is not necessarily attacks or fraud; it is simply unusual, and therefore, worth further investigation.

## Selecting Algorithm :
#### K-means Clustering:
Clustering is the best-known type of unsupervised learning. Clustering algorithms try
to find natural groupings in data. Data points that are like one another but unlike
others are likely to represent a meaningful grouping, so clustering algorithms try to
put such data into the same cluster.