# Common Ingest Pipelines
This module will have the common util methods needed for invoking any of the REST api to be consumed.
The end points of this application can be triggered externally, eg by a process
running on Hadoop Cluster under orchestration by Oozie. It would fetch the data requested from one of the externally
hosted Marketing APIs (eg, Google Ads Manager, Facebook)/S3/SFTP and land the data to HDFS in a custom location
controlled by the client.
