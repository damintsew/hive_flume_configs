#!/usr/bin/env bash


[cloudera@quickstart ~]$ sudo -u hdfs hadoop fs -mkdir /user/adamintsev
[cloudera@quickstart ~]$ sudo -u hdfs hadoop fs -chown cloudera /user/adamintsev


flume-ng agent --conf /etc/flume-ng/conf/ --conf-file /etc/flume-ng/conf/adamintsev-flume.properties --name AgentDas -Dflume.root.logger=INFO,console

python3 data_gen.py 2>&1 | nc 192.168.1.7 6666