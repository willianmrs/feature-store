#!/bin/bash

sudo -E /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties 1> /tmp/kafka.log 2>&1 &
sudo -E /opt/zookeeper/bin/zkServer.sh start-foreground 1> /tmp/zookeeper.log 2>&1 &
jupyter notebook --ip 0.0.0.0 --allow-root --notebook-dir /tmp >> /tmp/jupyter.logs 2>&1  &

/bin/bash 