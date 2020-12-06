FROM python:3.8.6-buster

RUN mkdir -p /home/ifood/conf


RUN apt-get update -y
RUN apt-get install python3-pip sudo netcat wget curl vim software-properties-common zip unzip -y
RUN pip3 install jupyter spylon-kernel setuptools wheel numpy scipy scikit-learn lightgbm
RUN python -m spylon_kernel install
ARG PARAM_UID=8500
ARG PARAM_GID=8500

RUN mkdir -p /etc/sudoers.d/
RUN groupadd -g "${PARAM_GID}" ifood || true
RUN adduser --disabled-password --uid "${PARAM_UID}" --gid "${PARAM_GID}" --gecos ifood ifood
RUN groupadd -r supersudo && echo "%supersudo ALL=(ALL:ALL) NOPASSWD: ALL" > /etc/sudoers.d/supersudo
RUN usermod -a -G supersudo ifood
RUN mkdir -p /home/ifood/project

ENV JAVA_HOME /usr/lib/java
ENV PATH $PATH:$JAVA_HOME/bin
ENV HADOOP_VERSION 3.2.0
ENV HADOOP_HOME /usr/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

ENV HADOOP_USER_NAME hdfs
ENV SPARK_DIST_CLASSPATH ""
ENV SPARK_HOME /opt/spark
ENV PATH $HADOOP_HOME/bin:/home/ifood/bin:$SPARK_HOME/bin:$PATH

RUN rm -rf /usr/bin/java
RUN rm -rf /etc/alternatives/java

ADD scripts/start.sh /home/ifood
RUN ls -l /home/ifood
RUN chmod +x /home/ifood/start.sh

RUN mkdir -p /home/ifood/.jupyter/
ADD conf/jupyter_notebook_config.py /home/ifood/.jupyter
RUN chown ifood:ifood -R  /home/ifood

RUN curl -kL https://repoz.dextra.com.br/repoz/r/pub/jdk/oracle/jdk-8u221-linux-x64.tar.gz > /opt/jdk-8u221-linux-x64.tar.gz

#RUN curl -kL https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz  > /opt/spark-2.4.4-bin-hadoop2.7.tgz
RUN curl -kL https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz > /opt/spark-3.0.1-bin-hadoop3.2.tgz
RUN curl -kL http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz > /opt/hadoop-$HADOOP_VERSION.tar.gz

WORKDIR /opt
RUN tar xzfv jdk-8u221-linux-x64.tar.gz
RUN mv jdk1.8.0_221 /usr/lib/java

#RUN tar xzf /opt/spark-2.4.4-bin-hadoop2.7.tgz
RUN tar xzf /opt/spark-3.0.1-bin-hadoop3.2.tgz
RUN ln -s spark-3.0.1-bin-hadoop3.2 /opt/spark

RUN tar xzfv /opt/hadoop-$HADOOP_VERSION.tar.gz

RUN rm -rf /opt/hadoop-$HADOOP_VERSION/share/doc
RUN ln -s /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME 
RUN sudo mkdir $HADOOP_HOME/logs

ADD conf/envs.sh /home/ifood
RUN cat /home/ifood/envs.sh | tee -a /etc/bash.bashrc

WORKDIR /home/ifood

ADD scripts/bin bin

EXPOSE 8888

USER ifood

RUN mkdir -p /tmp/spark/logs
WORKDIR "/home/ifood/project"
ADD conf/hadoop-local /home/ifood/conf/hadoop-local
RUN sudo chmod +x /home/ifood/bin/*
RUN sudo mkdir /artefacts/
ADD ./artefact /artefacts
RUN mkdir /tmp/ifood
CMD [ "/home/ifood/start.sh" ]
