FROM debian:7

RUN apt-get -y update
RUN apt-get install -y wget

RUN wget http://public-repo-1.hortonworks.com/HDP/debian7/2.x/updates/2.3.4.0/hdp.list -O /etc/apt/sources.list.d/hdp.list

RUN apt-get -y update
RUN apt-get install -y default-jre
RUN apt-get install -y --force-yes hadoop hadoop-yarn hive spark-2-3-4-0-3485

COPY conf/spark-defaults.conf /usr/hdp/current/spark-client/conf/spark-defaults.conf
COPY conf/hive-site.xml /usr/hdp/current/spark-client/conf/hive-site.xml
COPY conf/yarn-site.xml /usr/hdp/current/hadoop-client/conf/yarn-site.xml
COPY conf/core-site.xml /usr/hdp/current/hadoop-client/conf/core-site.xml

COPY target/scala-2.10/handy-pipeline-assembly-0.2.jar /handy-pipeline-assembly.jar

# COPY ./entrypoint.sh /
# ENTRYPOINT ["/entrypoint.sh"]
