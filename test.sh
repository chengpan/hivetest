#!/bin/bash
CLASSPATH=.:$HIVE_HOME/conf
HADOOP_CLASSPATH=.:
for i in ${HADOOP_HOME}/lib/*.jar ; do
    CLASSPATH=$CLASSPATH:$i
done

for i in ${HADOOP_HOME}/lib/lib/*.jar ; do
    CLASSPATH=$CLASSPATH:$i
done

for i in ${HIVE_HOME}/lib/*.jar ; do
    CLASSPATH=$CLASSPATH:$i
    HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$i
done

export CLASSPATH 
export HADOOP_CLASSPATH

javac -cp ${CLASSPATH} Hive2JdbcClient.java 
jar cf hvtest.jar Hive2JdbcClient*.class
#java -cp ${CLASSPATH} Hive2JdbcClient

#this way, path will be located in local file system
#java -cp ${CLASSPATH} WordCount /data/log_server/test/mr/input /data/log_server/test/mr/output
