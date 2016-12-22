#!/bin/bash
# Script For Tiny Google
echo $1 ' -> echo $1'
if [ $1 == "-m" ]; then
	hadoop com.sun.tools.javac.Main WordCount.java
	jar cf wc.jar WordCount*.class
	hadoop jar wc.jar WordCount hdfs://had6110.cs.pitt.edu:8020/user/chatree/CS1699/Books wc_output
	
	hadoop com.sun.tools.javac.Main TinyGoogle.java
	jar cf gt.jar TinyGoogle*.class
	hadoop jar gt.jar TinyGoogle wc_output

	hdfs dfs -rm -r wc_output
elif [ $1 == "-s" ]; then 
	/opt/spark/bin/spark-shell --master yarn -i scala/Tiny_Google.scala
fi
