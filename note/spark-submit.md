# spark-submit 任务都做了什么  
### spark-submit后执行的java程序  
我们提交如下命令：  
```
/usr/local/spark/spark-2.4.3-bin-hadoop2.7/bin/spark-submit \
--master local[1] \
--driver-memory 1000M \
--class com.jason.exaples.MysqlExample \
--jars /home/jason/.m2/repository/mysql/mysql-connector-java/5.1.45/mysql-connector-java-5.1.45.jar \
spark-exaples-1.0-SNAPSHOT.jar
```

则spark-submit脚本实际执行的命令是：  
```
/usr/local/jdk/jdk1.8.0_131/bin/java \
-cp /usr/local/spark/spark-2.4.3-bin-hadoop2.7/conf/:/usr/local/spark/spark-2.4.3-bin-hadoop2.7/jars/* \
-Xmx1000M org.apache.spark.deploy.SparkSubmit \
--master local[1] \
--conf spark.driver.memory=1000M \
--class com.jason.exaples.MysqlExample \
--jars /home/jason/.m2/repository/mysql/mysql-connector-java/5.1.45/mysql-connector-java-5.1.45.jar \
spark-exaples-1.0-SNAPSHOT.jar
```
可见最终启动的是org.apache.spark.deploy.SparkSubmit， 器后跟的是我们submi时候的参数  

