# words-counter
Spark application to count words, for testing hardware
Dependencies:
spark-2.4.0
scala 2.11

How to run:
spark-submit --master local[*] words-counter-1.0.jar test.txt 1000

Or use:
test.bat * 1000


Add this string to the spark/conf/log4j.properties:
log4j.logger.org.apache.spark.util.ShutdownHookManager=FATAL

This will remove Exception issued by SparkShutdownHookManager on Windows
