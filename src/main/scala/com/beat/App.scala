package com.beat

import java.nio.file.{Files, StandardOpenOption}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  *
  */

object App {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  /**
    * Creates concatenated text file based on a sample file with name "temp"+"name of sample text file"
    * Uses spark to calculate words in this file and prints time of evaluation
    * @param args
    * first - text file name
    * second - multiplier - number of text file copies
    */
  def main(args: Array[String]) {
    val start0 = System.nanoTime()
    if (args.length < 2) {
      System.err.println("Usage: App fileNAme limit")
      System.exit(1)
    }

    val logFileName = args(0)
    val limit = args(1).toInt
    import java.nio.file.Paths
    val logFile = Paths.get(logFileName)
    val tempLogFile = Paths.get("temp" + logFileName)
    if (Files.exists(tempLogFile)) {
      Files.delete(tempLogFile)
    }
    import java.nio.charset.StandardCharsets
    val charset = StandardCharsets.UTF_8
    val lines = Files.readAllLines(logFile, charset)
    for (x <- 1 to limit) {
      Files.write(tempLogFile, lines, charset, StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
    }

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val start = System.nanoTime()
    new App().run(tempLogFile.toString)
    println(s"Exec time = ${(System.nanoTime() - start) / 100000000 / 10f}")
  }


}

class App {


  def run(tempLogFile: String): Unit = {


    val spark = SparkSession.builder.appName("Simple Application").config("spark.hadoop.validateOutputSpecs", "false") getOrCreate()
    val logData = spark.read.textFile(tempLogFile).repartition(spark.sparkContext.defaultParallelism).rdd
    val counts = logData.flatMap(l => l.split("[\\W]")).map(w => w.trim).map(w => (w, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    counts.coalesce(1).saveAsTextFile("result.txt")
    spark.stop()
  }
}
