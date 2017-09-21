package com.hcl.app

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]) {
    print("sdasd..");
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("simpleapp")
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text("src/main/resources/data.txt").as[String]

    val words = data.flatMap(value => value.split("\\s+"))

    val groupedWords = words.groupByKey(_.toLowerCase)

    val counts = groupedWords.count()

    counts.show()
  }

}
