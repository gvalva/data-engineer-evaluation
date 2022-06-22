package com.sayari.spark

import org.apache.spark.sql.SparkSession

trait SparkManager {

  protected var sparkConfig: SparkConfig = new SparkConfig

  val spark: SparkSession = sparkConfig.spark

}
