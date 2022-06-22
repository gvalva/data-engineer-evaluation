package com.sayari.spark
import org.apache.spark.sql.SparkSession

import java.io.File

class SparkConfig {
  
  val appName = "Evaluation"
  val masterUri = "local[*]"
  val sparkEventLogDir = "file:///tmp/eventlog"
  val checkpointDirectory = "file:///tmp/evaluation/checkpoint"

  def spark: SparkSession = {
    val ssb: SparkSession.Builder = SparkSession.builder
      .appName(appName)

    if (masterUri.nonEmpty) {
      ssb.master(masterUri)
    }

    ssb.config("spark.eventLog.enabled", value = true)
    ssb.config("spark.sql.optimizer.maxIterations", 300)
    ssb.config("spark.driver.maxResultSize", 0)
    ssb.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    ssb.config("spark.scheduler.mode", "FAIR")

    if (sparkEventLogDir.startsWith("file://")) {
      val f: File = new File(sparkEventLogDir.replace("file://", ""))
      if (!f.exists) {
        f.mkdirs
      }
    }

    ssb.config("spark.eventLog.dir", s"${sparkEventLogDir}")
    val sparkSession = ssb.getOrCreate()

    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    sparkSession.sparkContext.setCheckpointDir(checkpointDirectory)

    sparkSession
  }

}
