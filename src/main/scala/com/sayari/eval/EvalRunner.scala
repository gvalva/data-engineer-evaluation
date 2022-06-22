package com.sayari.eval

import com.sayari.spark.SparkConfig
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._

object EvalRunner {

  def main(args: Array[String]): Unit = {

    val sparkConfig: SparkConfig = new SparkConfig
    val spark = sparkConfig.spark

    val ofacPath = args(0)
    val gbrPath = args(1)
    val sinkPath = args(2)

    val rawOfacDF = spark.read.json(ofacPath)
    val rawGbrDF = spark.read.json(gbrPath)

    val joinedByNameDF = matchByName(rawOfacDF, rawGbrDF)
    val joinedByAliasNameDF = matchByAlias(rawOfacDF, rawGbrDF)
    val joinedByIDNumbersDF = matchByIDNumber(rawGbrDF, rawGbrDF)

    val completeMatchesDF = joinedByNameDF.union(joinedByIDNumbersDF).union(joinedByAliasNameDF)
    val completeMatchesWithNamesDF = consolidateMatches(rawOfacDF, rawGbrDF, completeMatchesDF)
    completeMatchesWithNamesDF.coalesce(1).write.option("header", true).csv(sinkPath)

    System.exit(0)
  }

  def matchByName(rawOfacDF: Dataset[Row], rawGbrDF: Dataset[Row]): Dataset[Row] = {
    val ofacNameDF = rawOfacDF
      .select("id", "name")
      .withColumn("ofac_normalized_name", trim(upper(col("name"))))
      .withColumnRenamed("id", "ofac_id")
      .drop("id")
      .drop("name")

    val gbrNameDF = rawGbrDF
      .select("id", "name")
      .withColumn("gbr_normalized_name", trim(upper(col("name"))))
      .withColumnRenamed("id", "gbr_id")
      .drop("name")

    ofacNameDF
      .join(gbrNameDF, ofacNameDF("ofac_normalized_name") === gbrNameDF("gbr_normalized_name"), "inner")
      .drop("ofac_normalized_name", "gbr_normalized_name")
      .dropDuplicates()
      .checkpoint()
  }

  def matchByAlias(rawOfacDF: Dataset[Row], rawGbrDF: Dataset[Row]): Dataset[Row] = {
    val ofacAliasNameDF = rawOfacDF
      .select("id", "aliases.value")
      .filter("aliases.value is not NULL")
      .filter("size(aliases.value) > 0")
      .withColumn("ofac_distinct_aliases", array_distinct(col("value")))
      .withColumn("ofac_alias", explode(col("value")))
      .withColumn("normalized_ofac_alias", trim(upper(col("ofac_alias"))))
      .withColumnRenamed("id", "ofac_id")
      .drop("alias.value")
      .drop("ofac_distinct_aliases")
      .drop("ofac_alias")
      .drop("value")
      .checkpoint()

    val gbrAliasNameDF = rawGbrDF
      .select("id", "aliases.value")
      .filter("aliases.value is not NULL")
      .filter("size(aliases.value) > 0")
      .withColumn("gbr_distinct_aliases", array_distinct(col("value")))
      .withColumn("gbr_alias", explode(col("value")))
      .withColumn("normalized_gbr_alias", trim(upper(col("gbr_alias"))))
      .withColumnRenamed("id", "gbr_id")
      .drop("alias.value")
      .drop("gbr_distinct_aliases")
      .drop("gbr_alias")
      .drop("value")
      .checkpoint()

    ofacAliasNameDF
      .join(gbrAliasNameDF, ofacAliasNameDF("normalized_ofac_alias") === gbrAliasNameDF("normalized_gbr_alias"), "inner")
      .drop("normalized_ofac_alias", "normalized_gbr_alias")
      .dropDuplicates()
      .checkpoint()
  }

  def matchByIDNumber(rawOfacDF: Dataset[Row], rawGbrDF: Dataset[Row]): Dataset[Row] = {
    val ofacIDNumbersDF = rawOfacDF
      .select("id", "id_numbers.value")
      .filter("id_numbers.value is not NULL")
      .filter("size(id_numbers.value) > 0")
      .withColumn("ofac_distinct_id_numbers", array_distinct(col("value")))
      .withColumn("ofac_id_numbers", explode(col("value")))
      .withColumn("normalized_ofac_id_numbers", trim(upper(col("ofac_id_numbers"))))
      .withColumnRenamed("id", "ofac_id")
      .drop("id_numbers.value")
      .drop("ofac_distinct_id_numbers")
      .drop("ofac_id_numbers")
      .drop("value")
      .checkpoint()

    val gbrIDNumbersDF = rawGbrDF
      .select("id", "id_numbers.value")
      .filter("id_numbers.value is not NULL")
      .filter("size(id_numbers.value) > 0")
      .withColumn("gbr_distinct_id_numbers", array_distinct(col("value")))
      .withColumn("gbr_id_numbers", explode(col("value")))
      .withColumn("normalized_gbr_id_numbers", trim(upper(col("gbr_id_numbers"))))
      .withColumnRenamed("id", "gbr_id")
      .drop("id_numbers.value")
      .drop("gbr_distinct_id_numbers")
      .drop("gbr_id_numbers")
      .drop("value")
      .checkpoint()

    ofacIDNumbersDF
      .join(gbrIDNumbersDF, ofacIDNumbersDF("normalized_ofac_id_numbers") === gbrIDNumbersDF("normalized_gbr_id_numbers"), "inner")
      .drop("normalized_ofac_id_numbers", "normalized_gbr_id_numbers")
      .dropDuplicates()
      .checkpoint()
  }

  def consolidateMatches(rawOfacDF: Dataset[Row], rawGbrDF: Dataset[Row], completeMatchesDF: Dataset[Row]): Dataset[Row] = {
    val nameIdPairOFAC_DF = rawOfacDF
      .select("name", "id")
      .withColumnRenamed("name", "ofac_name")
      .withColumnRenamed("id", "paired_ofac_id")

    val nameIdPairGBR_DF = rawGbrDF
      .select("name", "id")
      .withColumnRenamed("name", "gbr_name")
      .withColumnRenamed("id", "paired_gbr_id")

    completeMatchesDF
      .join(nameIdPairOFAC_DF, nameIdPairOFAC_DF("paired_ofac_id") === completeMatchesDF("ofac_id"), "inner")
      .join(nameIdPairGBR_DF, nameIdPairGBR_DF("paired_gbr_id") === completeMatchesDF("gbr_id"), "inner")
      .drop("paired_ofac_id", "paired_gbr_id")
      .checkpoint()
  }
}
