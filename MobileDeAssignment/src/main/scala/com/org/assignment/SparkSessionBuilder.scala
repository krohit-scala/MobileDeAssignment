package com.org.assignment

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def build : SparkSession = {
    val conf = new SparkConf().setMaster("local")
    
    val sparkSession = SparkSession.builder
      .config(conf)
      .appName("Ads And Frauds")
      .getOrCreate()
      sparkSession
  }
}