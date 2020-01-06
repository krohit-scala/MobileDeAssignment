package com.org.assignment.unsolved

import com.org.assignment.unsolved.AdsAndFraudsAnswers
import com.org.assignment.SparkSessionBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
import scala.io.Source
import java.io.FileInputStream
import java.io.InputStream

object OutputWriter {
  def write(outDF: DataFrame, path: String) = {
    // For demonstration purpose only. 
    // Repartition clause would be removed while writing the DataFrame in real world use cases.
    if(path != null && !path.isEmpty){
      val numPartitions = 1
      outDF.repartition(numPartitions)
           .write
           .format("csv")
           .option("path", path)
           .save
    }
  }
}

object AdsAndFrauds {
  def main(args: Array[String]): Unit = {
    // Variables for input arguments
    var adsInputDir : String = null
    var fraudulentInputDir : String = null
    var outputDir : String = null
    var questionNumber = 0
    
    // If arguments are supplied, consume them
    if (args != null && !(args.isEmpty) && args.size < 4) {
      adsInputDir = args(0)
      fraudulentInputDir = args(1)
      questionNumber = args(2).toInt
      outputDir = args(3)
    }
    else{
      // Read properties file for input files
      val inputFile : InputStream = new FileInputStream("application.properties")
      val properties = new Properties()
      properties.load(inputFile)
      adsInputDir = properties.getProperty("adsInputDir")
      fraudulentInputDir = properties.getProperty("fraudulentInputDir")
      questionNumber = 1
      outputDir = null
    }

    val answerDf = new AdsAndFraudsAnswers(adsInputDir, fraudulentInputDir, questionNumber).getAnswer
    // answerDf.where($"adId" === 230094435).show
    answerDf.show
    
    // Write Output at destination
    if(outputDir != null && !outputDir.isEmpty && answerDf != null){
      OutputWriter.write(answerDf, outputDir)
    }
  }

}