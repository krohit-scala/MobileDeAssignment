package com.org.assignment.unsolved

// import com.org.assignment.unsolved.AdsAndFrauds
import com.org.assignment.SparkSessionBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}
 import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


class AdsAndFraudsAnswers (adsInputDir: String, fraudulentInputDir: String, questionNumber: Integer) {
  val spark = SparkSessionBuilder.build
  import spark.implicits._
  
  // Generate base data for fraudulent ads
  val fraudAdsDf = getFraudulentAds(spark, adsInputDir, fraudulentInputDir)
  println(s"Question Number: ${questionNumber}")
  
  def getAnswer : DataFrame = {
    if(questionNumber != null && questionNumber == 1)
      return findFraudAds
    else if(questionNumber != null && questionNumber == 2)
      return findAverage
    else if(questionNumber != null && questionNumber == 3)
      return findAverageDateWise
    else
      return null
  }
  
  // Solution for requirement 1
  def findFraudAds : DataFrame = {
    // Get the answer
    val answer1 = fraudAdsDf.select($"adId", $"dateCreated".alias("dateCreated"))
    
    // Return the answer
    answer1    
  }
  
  // Solution for requirement 2
  def findAverage : DataFrame = {
    // Get the answer
    val answer2 = fraudAdsDf.agg(avg($"price").alias("AveragePrice"))

    // Return the answer
    answer2
  }

  // Solution for requirement 3
  def findAverageDateWise : DataFrame = {
    // Get the answer
    val answer3 = fraudAdsDf.groupBy($"dateCreated".alias("dateCreated"))
                        .agg(avg("price").alias("AveragePrice"))
                        .select(
                            $"dateCreated",
                            when(
                                $"AveragePrice" === null, 0
                            ).otherwise(
                                $"AveragePrice"
                            ).alias("AveragePrice")
                        )
    
    // Return the answer
    answer3
  }
  
  // Create the base dataset for finding answers
  def getFraudulentAds (spark: SparkSession, adsInputDir: String, fraudulentInputDir: String)  : DataFrame = { 
    
    // Schema for ads dataset
    val adsSchema = StructType(
        List(
            StructField("adId",LongType,true),
            StructField("creationTime",LongType,true),
            StructField("price",DoubleType,true)
        )
    )

    // Loading ads dataset
    val adsDf = spark.read.format("csv")
                        .option("header", "false")
                        .schema(adsSchema)
                        .option("path", adsInputDir)
                        .load
                        .withColumn(
                            "dateCreated",
                            date_format(
                                to_date(
                                    substring(
                                        from_unixtime($"creationTime".divide(1000)), 
                                        0,
                                        10
                                    ),
                                    "yyyy-MM-dd"
                                )
                            ,"dd-MM-yyyy")
                        )
    
    // Show the ads dataframe
    // adsDf.select($"*", from_unixtime($"creationTime".divide(1000)).alias("ts1")).where($"adId" === 230094435).show
    // adsDf.show 

    // Schema for ads dataset
    val fraudSchema = StructType(
        List(
            StructField("adId",LongType,true),
            StructField("reportedTime",StringType,true)
        )
    )
    
    // Loading ads dataset
    val fraudDf = spark.read.format("csv")
                        .option("header", "false")
                        .schema(fraudSchema)
                        .option("path", fraudulentInputDir)
                        .load
    
    // Show the fraud dataframe
    // fraudDf.show

    // Renaming the column name in fraudDf to avoid ambiguity during joins
    val b = fraudDf.withColumnRenamed("adId", "adId1")
    val fraudAdsDf = adsDf.join(
                    b,
                    $"adId" === $"adId1",
                    "inner"
                  )
    
    // Show the joined dataframe
    // fraudAdsDf.show    

    // return the base dataset
    // fraudAdsDf.where($"adId" === 230094435).show
    fraudAdsDf
  }
}