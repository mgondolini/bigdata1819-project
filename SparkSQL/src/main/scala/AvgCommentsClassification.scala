// Local init
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


val sc = new SparkContext(new SparkConf().setAppName("Youtube Trending Videos"))

// Init SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//Further Imports
import org.apache.spark.sql.functions._
import sqlContext.implicits._

// File path
val categoryJsonFilePath: String = "project/dataset/US_category_id_flat.json"
val caVideosCsvFilePath: String = "project/dataset/cleaned/CAvideos.csv"
val gbVideosCsvFilePath: String = "project/dataset/cleaned/GBvideos.csv"
val usVideosCsvFilePath: String = "project/dataset/cleaned/USvideos.csv"

// Import data from JSON file to DataFrame
val categoryNames = sqlContext.read.json(categoryJsonFilePath)

// Import data from CSV to DataFrame
val CaDF = sqlContext.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(caVideosCsvFilePath)

val GbDF = sqlContext.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(gbVideosCsvFilePath)

val UsDF = sqlContext.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(usVideosCsvFilePath)

// UNION
val trendingVideosUnionDF = CaDF.union(GbDF).union(UsDF).filter("comments_disabled == 'False' AND ratings_disabled == 'False'").withColumn("dislikes", when(col("dislikes").equalTo("0"), "1").otherwise(col("dislikes")))
trendingVideosUnionDF.cache()

// Empirically computed Threshold
object Threshold{
  val MIN = 4
  val MAX = 6
}

// Classification for neutral, good and bad videos computed considering likes/dislikes ratio
val neutralVideosDF = trendingVideosUnionDF.where("likes/dislikes >= " + Threshold.MIN + " AND likes/dislikes <= " + Threshold.MAX)
neutralVideosDF.cache()

val neutralNumber = neutralVideosDF.count()

val goodVideosDF = trendingVideosUnionDF.where("likes/dislikes > " + Threshold.MAX)
goodVideosDF.cache()

val goodNumber = goodVideosDF.count()

val badVideosDF = trendingVideosUnionDF.where("likes/dislikes < " + Threshold.MIN)
badVideosDF.cache()

val badNumber = badVideosDF.count()

// Average number of comments for classification
val neutralComments = neutralVideosDF.agg(sum("comment_count")/neutralNumber).as[String].collect()

val goodComments = goodVideosDF.agg(sum("comment_count")/goodNumber).as[String].collect()

val badComments = badVideosDF.agg(sum("comment_count")/badNumber).as[String].collect()

// Result
val classificationDF = Seq(("neutral", neutralNumber, neutralComments(0)),("good", goodNumber, goodComments(0)), ("bad",  badNumber, badComments(0))).toDF("Classification", "Videos Number", "Average Comments")
classificationDF.show()

classificationDF.coalesce(1).write.mode("overwrite").option("header","true").csv("project/spark/output/AvgCommentsClassification.csv")