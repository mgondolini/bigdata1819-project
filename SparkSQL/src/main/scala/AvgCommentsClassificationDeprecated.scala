import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object AvgCommentsClassificationDeprecated {

  def main(args: Array[String]): Unit = {

    // Init SparkSession
    import org.apache.spark.sql._
    //  val sqlSession: SparkSession = SparkSession.builder().getOrCreate()
    val sc = new SparkContext(new SparkConf().setAppName("Youtube Trending Videos"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //Further Imports
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // File path
    val categoryJsonFilePath: String = "exam/dataset/US_category_id_flat.json"
    val caVideosCsvFilePath: String = "exam/dataset/cleaned/CAvideos.csv"
    val gbVideosCsvFilePath: String = "exam/dataset/cleaned/GBvideos.csv"
    val usVideosCsvFilePath: String = "exam/dataset/cleaned/USvideos.csv"

    // Import data from JSON file to DataFrame
    //  val categoryNames: DataFrame = sqlSession.read.json(categoryJsonFilePath)

    val categoryNames = sqlContext.jsonFile(categoryJsonFilePath)

    // Import data from CSV to DataFrame
    //  val CaDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(caVideosCsvFilePath)
    //
    //  val GbDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(gbVideosCsvFilePath)
    //
    //  val UsDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(usVideosCsvFilePath)


    val schemaString = "video_id trending_date title channel_title category_id publish_time tags views likes dislikes comment_count thumbnail_link comments_disabled ratings_disabled video_error_or_removed description"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val CaVideos = sc.textFile(caVideosCsvFilePath)
    val rowCaRDD = CaVideos.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15)))
    val CaDF = sqlContext.createDataFrame(rowCaRDD, schema)


    val GbVideos = sc.textFile(gbVideosCsvFilePath)
    val rowGbRDD = GbVideos.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15)))
    val GbDF = sqlContext.createDataFrame(rowGbRDD, schema)

    val UsVideos = sc.textFile(usVideosCsvFilePath)
    val rowUsRDD = UsVideos.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15)))
    val UsDF = sqlContext.createDataFrame(rowUsRDD, schema)

    // UNION
    //  val trendingVideosUnionDF: DataFrame = CaDF.union(GbDF).union(UsDF).filter("comments_disabled == 'False' AND ratings_disabled == 'False'").withColumn("dislikes", when(col("dislikes").equalTo("0"), "1").otherwise(col("dislikes")))
    //  trendingVideosUnionDF.cache()

    val trendingVideosUnionDF: DataFrame = CaDF.unionAll(GbDF).unionAll(UsDF).filter(col("comments_disabled") === "False").filter(col("ratings_disabled") === "False").withColumn("dislikes", when(col("dislikes").equalTo("0"), "1").otherwise(col("dislikes")))
    trendingVideosUnionDF.cache()

    // Empirically computed Threshold
    object Threshold{
      val MIN: Int = 4
      val MAX: Int = 6
    }

    // Classification for neutral, good and bad videos computed considering likes/dislikes ratio
    val neutralVideosDF: DataFrame = trendingVideosUnionDF.where($"likes"/$"dislikes" >= Threshold.MIN).where($"likes"/$"dislikes" <= Threshold.MAX)
    neutralVideosDF.cache()

    val neutralNumber: Long = neutralVideosDF.count()

    val goodVideosDF: DataFrame = trendingVideosUnionDF.where($"likes"/$"dislikes" > Threshold.MAX)
    goodVideosDF.cache()

    val goodNumber: Long = goodVideosDF.count()

    val badVideosDF: DataFrame = trendingVideosUnionDF.where($"likes"/$"dislikes" < Threshold.MIN)
    badVideosDF.cache()

    val badNumber: Long = badVideosDF.count()

    // Average number of comments for classification
    val neutralComments: Array[String] = neutralVideosDF.agg(sum("comment_count")/neutralNumber).as[String].collect()

    val goodComments: Array[String] = goodVideosDF.agg(sum("comment_count")/goodNumber).as[String].collect()

    val badComments: Array[String] = badVideosDF.agg(sum("comment_count")/badNumber).as[String].collect()

    // Result
    val classificationDF: DataFrame = Seq(("neutral", neutralNumber, neutralComments(0)),("good", goodNumber, goodComments(0)), ("bad",  badNumber, badComments(0))).toDF("Classification", "Videos Number", "Average Comments")
    classificationDF.show()

    classificationDF.coalesce(1).write.mode("overwrite").option("header","true").csv("project/spark/output/AvgCommentsClassification.csv")
    classificationDF.coalesce(1).write.format("com.databricks.spark.csv").save("project/spark/output/AvgCommentsClassification.csv")

  }

  // Init SparkSession
  import org.apache.spark.sql._
//  val sqlSession: SparkSession = SparkSession.builder().getOrCreate()
  val sc = new SparkContext(new SparkConf().setAppName("Youtube Trending Videos"))
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //Further Imports
  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

  // File path
  val categoryJsonFilePath: String = "exam/dataset/US_category_id_flat.json"
  val caVideosCsvFilePath: String = "exam/dataset/cleaned/CAvideos.csv"
  val gbVideosCsvFilePath: String = "exam/dataset/cleaned/GBvideos.csv"
  val usVideosCsvFilePath: String = "exam/dataset/cleaned/USvideos.csv"

  // Import data from JSON file to DataFrame
//  val categoryNames: DataFrame = sqlSession.read.json(categoryJsonFilePath)

  val categoryNames = sqlContext.jsonFile(categoryJsonFilePath)

  // Import data from CSV to DataFrame
//  val CaDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(caVideosCsvFilePath)
//
//  val GbDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(gbVideosCsvFilePath)
//
//  val UsDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(usVideosCsvFilePath)


  val schemaString = "video_id trending_date title channel_title category_id publish_time tags views likes dislikes comment_count thumbnail_link comments_disabled ratings_disabled video_error_or_removed description"
  val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

  val CaVideos = sc.textFile(caVideosCsvFilePath)
  val rowCaRDD = CaVideos.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15)))
  val CaDF = sqlContext.createDataFrame(rowCaRDD, schema)


  val GbVideos = sc.textFile(gbVideosCsvFilePath)
  val rowGbRDD = GbVideos.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15)))
  val GbDF = sqlContext.createDataFrame(rowGbRDD, schema)

  val UsVideos = sc.textFile(usVideosCsvFilePath)
  val rowUsRDD = UsVideos.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15)))
  val UsDF = sqlContext.createDataFrame(rowUsRDD, schema)

  // UNION
//  val trendingVideosUnionDF: DataFrame = CaDF.union(GbDF).union(UsDF).filter("comments_disabled == 'False' AND ratings_disabled == 'False'").withColumn("dislikes", when(col("dislikes").equalTo("0"), "1").otherwise(col("dislikes")))
//  trendingVideosUnionDF.cache()

  val trendingVideosUnionDF: DataFrame = CaDF.unionAll(GbDF).unionAll(UsDF).filter(col("comments_disabled") === "False").filter(col("ratings_disabled") === "False").withColumn("dislikes", when(col("dislikes").equalTo("0"), "1").otherwise(col("dislikes")))
  trendingVideosUnionDF.cache()

  // Empirically computed Threshold
  object Threshold{
    val MIN: Int = 4
    val MAX: Int = 6
  }

  // Classification for neutral, good and bad videos computed considering likes/dislikes ratio
  val neutralVideosDF: DataFrame = trendingVideosUnionDF.where($"likes"/$"dislikes" >= Threshold.MIN).where($"likes"/$"dislikes" <= Threshold.MAX)
  neutralVideosDF.cache()

  val neutralNumber: Long = neutralVideosDF.count()

  val goodVideosDF: DataFrame = trendingVideosUnionDF.where($"likes"/$"dislikes" > Threshold.MAX)
  goodVideosDF.cache()

  val goodNumber: Long = goodVideosDF.count()

  val badVideosDF: DataFrame = trendingVideosUnionDF.where($"likes"/$"dislikes" < Threshold.MIN)
  badVideosDF.cache()

  val badNumber: Long = badVideosDF.count()

  // Average number of comments for classification
  val neutralComments: Array[String] = neutralVideosDF.agg(sum("comment_count")/neutralNumber).as[String].collect()

  val goodComments: Array[String] = goodVideosDF.agg(sum("comment_count")/goodNumber).as[String].collect()

  val badComments: Array[String] = badVideosDF.agg(sum("comment_count")/badNumber).as[String].collect()

  // Result
  val classificationDF: DataFrame = Seq(("neutral", neutralNumber, neutralComments(0)),("good", goodNumber, goodComments(0)), ("bad",  badNumber, badComments(0))).toDF("Classification", "Videos Number", "Average Comments")
  classificationDF.show()

  classificationDF.coalesce(1).write.mode("overwrite").option("header","true").csv("project/spark/output/AvgCommentsClassification.csv")
  classificationDF.coalesce(1).write.format("com.databricks.spark.csv").save("project/spark/output/AvgCommentsClassification.csv")
}