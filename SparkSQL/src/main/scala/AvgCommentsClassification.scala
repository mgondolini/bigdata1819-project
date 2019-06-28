
object AvgCommentsClassification {

  def main(args: Array[String]): Unit = {

    // Init SparkSession
    import org.apache.spark.sql._
    val sqlSession: SparkSession = SparkSession.builder().getOrCreate()

    //Further Imports
    import org.apache.spark.sql.functions._
    import sqlSession.implicits._

    // File path
    val caVideosCsvFilePath: String = "exam/dataset/cleaned/CAvideos.csv"
    val gbVideosCsvFilePath: String = "exam/dataset/cleaned/GBvideos.csv"
    val usVideosCsvFilePath: String = "exam/dataset/cleaned/USvideos.csv"

    // Import data from CSV to DataFrame
    val CaDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(caVideosCsvFilePath)

    val GbDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(gbVideosCsvFilePath)

    val UsDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(usVideosCsvFilePath)

    // UNION
    val trendingVideosUnionDF: DataFrame = CaDF.union(GbDF).union(UsDF).filter("comments_disabled == 'False' AND ratings_disabled == 'False'").withColumn("dislikes", when(col("dislikes").equalTo("0"), "1").otherwise(col("dislikes")))
    trendingVideosUnionDF.cache()

    // Empirically computed Threshold
    object Threshold {
      val MIN: Int = 4
      val MAX: Int = 6
    }

    // Classification for neutral, good and bad videos computed considering likes/dislikes ratio
    val neutralVideosDF: DataFrame = trendingVideosUnionDF.where("likes/dislikes >= " + Threshold.MIN + " AND likes/dislikes <= " + Threshold.MAX)
    neutralVideosDF.cache()

    val neutralNumber: Long = neutralVideosDF.count()

    val goodVideosDF: DataFrame = trendingVideosUnionDF.where("likes/dislikes > " + Threshold.MAX)
    goodVideosDF.cache()

    val goodNumber: Long = goodVideosDF.count()

    val badVideosDF: DataFrame = trendingVideosUnionDF.where("likes/dislikes < " + Threshold.MIN)
    badVideosDF.cache()

    val badNumber: Long = badVideosDF.count()

    // Average number of comments for classification
    val neutralComments: Array[String] = neutralVideosDF.agg(sum("comment_count") / neutralNumber).as[String].collect()

    val goodComments: Array[String] = goodVideosDF.agg(sum("comment_count") / goodNumber).as[String].collect()

    val badComments: Array[String] = badVideosDF.agg(sum("comment_count") / badNumber).as[String].collect()

    // Result
    val classificationDF: DataFrame = Seq(("neutral", neutralNumber, neutralComments(0)), ("good", goodNumber, goodComments(0)), ("bad", badNumber, badComments(0))).toDF("Classification", "Videos Number", "Average Comments")
    classificationDF.show()

    classificationDF.coalesce(1).write.mode("overwrite").option("header", "true").csv("project/spark/output/AvgCommentsClassification.csv")
  }
}