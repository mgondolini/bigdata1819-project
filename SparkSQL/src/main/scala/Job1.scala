// Local init
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

val sc = new SparkContext(new SparkConf().setAppName("Youtube Trending Videos"))

// Init SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// File path
val categoryJsonFilePath: String = "dataset/US_category_id_flat.json"
val caVideosCsvFilePath: String = "dataset/CAvideos.csv"
val gbVideosCsvFilePath: String = "dataset/GBvideos.csv"
val usVideosCsvFilePath: String = "dataset/USvideos.csv"


// Import data from JSON file to DataFrame
val categoryNames = sqlContext.read.json(categoryJsonFilePath)

// SPARK SHELL 1
//Load to dfa CSV without a defined schema
//import org.apache.spark.sql.types.{StructType,StructField,StringType}
//val videos = sc.textFile(caVideosCsvFilePath)
//val schemaString = "video_id trending_date title channel_title category_id publish_time tags views likes dislikes comment_count thumbnail_link comments_disabled ratings_disabled video_error_or_removed description"
//val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
//val rowRDD = videos.map(_.split(",")).map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10), e(11), e(12), e(13), e(14), e(15)))
//val videosDF = sqlContext.createDataFrame(rowRDD, schema)

// SPARK SHELL 2

val CaDF = sqlContext.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(caVideosCsvFilePath)

val GbDF = sqlContext.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(gbVideosCsvFilePath)

val UsDF = sqlContext.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(usVideosCsvFilePath)

// UNION

// Da controllare HEADER
val trendingVideosUnionDF = CaDF.unionAll(GbDF).unionAll(UsDF)

// Alternativa
val unionWhileLoading = sqlContext.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(gbVideosCsvFilePath,usVideosCsvFilePath,caVideosCsvFilePath)

// Classification
val neutral = trendingVideosUnionDF.where("likes/(dislikes) > 4 AND likes/(dislikes) < 6 ") // 4988, MR=7224.0
val neutralNumber = neutral.count()

val good = trendingVideosUnionDF.where("likes/(dislikes) > 6") // 106727, MR=:03881.0
val goodNumber = good.count()

val bad = trendingVideosUnionDF.where("likes/(dislikes) < 4") // 7804, MR=7742.0
val badNumber = bad.count()

// Average number of comments for classification
val neutralComments = neutral.agg(sum("comment_count")/neutralNumber).as[String].collect() // 8234.236367281475, MR=7133.091085271318

val goodComments = good.agg(sum("comment_count")/goodNumber).as[String].collect() // 8587.243958885756, MR=8723.434044724252

val badComments = bad.agg(sum("comment_count")/badNumber).as[String].collect() // 13236.336622245002, MR=13390.349005424954

// Result
val classificationDF = Seq(("neutral", neutralNumber, neutralComments(0)),("good", goodNumber, goodComments(0)), ("bad",  badNumber, badComments(0))).toDF("Classification", "Count", "Average Comments")
classificationDF.show()




//TODO Aggiungere una nuova colonna per la classificazione e raggruppare in base a quella (???)
//trendingVideosUnionDF.registerTempTable("trendingVideos")
//val trendingVideosClassDF = sqlContext.sql("select * where(likes/(dislikes) > 6) as classification from trendingVideos")
//good.groupBy("classification").(sum("comment_count")/good.count())
