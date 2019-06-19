// Local init
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}

val sc = new SparkContext(new SparkConf().setAppName("Youtube Trending Videos"))

// Init SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// File path
val categoryJsonFilePath: String = "dataset/US_category_id_flat.json"
val caVideosCsvFilePath: String = "dataset/CAvideos.csv"
val gbVideosCsvFilePath: String = "dataset/GBvideos.csv"
val usVideosCsvFilePath: String = "dataset/USvideos.csv"


//Import data from JSON file to DataFrame
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


//da controllare HEADER
val trendingVideosUnionDF = CaDF.union(GbDF).union(UsDF)

//val trendingVideosJoinCategoryDF = trendingVideosUnionDF.join(categoryNames, trendingVideosUnionDF("category_id")===categoryNames("id"), "left").drop("category_id").drop("id")

val broadcastVideosJoin = trendingVideosUnionDF.join(broadcast(categoryNames.withColumnRenamed("id", "category_id")), "category_id")

// Nel caso volessimo la colonna Category nella vecchia posisizione di category_id
val reorderedColumnNames: Array[String] = Array("video_id", "trending_date", "title", "channel_title", "category_id", "category", "publish_time", "tags", "views", "likes", "dislikes", "comment_count", "thumbnail_link", "comments_disabled", "ratings_disabled", "video_error_or_removed", "description")
val trendingVideosDFCategoryOrdered = broadcastVideosJoin.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

// Seleziona le colonne category e tags
val categoryTags = trendingVideosDFCategoryOrdered.select("category_id", "category", "tags")
//Si può fare anche così
//trendingVideosDFCategoryOrdered.registerTempTable("trendingVideos")
//val categoryTags2 = sqlContext.sql("select category, tags from trendingVideos")

// Raggruppa i tag per categoria
val groupedByCategory = categoryTags.groupBy(col("category_id"), col("category")).agg(collect_list(col("tags")) as "tags").withColumn("tags", split(concat_ws(",", col("tags")), "(\\|)"))
groupedByCategory.show()

// Converte il formato della colonna tags da Array[String] a String
val groupedByCategoryString = groupedByCategory.as[(String, String, Array[String])].map { case (id, category, tags) => (id, category, tags.mkString(",")) }.toDF("category_id","category", "tags")
groupedByCategoryString.show()

// Stampa i tag per categoria
groupedByCategory.select("category_id","category","tags").take(1).foreach(println)








// ------ COSE UTILI -------
// Trasformare il DF in RDD
val rows = groupedByCategory.rdd
// Prendere un elemento della riga
rows.map(row => row.get(1).asInstanceOf[String])

