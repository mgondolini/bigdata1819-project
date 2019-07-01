import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object TopTagsInVideosCategoriesDeprecated {

  def main(args: Array[String]): Unit = {

    // Init SparkContext
    import org.apache.spark.sql._
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

    //Import data from JSON file to DataFrame
    val categoryNames = sqlContext.jsonFile(categoryJsonFilePath)

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
    val trendingVideosUnionDF: DataFrame = CaDF.unionAll(GbDF).unionAll(UsDF)

    // JOIN on Category Names
    val broadcastVideosJoin: DataFrame = trendingVideosUnionDF.join(broadcast(categoryNames.withColumnRenamed("id", "category_id")), "category_id")
    broadcastVideosJoin.registerTempTable("trendingVideosTmp")

    // Dataframe composed by category and tags columns
    val categoryTags: DataFrame = sqlContext.sql("select category, tags from trendingVideosTmp")
    categoryTags.show()

    // Explode tag list
    val categoryTagsExploded: DataFrame = categoryTags.withColumn("tags", explode(split($"tags", "(\\|)"))).filter("tags != '[none]'").withColumn("tags", lower($"tags"))
    categoryTagsExploded.registerTempTable("categoryTagsExplodedTmp")
    sqlContext.cacheTable("categoryTagsExplodedTmp")


    // Count the occurrences of the same tags for each category, create a column count_tag to store them
    val tagsCount: DataFrame = sqlContext.sql("select category, tags, COUNT(*) from categoryTagsExplodedTmp group by category, tags").withColumnRenamed("count(1)", "count_tag")
    tagsCount.registerTempTable("tagsCountTmp")
    sqlContext.cacheTable("tagsCountTmp")


    // Select top 10 rows for each category, ordered by descending tag count
    val top10query: String = """
                               | select category, tags, count_tag
                               |	from (select *, row_number() over (partition by category order by category, count_tag desc) as top_n_rows
                               |				  from tagsCountTmp ) a
                               |	where top_n_rows <= 10
                               |	order by a.category, a.count_tag desc
                             """.stripMargin

    val top10CategoryTags: DataFrame = sqlContext.sql(top10query)
    top10CategoryTags.registerTempTable("top10CategoryTagsTmp")
    sqlContext.cacheTable("top10CategoryTagsTmp")
    top10CategoryTags.show()

    // Group Dataframe by category, creating a list of tags#count values in column top10_tags
    val groupByCategoryQuery: String = """
                                         | select category, max(top10_tags)
                                         | from (select *, collect_list(concat(tags, "#", count_tag)) over (partition by category order by count_tag desc) as top10_tags
                                         |       from top10CategoryTagsTmp ) a
                                         | group by a.category
                                       """.stripMargin

    val groupedByCategory: DataFrame = sqlContext.sql(groupByCategoryQuery)
    groupedByCategory.cache()

    // Field top10_tags converted to string to save the DataFrame as csv and for a better visualization
    val groupedByCategoryString: DataFrame = groupedByCategory.as[(String, Array[String])].map{ case(category, top10_tags) => (category, top10_tags.mkString(",")) }.toDF("category", "top10_tags")
    groupedByCategoryString.show()
    groupedByCategoryString.collect().foreach(println)

    groupedByCategoryString.coalesce(1).write.mode("overwrite").option("header","true").csv("project/spark/output/TopTagsInVideosCategories.csv")

  }
}



