
object TopTagsInVideosCategories {

  // Init SparkSession
  import org.apache.spark.sql._
  val sqlSession: SparkSession = SparkSession.builder().getOrCreate()

  //Further Imports
  import org.apache.spark.sql.functions._
  import sqlSession.implicits._

  // File path
  val categoryJsonFilePath: String = "project/dataset/US_category_id_flat.json"
  val caVideosCsvFilePath: String = "project/dataset/cleaned/CAvideos.csv"
  val gbVideosCsvFilePath: String = "project/dataset/cleaned/GBvideos.csv"
  val usVideosCsvFilePath: String = "project/dataset/cleaned/USvideos.csv"

  //Import data from JSON file to DataFrame
  val categoryNames: DataFrame = sqlSession.read.json(categoryJsonFilePath)

  // Import data from CSV to DataFrame
  val CaDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(caVideosCsvFilePath)

  val GbDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(gbVideosCsvFilePath)

  val UsDF: DataFrame = sqlSession.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(usVideosCsvFilePath)

  // UNION
  val trendingVideosUnionDF: DataFrame = CaDF.union(GbDF).union(UsDF)

  // JOIN on Category Names
  val broadcastVideosJoin: DataFrame = trendingVideosUnionDF.join(broadcast(categoryNames.withColumnRenamed("id", "category_id")), "category_id")
  broadcastVideosJoin.createOrReplaceTempView("trendingVideosTmp")

  // Dataframe composed by category and tags columns
  val categoryTags: DataFrame = sqlSession.sql("select category, tags from trendingVideosTmp")
  categoryTags.show()

  // Explode tag list
  val categoryTagsExploded: DataFrame = categoryTags.withColumn("tags", explode(split($"tags", "(\\|)"))).filter("tags != '[none]'").withColumn("tags", lower($"tags"))
  categoryTagsExploded.createOrReplaceTempView("categoryTagsExplodedTmp")
  sqlSession.sqlContext.cacheTable("categoryTagsExplodedTmp")


  // Count the occurrences of the same tags for each category, create a column count_tag to store them
  val tagsCount: DataFrame = sqlSession.sql("select category, tags, COUNT(*) from categoryTagsExplodedTmp group by category, tags").withColumnRenamed("count(1)", "count_tag")
  tagsCount.createOrReplaceTempView("tagsCountTmp")
  sqlSession.sqlContext.cacheTable("tagsCountTmp")


  // Select top 10 rows for each category, ordered by descending tag count
  val top10query: String = """
      | select category, tags, count_tag
      |	from (select *, row_number() over (partition by category order by category, count_tag desc) as top_n_rows
      |				  from tagsCountTmp ) a
      |	where top_n_rows <= 10
      |	order by a.category, a.count_tag desc
    """.stripMargin

  val top10CategoryTags: DataFrame = sqlSession.sql(top10query)
  top10CategoryTags.createOrReplaceTempView("top10CategoryTagsTmp")
  sqlSession.sqlContext.cacheTable("top10CategoryTagsTmp")
  top10CategoryTags.show()


  // Group Dataframe by category, creating a list of tags#count values in column top10_tags
  val groupByCategoryQuery: String = """
      | select category, max(top10_tags)
      | from (select *, collect_list(concat(tags, "#", count_tag)) over (partition by category order by count_tag desc) as top10_tags
      |       from top10CategoryTagsTmp ) a
      | group by a.category
    """.stripMargin

  val groupedByCategory: DataFrame = sqlSession.sql(groupByCategoryQuery)
  groupedByCategory.cache()

  // Field top10_tags converted to string to save the DataFrame as csv and for a better visualization
  val groupedByCategoryString: DataFrame = groupedByCategory.as[(String, Array[String])].map{ case(category, top10_tags) => (category, top10_tags.mkString(",")) }.toDF("category", "top10_tags")
  groupedByCategoryString.show()
  groupedByCategoryString.collect().foreach(println)

  groupedByCategoryString.coalesce(1).write.mode("overwrite").option("header","true").csv("project/spark/output/TopTagsInVideosCategories.csv")

}



