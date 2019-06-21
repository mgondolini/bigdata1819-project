// Local init
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val sc = new SparkContext(new SparkConf().setAppName("Youtube Trending Videos"))

// Init SQLContext
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// File path
val categoryJsonFilePath: String = "project/dataset/US_category_id_flat.json"
val caVideosCsvFilePath: String = "project/dataset/cleaned/CAvideos.csv"
val gbVideosCsvFilePath: String = "project/dataset/cleaned/GBvideos.csv"
val usVideosCsvFilePath: String = "project/dataset/cleaned/USvideos.csv"

//Import data from JSON file to DataFrame
val categoryNames = sqlContext.read.json(categoryJsonFilePath)

// SPARK SHELL 2
val CaDF = sqlContext.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(caVideosCsvFilePath)

val GbDF = sqlContext.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(gbVideosCsvFilePath)

val UsDF = sqlContext.read.format("csv").option("delimiter", ",").option("header", "true").option("mode", "DROPMALFORMED").load(usVideosCsvFilePath)

// UNION
val trendingVideosUnionDF = CaDF.union(GbDF).union(UsDF)

// JOIN
val broadcastVideosJoin = trendingVideosUnionDF.join(broadcast(categoryNames.withColumnRenamed("id", "category_id")), "category_id")
broadcastVideosJoin.registerTempTable("trendingVideosTmp")

// Dataframe composed by category and tags columns
val categoryTags = sqlContext.sql("select category, tags from trendingVideosTmp")
categoryTags.show()

// Explode tag list
val categoryTagsExploded = categoryTags.withColumn("tags", explode(split($"tags", "(\\|)"))).filter("tags != '[None]'")
categoryTagsExploded.registerTempTable("categoryTagsExplodedTmp")


// Count the same tags for each category, create a column count_tag
val tagsCount = sqlContext.sql("select category, tags, COUNT(*) from categoryTagsExplodedTmp group by category, tags").withColumnRenamed("count(1)", "count_tag")
tagsCount.show()
tagsCount.registerTempTable("tagsCountTmp")

// Select top 10 rows for each category
val top10query = """ select category, tags, count_tag
				from (select *, row_number() over (partition by category order by category, count_tag desc) as top_n_rows
				      from tagsCountTmp
				     ) a
				where top_n_rows <= 10
				order by a.category, a.count_tag desc """

val top10CategoryTags =  sqlContext.sql(top10query)
top10CategoryTags.show()

import org.apache.spark.sql.functions.struct

// Collapse tags and count_tags column values in one column "tags:count"
val collapsedTagsCount = top10CategoryTags.withColumn("tags:count", struct(tagsCount("tags"), tagsCount("count_tag"))).drop("tags").drop("count_tag")
collapsedTagsCount.show()

// Group Dataframe by category, creating a list of tags:count in column top10_tags
val groupedByCategory = collapsedTagsCount.groupBy("category").agg(collect_list(col("tags:count")) as "top10_tags")
groupedByCategory.show()

// Stampa tutto
groupedByCategory.collect().foreach(println)

//TODO togliere [none] dai tag