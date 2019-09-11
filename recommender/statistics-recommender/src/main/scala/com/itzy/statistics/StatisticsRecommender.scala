package com.itzy.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Author: ZY
  * @Date: 2019/9/11 18:31
  * @Version 1.0
  * @Description:
  */
/**
  * Rating数据集
  * 4867        用户ID
  * 457976      商品ID
  * 5.0         评分
  * 1395676800  时间戳
  */
case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

/**
  * MongoDB连接配置
  *
  * @param uri MongoDB的连接uri
  * @param db  要操作的db
  */
case class MongoConfig(uri: String, db: String)

object StatisticsRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //加入隐式转换
    import spark.implicits._

    //数据加载进来
    val ratingDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    //创建一张名叫ratings的表
    ratingDF.createOrReplaceTempView("ratings")

    //TODO: 不同的统计推荐结果
    spark.stop()
  }

}
