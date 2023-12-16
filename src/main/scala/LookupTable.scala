import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, col, explode, lit, percent_rank, udf}
import org.locationtech.geomesa.utils.geohash.GeoHash


object LookupTable extends App {

    private val spark = SparkSession.builder()
    .appName("Geospatial Indexing")
    .master("local[*]")
    .getOrCreate()

  private val getGeohash = (lat: Double, lon: Double, prec: Int) => GeoHash(lon, lat, 63).hash.substring(0, prec)
  val getGeohashUDF = spark.udf.register("getGeohash", getGeohash)

  private val df = spark
    .read
    .option("header", "true")
    .csv("input/open_pubs.csv")
    .withColumn("geohash_2", getGeohashUDF(col("latitude"), col("longitude"), lit(2)))
    .withColumn("geohash_3", getGeohashUDF(col("latitude"), col("longitude"), lit(3)))
    .withColumn("geohash_4", getGeohashUDF(col("latitude"), col("longitude"), lit(4)))


  private val threshold: Long = df
    .groupBy("geohash_4")
    .count()
    .withColumn("percent_rank", percent_rank().over(Window.orderBy("count")))
    .filter(col("percent_rank") > 0.995)
    .select("count")
    .first()
    .getLong(0)



  val columns = df.columns.filter(_.startsWith("geohash_"))

  private val geohashCounts = df
    .select(explode(array(columns.map(col): _*)).as("geohash"))
    .groupBy("geohash")
    .count()
    .sort(col("count").desc)


  private val geohashCountsJoined = df
    .join(geohashCounts, col("geohash_3") === col("geohash"))
    .withColumnRenamed("count", "count_3")
    .drop("geohash")
    .join(geohashCounts, col("geohash_2") === col("geohash"))
    .withColumnRenamed("count", "count_2")
    .drop("geohash")


  private val getValidGeoHash = udf((geohash: String, count_3: Long, count_2: Long) => {
    geohash match {
      case _ if count_3 >= threshold => geohash.substring(0, 4)
      case _ if count_2 >= threshold => geohash.substring(0, 3)
      case _  => geohash.substring(0, 2)
    }
  })

  private val dfWithValidGeohash = geohashCountsJoined
    .withColumn("valid_geohash", getValidGeoHash(col("geohash_4"), col("count_3"), col("count_2")))
    .drop("geohash_2", "geohash_3", "geohash_4", "count_2", "count_3")


  dfWithValidGeohash
    .select("valid_geohash")
    .distinct()
    .coalesce(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv("lookup/output.csv")
}
