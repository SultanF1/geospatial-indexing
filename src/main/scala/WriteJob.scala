import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.locationtech.geomesa.utils.geohash.GeoHash

object WriteJob extends App {

  val spark = SparkSession
    .builder()
    .appName("Geospatial Indexing")
    .master("local[*]")
    .getOrCreate()

  val lookupTable: List[String] = spark.read
    .csv("lookup/output.csv")
    .collect()
    .map(_.getString(0))
    .toList

  private val getGeohash = (lat: Double, lon: Double, prec: Int) => GeoHash(lon, lat, 63).hash.substring(0, prec)
  val getGeohashUDF = spark.udf.register("getGeohash", getGeohash)

  val df = spark.read
    .option("header", "true")
    .csv("input/open_pubs.csv")
    .withColumn(
      "geohash",
      getGeohashUDF(col("latitude"), col("longitude"), lit(4))
    )

  val join = (geohash: String) ⇒ {
    geohash match {
      case _ if geohash == null ⇒ null
      case _ if lookupTable.contains(geohash) ⇒ geohash
      case _ if lookupTable.contains(geohash.substring(0, 3)) ⇒
        geohash.substring(0, 3)
      case _ ⇒ geohash.substring(0, 2)
    }
  }

  private val joinUDF = spark.udf.register("join", join)


  private val dfWithValidGeohashes =
    df.withColumn("valid_geohash", joinUDF(col("geohash")))
      .drop("geohash")

  dfWithValidGeohashes
    .withColumn("geohash_1", col("valid_geohash").substr(lit(0), lit(1)))
    .withColumn("geohash_2", col("valid_geohash").substr(lit(0), lit(2)))
    .withColumn("geohash_3", col("valid_geohash").substr(lit(0), lit(3)))
    .withColumn("geohash_4", col("valid_geohash").substr(lit(0), lit(4)))
    .write
    .mode("overwrite")
    .partitionBy("geohash_1", "geohash_2", "geohash_3", "geohash_4")
    .csv("output")


}
