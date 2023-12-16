import LookupTable.{getGeohashUDF, spark}
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

  private val substringUDF = spark.udf.register(
    "substring",
    (geohash: String, prec: Int) ⇒ geohash match {
        case _ if geohash == null ⇒ null
        case _ if geohash.length >= prec ⇒ geohash.substring(0, prec)
        case _ ⇒ geohash
    }
  )

  private val dfWithValidGeohashes =
    df.withColumn("valid_geohash", joinUDF(col("geohash")))
      .withColumn("geohash_2", substringUDF(col("valid_geohash"), lit(2)))
      .withColumn("geohash_3", substringUDF(col("valid_geohash"), lit(3)))
      .withColumn("geohash_4", substringUDF(col("valid_geohash"), lit(4)))

  dfWithValidGeohashes.write
    .partitionBy("geohash_2", "geohash_3", "geohash_4")
    .mode("overwrite")
    .parquet("output")

}
