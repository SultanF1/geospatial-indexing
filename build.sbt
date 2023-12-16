ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"
lazy val sparkVersion = "3.3.0"

resolvers ++= Seq(
  "osgeo" at "https://repo.osgeo.org/repository/release",
  "confluent" at "https://packages.confluent.io/maven/"
)


lazy val root = (project in file("."))
  .settings(
    name := "geospatial-indexing",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.scala-lang.modules", "scala-xml_2.12") exclude("org.scala-lang.modules", "scala-parser-combinators_2.12"),
      "org.apache.spark" %% "spark-sql" % sparkVersion exclude("org.scala-lang.modules", "scala-xml_2.12") exclude("org.scala-lang.modules", "scala-parser-combinators_2.12"),
      "org.locationtech.geomesa" %% "geomesa-spark-sql" % "4.0.4",
      "org.locationtech.geomesa" %% "geomesa-utils" % "4.0.4"
    )
  )


