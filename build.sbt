name := "PoliticianMandatReader"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.pdfbox" % "pdfbox" % "1.8.10" % "compile",
  "org.apache.spark" %% "spark-core" % "1.5.0" % "compile",
  "org.apache.spark" %% "spark-sql" % "1.5.0" % "compile"
)