name := "pb_dictionary_extractor"

version := "0.1"

val scalaMajorVersion = "2.12"
val sparkVersion = "3.1.2"

scalaVersion := s"$scalaMajorVersion.6"

libraryDependencies := Seq(
  "org.apache.spark" %% s"spark-core" % sparkVersion,
  "org.apache.spark" %% s"spark-sql" % sparkVersion,
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3"
)