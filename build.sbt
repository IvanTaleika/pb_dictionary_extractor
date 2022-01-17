name := "pb_dictionary_extractor"

version := "0.1"

val scalaMajorVersion = "2.12"
val sparkVersion      = "3.2.0"

scalaVersion := s"$scalaMajorVersion.15"

libraryDependencies := Seq(
  "org.apache.spark" %% s"spark-core" % sparkVersion,
  "org.apache.spark" %% s"spark-sql" % sparkVersion,
  "io.delta" %% "delta-core" % "1.1.0",
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.1.1" % Test exclude ("junit", "junit"),
  "org.scalatest" %% "scalatest" % "3.0.9" % Test,
  "org.scalamock" %% "scalamock" % "5.1.0" % Test,
  // Scala mocks cannot be used without ScalaTest traits what makes them non-serializable
  "org.mockito" % "mockito-core" % "4.2.0" % Test
)
