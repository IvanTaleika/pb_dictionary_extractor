name := "pb_dictionary_extractor"

version := "0.1"

val scalaMajorVersion = "2.12"
val scalaMinorVersion = "15"
val sparkVersion      = "3.2.0"
val circeVersion      = "0.14.1"
scalaVersion := s"$scalaMajorVersion.$scalaMinorVersion"

libraryDependencies := Seq(
  "org.apache.spark" %% s"spark-core" % sparkVersion,
  "org.apache.spark" %% s"spark-sql" % sparkVersion,
  "io.delta" %% "delta-core" % "1.1.0",

  "org.xerial" % "sqlite-jdbc" % "3.36.0.3",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.1",
  "org.clapper" %% "grizzled-slf4j" % "1.3.4",

  "com.google.apis" % "google-api-services-sheets" % "v4-rev612-1.25.0",
  "com.google.apis" % "google-api-services-drive" % "v3-rev197-1.25.0",
  "com.google.auth" % "google-auth-library-oauth2-http" % "1.5.3",

  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.1.1" % Test exclude ("junit", "junit"),
  "org.scalatest" %% "scalatest" % "3.0.9" % Test,
  "org.scalamock" %% "scalamock" % "5.1.0" % Test,
  // Scala mocks cannot be used without ScalaTest traits what makes them non-serializable
  "org.mockito" % "mockito-core" % "4.2.0" % Test
)

excludeDependencies ++= Seq(
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)

// In intellij IDEA - modify ScalaTest template, passing `-l org.scalatest.tags.Slow` in `Program arguments`
testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow")
