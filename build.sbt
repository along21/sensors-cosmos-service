name := """sensors-cosmos"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  jdbc,
  ehcache,
  ws,
  filters,
  guice,
  "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test,
  "org.mockito" % "mockito-core" % "2.13.0",
  "com.typesafe.slick" %% "slick" % "3.2.3",
  "com.h2database" % "h2" % "1.4.190",
  "com.github.tototoshi" %% "scala-csv" % "1.3.5",
  "com.github.tototoshi" %% "play-json-naming" % "1.2.0",
  "org.logback-extensions" % "logback-ext-loggly" % "0.1.4",
  "com.mindscapehq" % "raygun4java-play2" % "2.1.1",
  "com.microsoft.azure" % "azure-documentdb" % "2.4.4",
  "com.microsoft.azure" % "documentdb-bulkexecutor" % "2.6.0",
  "com.microsoft.azure" % "azure-cosmosdb" % "2.6.3",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.4.1.jre8",
  "com.iofficecorp" %% "play-application-info-client" % "9.6.0"
  exclude("com.typesafe.slick", "slick_2.12"),
  "com.iofficecorp" %% "play-api-pagination" % "1.0.0"
  exclude("com.typesafe.slick", "slick_2.12"),
  "com.iofficecorp" %% "play-v2api-client" % "9.6.0"
  exclude("com.typesafe.slick", "slick_2.12"),
  "com.iofficecorp" %% "play-ioffice-controller" % "9.6.0"
  exclude("com.typesafe.slick", "slick_2.12"),
  "com.iofficecorp" %% "metamorphosis" % "0.5.2"
)


resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/"
resolvers += "artifactory" at "http://bartifactory.corp.iofficecorp.com:8081/artifactory/libs-release-local/"
resolvers += Resolver.url("Typesafe Ivy releases", url("https://repo.typesafe.com/typesafe/ivy-releases"))(Resolver.ivyStylePatterns)
resolvers += "confluent" at "http://packages.confluent.io/maven/"

fork := true
javaOptions += "-Duser.timezone=UTC"
fork in test := true
javaOptions in Test += "-Dconfig.file=conf/application.test.conf"
javaOptions in Test += "-Duser.timezone=UTC"

import play.sbt.routes.RoutesKeys
RoutesKeys.routesImport += "controllers.QueryStringDateBindables._"
RoutesKeys.routesImport += "controllers.QueryStringDateBindables"
