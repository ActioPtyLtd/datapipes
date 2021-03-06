
name := "root"

scalaVersion := "2.11.1"

resolvers += Resolver.bintrayRepo("hseeberger", "maven")

lazy val common = project
  .settings(
    assemblyJarName in assembly := "commons.jar")
  .settings(libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.1")

lazy val pipescript = project
  .settings(libraryDependencies ++= Seq("com.typesafe" % "config" % "1.3.1",
      "org.scalameta" %% "scalameta" % "1.0.0"))
  .dependsOn(common)

lazy val datasources = project
  .settings(libraryDependencies ++= Seq(
    "org.apache.commons" % "commons-csv" % "1.4",
    "commons-net" % "commons-net" % "3.5",
    "commons-io" % "commons-io" % "2.4",
    //"org.scalameta" %% "scalameta" % "1.0.0",
    //"org.scala-lang.modules" %% "scala-async" % "0.9.6",
    "ch.qos.logback" % "logback-classic" % "1.1.7",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    "com.microsoft.sqlserver" % "mssql-jdbc" % "6.2.2.jre8",
    "org.postgresql" % "postgresql" % "42.1.1",
    "org.apache.httpcomponents" % "httpclient" % "4.5.2",
    "me.chrons" %% "boopickle" % "1.2.5",
    "com.amazonaws" % "aws-java-sdk-s3" % "1.11.184",
      "org.apache.commons" % "commons-compress" % "1.15",
    "com.github.oshi" % "oshi-core" % "3.4.4",
    "com.github.albfernandez" % "javadbf" % "1.2.1"))
  .dependsOn(pipescript)

lazy val pipeline = project
  .settings(libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "0.9.6")
  .dependsOn(common)
  .dependsOn(task)
  .dependsOn(pipescript)

lazy val task = project
  .settings(libraryDependencies ++= Seq(
    "org.scalameta" %% "scalameta" % "1.0.0",
    "ch.qos.logback" % "logback-classic" % "1.1.7",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    "me.chrons" %% "boopickle" % "1.2.5",
      "commons-lang" % "commons-lang" % "2.6",
      "commons-codec" % "commons-codec" % "1.10"))
  .dependsOn(common, datasources)

lazy val application = project
  .settings(libraryDependencies ++= Seq("commons-cli" % "commons-cli" % "1.3.1",
    "de.heikoseeberger" % "akka-http-json4s_2.11" % "1.16.1",
    "com.typesafe.akka" %% "akka-http" % "10.0.11",
    "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.1",
    "org.quartz-scheduler" % "quartz" % "2.3.0"
  ))
  .settings(mainClass in assembly := Some("actio.datapipes.application.AppConsole"))
  .dependsOn(pipescript, datasources, common, task, pipeline)

lazy val root =
  (project in file("."))
    .aggregate(application, pipescript, datasources, common, pipeline, task)

lazy val test = project
  .settings(libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test")
  .dependsOn(pipescript, datasources, common, task, pipeline)