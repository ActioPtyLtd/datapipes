
name := "root"

version := "1.0"

scalaVersion := "2.11.1"

lazy val common = project
  .settings(libraryDependencies ++= Seq("org.json4s" %% "json4s-native" % "3.5.1"))

lazy val pipescript = project
  .dependsOn(common)

lazy val datasources = project
  .settings(libraryDependencies ++= Seq("org.apache.commons" % "commons-csv" % "1.4"))
  .settings(libraryDependencies ++= Seq("org.scala-lang.modules" %% "scala-async" % "0.9.6"))
  .dependsOn(pipescript)

lazy val pipeline = project
  .settings(libraryDependencies ++= Seq("org.scala-lang.modules" %% "scala-async" % "0.9.6"))
  .dependsOn(common)
  .dependsOn(task)
  .dependsOn(pipescript)

lazy val task = project
  .settings(libraryDependencies ++= Seq("org.scalameta" %% "scalameta" % "1.0.0"))
  .settings(libraryDependencies ++= Seq("ch.qos.logback" % "logback-classic" % "1.1.7"))
  .settings(libraryDependencies ++= Seq("com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"))
  .dependsOn(common, datasources)

lazy val application = project
  .dependsOn(pipescript, datasources, common, task, pipeline)

lazy val root =
  project.in( file(".") )
    .aggregate(application, pipescript, datasources, common, pipeline, task)

lazy val test = project
  .settings(libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test")
  .dependsOn(pipescript, datasources, common, task, pipeline)