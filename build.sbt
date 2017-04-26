
name := "root"

version := "1.0"

scalaVersion := "2.11.1"

lazy val common = project

lazy val model = project
  .settings(libraryDependencies ++= Seq("org.json4s" %% "json4s-native" % "3.5.1"))
  .dependsOn(common)

lazy val datasources = project
  .settings(libraryDependencies ++= Seq("org.apache.commons" % "commons-csv" % "1.4"))
  .settings(libraryDependencies ++= Seq("org.scala-lang.modules" %% "scala-async" % "0.9.6"))
  .dependsOn(model)


lazy val application = project
  .dependsOn(model, datasources, common)

lazy val root =
  project.in( file(".") )
    .aggregate(application, model, datasources, common)
