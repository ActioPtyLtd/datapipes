
name := "root"

version := "1.0"

scalaVersion := "2.11.1"

lazy val common = project

lazy val model = project
  .dependsOn(common)

lazy val datasources = project
  .settings(libraryDependencies ++= Seq("org.apache.commons" % "commons-csv" % "1.4"))
  .dependsOn(model)


lazy val application = project
  .dependsOn(model, datasources, common)

lazy val root =
  project.in( file(".") )
    .aggregate(application, model, datasources, common)
