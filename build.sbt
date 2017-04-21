
name := "pickleroot"

version := "1.0"

scalaVersion := "2.11.1"

lazy val common = project

lazy val model = project
  .dependsOn(common)

lazy val datasources = project
  .dependsOn(model)


lazy val root =
  project.in( file(".") )
    .aggregate(model, datasources, common)
