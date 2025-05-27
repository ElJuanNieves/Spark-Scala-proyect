// Versión de Scala
scalaVersion := "3.1.1"

// Nombre del proyecto y organización
ThisBuild / name := "spark-scala3-wherhouses"
ThisBuild / version := "0.1.0"

// Dependencia de Spark con compatibilidad para Scala 2.13 (porque Spark aún no soporta directamente Scala 3)
libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "3.2.0" % "provided").cross(CrossVersion.for3Use2_13)
)

// Permitir que sbt run incluya también dependencias 'provided' como Spark
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated
