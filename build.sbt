// Versión de Scala
scalaVersion := "3.1.1"

// Nombre del proyecto y organización
ThisBuild / name := "spark-scala3-wherhouses"
ThisBuild / version := "0.1.0"

// Dependencia de Spark con compatibilidad para Scala 2.13 (porque Spark aún no soporta directamente Scala 3)
libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "3.2.0" % "provided").cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-avro" % "3.2.0").cross(CrossVersion.for3Use2_13)
)

// Permitir que sbt run incluya también dependencias 'provided' como Spark
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// Assembly settings
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Don't include Spark dependencies in the assembled JAR
assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp filter { _.data.getName.startsWith("spark-") }
}

// Specify the name of the assembled JAR
assembly / assemblyJarName := s"${name.value}-${version.value}.jar"
