organization := "com.esri"

name := "FileGDB"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.1"

publishMavenStyle := true

resolvers += Resolver.mavenLocal

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

libraryDependencies ++= Seq(
  "com.esri.geometry" % "esri-geometry-api" % "1.2.1"
    exclude("org.codehaus.jackson", "jackson-core-asl")
    exclude("org.json", "json"),
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "2.2.3" % "test"
)
