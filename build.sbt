organization := "com.esri"

name := "FileGDB"

version := "0.2"

isSnapshot := true

scalaVersion := "2.11.8"

val sparkVersion = "2.2.1"

publishMavenStyle := true

resolvers += Resolver.mavenLocal

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "2.2.3" % "test"
)
