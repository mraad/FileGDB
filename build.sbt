organization := "com.esri"

name := "filegdb"

version := "0.20"

isSnapshot := true

scalaVersion := "2.11.8"
val sparkVersion = "2.3.4"

publishMavenStyle := true

resolvers += Resolver.mavenLocal

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.7" % "test"
)
