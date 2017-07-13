name := "spark-engine"

version := "1.0"

scalaVersion := "2.10.5"

//scalaVersion := "2.11.6"
//scalaVersion := "2.10.5"
//val sparkVersion = "1.4.1"
//val sparkCassandraVersion = "1.4.0"
//val sparkVersion = "1.6.0"
//val sparkCassandraVersion = "1.5.0-RC1"

val sparkVersion = "1.5.0"
val sparkCassandraVersion = "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraVersion % "provided"

resolvers += Resolver.sonatypeRepo("public")

logLevel := Level.Error

// We do this so that Spark Dependencies will not be bundled with our fat jar
// but will still be included on the classpath when we do a sbt/run
//run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

assemblySettings
