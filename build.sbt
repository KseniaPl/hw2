name := "otus_1"

version := "0.1"

scalaVersion := "2.13"


// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.2"


libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.2.2"

// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-slf4j-impl
libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.1" % Test


