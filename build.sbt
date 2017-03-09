name := "recommended-content-service"

version := "1.0"

scalaVersion := "2.11.7"

// Spark dependencies
libraryDependencies ++= {
  val sparkVersion = "1.6.0"

  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion
  )
}

// Cassandra dependencies
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0"

// Akka dependencies
libraryDependencies ++= {
  val akkaVersion = "2.4.2"

  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-http-experimental" % akkaVersion,
    "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaVersion
  )
}

