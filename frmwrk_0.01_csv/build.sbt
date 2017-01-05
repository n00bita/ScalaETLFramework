name := "MyApp"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2"
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.2"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.3.4" 
resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/" 
