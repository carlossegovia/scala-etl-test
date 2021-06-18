name := "applaudoTest"

version := "0.1"

scalaVersion := "2.12.7"

// name of the package
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  "ApplaudoETL" + "." + artifact.extension
}

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.6"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure
libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "2.7.3"

// https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "9.2.1.jre8"