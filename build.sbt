name := "scala-evaluation"

organization := "com.kalibri.eval"

scalaVersion := "2.11.12"

val sparkVer = "2.4.0"

libraryDependencies ++=
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-sql" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-repl" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-network-shuffle" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-hive" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-catalyst" % sparkVer % "provided" withSources(),
    "org.apache.spark" %% "spark-launcher" % sparkVer % "provided" withSources()
  )