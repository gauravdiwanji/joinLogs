name := "GauravDiwanjiAssetsAdJob"
 
version := "1.0"
 
scalaVersion := "2.10.5"

val sparkVersion = "2.1.1"
val hadoopVersion = "2.7.3"
 
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "com.amazonaws" % "aws-java-sdk" % "1.11.145"
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}