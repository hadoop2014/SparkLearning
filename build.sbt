name := "SparkLearning"

version := "1.0"

scalaVersion := "2.10.4"
//scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "org.apache.hive"% "hive-jdbc" % "0.13.1" ,
  //"io.spray" % "spray-can" % "1.3.1",
  //"io.spray" % "spray-routing" % "1.3.1",
  //"io.spray" % "spray-testkit" % "1.3.1" % "test",
  //"io.spray" %% "spray-json" % "1.2.6",
  //"com.typesafe.akka" %% "akka-actor" % "2.3.2",
  //"com.typesafe.akka" %% "akka-testkit" % "2.3.2" % "test",
  //"org.apache.hadoop" % "hadoop-common" % "2.2.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  //"org.apache.hadoop" % "hadoop-client" % "2.2.0" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.scalatest" %% "scalatest" % "2.2.0" ,
  "org.apache.spark" %% "spark-core" % "1.4.0",// exclude("hadoop-common","hadoop-client"),
  "org.apache.spark" %% "spark-sql" % "1.4.0",
  "org.apache.spark" %% "spark-hive" % "1.4.0",
  "org.apache.spark" %% "spark-mllib" % "1.4.0",
  "org.apache.spark" %% "spark-streaming" % "1.4.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.4.0" ,//exclude("org.apache.spark", "spark-streaming_2.10")
  //"org.apache.hadoop" %% "hadoop-client" % "2.4.0"
  "javax.servlet" % "javax.servlet-api" % "3.0.1",
  "org.eclipse.jetty"%"jetty-servlet"%"8.1.14.v20131031",
  "org.eclipse.jetty"%"jetty-http"%"8.1.14.v20131031",
  "org.eclipse.jetty"%"jetty-server"%"8.1.14.v20131031",
  "org.eclipse.jetty"%"jetty-util"%"8.1.14.v20131031",
  "org.eclipse.jetty"%"jetty-continuation"%"8.1.14.v20131031",
  "org.eclipse.jetty"%"jetty-security"%"8.1.14.v20131031",
  "org.eclipse.jetty"%"jetty-plus"%"8.1.14.v20131031",
  "org.apache.kafka"%%"kafka"%"0.8.2.1",
  "net.sf.json-lib"%"json-lib"%"2.4" from "http://gradle.artifactoryonline.com/gradle/libs/net/sf/json-lib/json-lib/2.4/json-lib-2.4-jdk15.jar",
  "com.databricks"%%"spark-csv"%"1.0.3"
  //"org.eclipse.jetty.orbit"%"javax.servlet"%"3.0.0.v201112011016"
  //"org.mortbay.jetty"%%"servlet-api"%"3.0.20100224"
  //"net.sf.jason"%"jason"%"1.4.1"
)

resolvers += "Maven" at "http://repo1.maven.org/maven2"