import sbt._

lazy val commonSettings = Seq(
  organization := "io.mindfulmachines",
  version := "0.9-SNAPSHOT",
  scalaVersion := "2.11.8",
  description := "Dependency and data pipeline management framework for Spark and Scala."
)

crossScalaVersions := Seq("2.10.6", "2.11.8")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2" % "provided" exclude("org.apache.hadoop", "hadoop-client")
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.2" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2" % "provided"  excludeAll ExclusionRule(organization = "javax.servlet")
libraryDependencies += "org.eclipse.jetty" % "jetty-webapp" % "9.3.9.v20160517" % "provided"
libraryDependencies += "net.java.dev.jets3t" % "jets3t" % "0.9.4" % "provided"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.12.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.2" % "provided"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "Typesafe Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"

pomIncludeRepository := { _ => false }

publishMavenStyle := true

fork in Test := true

javaOptions in Test ++= Seq("-Xmx1G")

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

sonatypeProfileName := "io.mindfulmachines"

pomExtra :=
  <url>https://github.com/mindfulmachines/peapod</url>
  <licenses>
    <license>
      <name>MIT License</name>
      <url>http://www.opensource.org/licenses/mit-license.php</url>
    </license>
  </licenses>
  <developers>
    <developer>
      <name>Marcin Mejran</name>
      <email>marcin@mindfulmachines.io</email>
      <organization>Mindful Machines</organization>
      <organizationUrl>http://www.mindfulmachines.io</organizationUrl>
    </developer>
    <developer>
      <name>Zoe Afshar</name>
      <email>zoe@mindfulmachines.io</email>
      <organization>Mindful Machines</organization>
      <organizationUrl>http://www.mindfulmachines.io</organizationUrl>
    </developer>
  </developers>
  <scm>
    <connection>scm:git:git@github.com:mindfulmachines/peapod.git</connection>
    <developerConnection>scm:git:git@github.com:mindfulmachines/peapod.git</developerConnection>
    <url>git@github.com:mindfulmachines/peapod.git</url>
  </scm>

lazy val root = (project in file(".")).
  //configs(IntegrationTest).
  //settings(Defaults.itSettings: _*).
  settings(commonSettings: _*).
  settings(
    name := "Peapod"
  )