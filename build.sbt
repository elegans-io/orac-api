import NativePackagerHelper._
import com.typesafe.sbt.packager.docker._

name := "Orac API"

organization := "io.elegans"

scalaVersion := "2.12.4"
crossScalaVersions := Seq("2.12.4")

resolvers ++= Seq("Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  Resolver.bintrayRepo("hseeberger", "maven"))

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers +=
  "Sonatype OSS Releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2"

libraryDependencies ++= {
  val AkkaHttpVersion	= "10.1.0"
  val AkkaVersion	= "2.5.8"
  val BreezeVersion	= "0.13.2"
  val ESClientVersion	= "6.2.3"
  val Log4JVersion	= "2.9.1"
  val LogbackVersion	= "1.2.3"
  val OracEntitiesVersion	= "1.0.2"
  val ParboiledVersion	= "2.1.4"
  val RoundeightsHasherVersion	= "1.2.0"
  val ScalatestVersion	= "3.0.1"
  val ScalazVersion	= "7.2.18"
  val ScoptVersion	= "3.7.0"
  val TikaVersion	= "1.17"
  Seq(
    "ch.qos.logback" %  "logback-classic" % "1.2.3",
    "com.github.scopt" %% "scopt" % ScoptVersion,
    "com.roundeights" %% "hasher" % RoundeightsHasherVersion,
    "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
    "com.typesafe.akka" %% "akka-contrib" % AkkaVersion,
    "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-core" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
    "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % AkkaVersion,
    "com.typesafe.akka" %% "akka-typed" % AkkaVersion,
    "io.elegans" %% "orac-entities" % OracEntitiesVersion,
    "org.apache.logging.log4j" % "log4j-api" % Log4JVersion,
    "org.apache.logging.log4j" % "log4j-core" % Log4JVersion,
    "org.apache.tika" % "tika-app" % TikaVersion,
    "org.apache.tika" % "tika-core" % TikaVersion,
    "org.apache.tika" % "tika-parsers" % TikaVersion,
    //"org.elasticsearch.client" % "elasticsearch-rest-client" % ESClientVersion,
    //"org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % ESClientVersion,
    "org.elasticsearch.client" % "transport" % ESClientVersion,
    "org.elasticsearch" % "elasticsearch" % ESClientVersion,
    "org.parboiled" %% "parboiled" % ParboiledVersion,
    "org.scalanlp" %% "breeze" % BreezeVersion,
    "org.scalanlp" %% "breeze-natives" % BreezeVersion,
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.scalaz" %% "scalaz-core" % ScalazVersion
  )
}

scalacOptions += "-deprecation"
scalacOptions += "-feature"
//scalacOptions += "-Ylog-classpath"
testOptions in Test += Tests.Argument("-oF")

enablePlugins(GitVersioning)
enablePlugins(GitBranchPrompt)
enablePlugins(JavaServerAppPackaging)
enablePlugins(UniversalPlugin)
enablePlugins(DockerPlugin)
enablePlugins(DockerComposePlugin)

git.useGitDescribe := true

//http://www.scala-sbt.org/sbt-native-packager/formats/docker.html
dockerCommands := Seq(
  Cmd("FROM", "java:8"),
  Cmd("RUN", "apt", "update"),
  Cmd("RUN", "apt", "install", "-y", "netcat"),
  Cmd("LABEL", "maintainer=\"Angelo Leto <angelo.leto@elegans.io>\""),
  Cmd("LABEL", "description=\"Docker container for Orac\""),
  Cmd("WORKDIR", "/"),
  Cmd("ADD", "/opt/docker", "/orac"),
  Cmd("VOLUME", "/orac/config"),
  Cmd("VOLUME", "/orac/log")
)

packageName in Docker := packageName.value
version in Docker := version.value
dockerRepository := Some("elegansio")

//dockerImageCreationTask := (publishLocal in Docker).value
composeNoBuild := true
composeFile := "docker-orac/docker-compose.test.yml"

// Assembly settings
mainClass in Compile := Some("io.elegans.orac.Main")

fork in Test := true
javaOptions in Test ++= Seq("-Dconfig.file=./src/test/resources/application.conf")

// do not buffer test output
logBuffered in Test := false

mappings in Universal ++= {
  // copy configuration files to config directory
  directory("scripts") ++
    contentOf("src/main/resources").toMap.mapValues("config/" + _).toSeq
}

scriptClasspath := Seq("../config/") ++ scriptClasspath.value

licenses := Seq(("GPLv2", url("https://www.gnu.org/licenses/old-licenses/gpl-2.0.md")))

