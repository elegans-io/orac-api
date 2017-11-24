import NativePackagerHelper._
import com.typesafe.sbt.packager.docker._

name := "Orac APIs"

organization := "io.elegans"

scalaVersion := "2.12.4"

resolvers ++= Seq("Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  Resolver.bintrayRepo("hseeberger", "maven"))

libraryDependencies ++= {
  val AkkaVersion       = "2.5.6"
  val AkkaHttpVersion   = "10.0.10"
  val ESClientVersion   = "6.0.0"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
    "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
    "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % AkkaVersion,
    "com.typesafe.akka" %% "akka-typed" % AkkaVersion,
    "com.typesafe.akka" %% "akka-contrib" % AkkaVersion,
    "com.typesafe.akka" %% "akka-http-core" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,
    "com.typesafe.akka" %% "akka-http-testkit" % AkkaHttpVersion,
    "ch.qos.logback"    %  "logback-classic" % "1.2.3",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.elasticsearch" % "elasticsearch" % ESClientVersion,
    "org.elasticsearch.client" % "transport" % ESClientVersion,
    //"org.elasticsearch.client" % "elasticsearch-rest-client" % ESClientVersion,
    //"org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % ESClientVersion,
    "org.scalanlp" %% "breeze" % "0.13",
    "org.scalanlp" %% "breeze-natives" % "0.13",
    "org.apache.logging.log4j" % "log4j-api" % "2.9.1",
    "org.apache.logging.log4j" % "log4j-core" % "2.9.1",
    "org.apache.tika" % "tika-core" % "1.16",
    "org.apache.tika" % "tika-parsers" % "1.16",
    "org.apache.tika" % "tika-app" % "1.16",
    "com.github.scopt" %% "scopt" % "3.6.0",
    "com.roundeights" %% "hasher" % "1.2.0",
    "org.parboiled" %% "parboiled" % "2.1.4"
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

