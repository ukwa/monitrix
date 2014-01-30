import sbt._
import Keys._
import play.Project._
import sbtbuildinfo.Plugin._

object ApplicationBuild extends Build {

    val appName         = "monitrix"
    val appVersion      = "0.2.0"

    val appDependencies = Seq(
      javaCore,
      cache,
      "org.mongodb" % "mongo-java-driver" % "2.9.1",
      "com.datastax.cassandra" % "cassandra-driver-core" % "1.0.4",
      "commons-httpclient" % "commons-httpclient" % "3.1",
      "org.apache.httpcomponents" % "httpclient" % "4.2.3",
      "commons-io" % "commons-io" % "2.4",
      "com.google.guava" % "guava" % "13.0.1",
      "net.sf.jasperreports" % "jasperreports" % "4.1.2",
      "com.typesafe" %% "play-plugins-mailer" % "2.1.0"
    )

    // We're using the SBT BuildInfo plugin to make library version numbers available to the view
    // https://github.com/sbt/sbt-buildinfo


    val main = play.Project(appName, appVersion, appDependencies)
      .settings( buildInfoSettings: _* )
      .settings(
      sourceGenerators in Compile <+= buildInfo,
      buildInfoKeys := Seq[BuildInfoKey](
          name,
          version,
          scalaVersion,
          sbtVersion,
          resolvers,
          libraryDependencies in Compile),
      buildInfoPackage := "uk.bl.monitrix"
    )

}
