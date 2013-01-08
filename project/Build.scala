import sbt._
import Keys._
import PlayProject._

object ApplicationBuild extends Build {

    val appName         = "monitrix"
    val appVersion      = "1.0-SNAPSHOT"

    val appDependencies = Seq(
      "org.mongodb" % "mongo-java-driver" % "2.9.1",
      "commons-httpclient" % "commons-httpclient" % "3.1",
      "com.google.guava" % "guava" % "13.0.1"
    )

    val main = PlayProject(appName, appVersion, appDependencies, mainLang = JAVA).settings(
      // Add your own project settings here      
    )

}
