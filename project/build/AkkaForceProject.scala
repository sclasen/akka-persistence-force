import sbt._


class AkkaForceProject(info: ProjectInfo) extends DefaultProject(info) {

  val ForceRepo = MavenRepository("Force Repo", "http://repo.t.salesforce.com/archiva/repository/releases")
  //val JBossRepo = MavenRepository("JBoss Repo", "http://repository.jboss.org/nexus/content/groups/public/")
  val GuiceyFruitRepo = MavenRepository("GuiceyFruit Repo", "http://guiceyfruit.googlecode.com/svn/repo/releases/")
  val EmbeddedRepo = MavenRepository("Embedded Repo", (info.projectPath / "embedded-repo").asURL.toString)
  val LocalMavenRepo       = MavenRepository("Local Maven Repo", (Path.userHome / ".m2" / "repository").asURL.toString)


  lazy val AKKA_VERSION = "1.1-SNAPSHOT"
  lazy val FORCE_API_VERSION = "20.0.0"
  lazy val FORCE_SDK_VERSION = "20.0.0"
  lazy val SCALATEST_VERSION = "1.2"
  lazy val CODEC_VERSION = "1.4"
  lazy val FORCE_API_GROUP = "com.force.api"
  lazy val FORCE_SDK_GROUP = "com.force.sdk"
  lazy val COMPILE = "compile"
  lazy val TEST = "test"

  val ForceApiModuleConfiguration = ModuleConfiguration(FORCE_API_GROUP, ForceRepo)
  val ForceSdkModuleConfiguration = ModuleConfiguration(FORCE_SDK_GROUP, ForceRepo)
  //val nettyModuleConfig = ModuleConfiguration("org.jboss.netty", JBossRepo)
  val guiceyFruitModuleConfig = ModuleConfiguration("org.guiceyfruit", GuiceyFruitRepo)


  val persistence_common = "se.scalablesolutions.akka" % "akka-persistence-common" % AKKA_VERSION % COMPILE
  val persistence_common_test = "se.scalablesolutions.akka" % "akka-persistence-common" % (AKKA_VERSION + "-test") % TEST
  val force_metadata = FORCE_API_GROUP % "force-metadata-api" % FORCE_API_VERSION % COMPILE
  val force_partner = FORCE_API_GROUP % "force-partner-api" % FORCE_API_VERSION % COMPILE
  val force_wsc = FORCE_API_GROUP % "force-wsc" % FORCE_API_VERSION % COMPILE
  val force_connector = FORCE_SDK_GROUP % "force-connector" % FORCE_SDK_VERSION % COMPILE
  val commons_codec = "commons-codec" % "commons-codec" % CODEC_VERSION % "compile"
  val junit = "junit" % "junit" % "4.5" % "test"
  val scalatest = "org.scalatest" % "scalatest" % SCALATEST_VERSION % TEST


}