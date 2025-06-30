import it.fpalini.pekko.connectors.accumulo.{AccumuloReadSettings, AccumuloWriteSettings}
import it.fpalini.pekko.connectors.accumulo.scaladsl.AccumuloStage
import org.apache.accumulo.core.data.{Mutation, Range as AccumuloRange}
import org.apache.accumulo.core.security.Authorizations
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.geomesa.testcontainers.AccumuloContainer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.shouldBe
import org.testcontainers.utility.DockerImageName

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class PekkoAccumuloTest extends AnyFlatSpec with BeforeAndAfterAll {

  private val image = DockerImageName.parse("ghcr.io/geomesa/accumulo-uno:2.1.3")
  private val accumulo = AccumuloContainer(image)
  private lazy val accumuloClient = accumulo.client()

  implicit private val actorSystem: ActorSystem = ActorSystem("AccumuloSystem")

  val writeSettingsVisibility1: AccumuloWriteSettings[String] = AccumuloWriteSettings(
    accumulo.client(), 
    "my_table",
    (s: String) => Seq(new Mutation("row1").at().family("fam").qualifier(s).visibility("a").put("value1"))
  )

  val writeSettingsVisibility2: AccumuloWriteSettings[String] = AccumuloWriteSettings(
    accumulo.client(),
    "my_table",
    (s: String) => Seq(new Mutation("row2").at().family("fam").qualifier(s).visibility("b").put("value2"))
  )

  val writeSettingsVisibility3: AccumuloWriteSettings[String] = AccumuloWriteSettings(
    accumulo.client(),
    "my_table",
    (s: String) => Seq(new Mutation("row3").at().family("fam").qualifier(s).visibility("c").put("value3"))
  )

  override protected def beforeAll(): Unit = {
    accumulo.start()
    accumuloClient.securityOperations().changeUserAuthorizations(accumulo.getUsername, Authorizations("a", "b"))
    
    Source(Seq("x1", "x2")).runWith(AccumuloStage.sink(writeSettingsVisibility1))
    Source(Seq("x3", "x4")).runWith(AccumuloStage.sink(writeSettingsVisibility2))
    Source(Seq("x5", "x6")).runWith(AccumuloStage.sink(writeSettingsVisibility3))
  }

  override protected def afterAll(): Unit =
    accumulo.stop()

  it should "read from Accumulo with visibility 'a'" in {
    val accumuloSettings = AccumuloReadSettings(accumulo.client(), "my_table", Option(Authorizations("a")))
    val maybeResults = AccumuloStage.source(Seq(AccumuloRange.prefix("row")), accumuloSettings).runWith(Sink.seq)

    val results = Await.result(maybeResults, 10.seconds)

    results.size shouldBe 2
  }

  it should "read from Accumulo with visibility 'b'" in {
    val accumuloSettings = AccumuloReadSettings(accumulo.client(), "my_table", Option(Authorizations("b")))
    val maybeResults = AccumuloStage.source(Seq(AccumuloRange.prefix("row")), accumuloSettings).runWith(Sink.seq)

    val results = Await.result(maybeResults, 10.seconds)

    results.size shouldBe 2
  }

  it should "NOT read from Accumulo with visibility 'c'" in {
    val accumuloSettings = AccumuloReadSettings(accumulo.client(), "my_table")
    val maybeResults = AccumuloStage.source(Seq(AccumuloRange.prefix("row")), accumuloSettings).runWith(Sink.seq)

    val results = Await.result(maybeResults, 10.seconds)

    results.size shouldBe 4
  }
}
