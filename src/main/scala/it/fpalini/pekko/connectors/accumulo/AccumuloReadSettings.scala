package it.fpalini.pekko.connectors.accumulo

import org.apache.accumulo.core.client.{ AccumuloClient, BatchScanner }
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.security.Authorizations
import org.apache.pekko
import org.apache.pekko.util.ccompat.JavaConverters.*

import java.util.{ function, Properties }
import scala.collection.immutable
import scala.jdk.FunctionConverters.enrichAsScalaFromFunction

final class AccumuloReadSettings private (val accumuloClient: AccumuloClient,
    val table: String,
    val authorizations: Option[Authorizations],
    val numQueryThreads: Option[Int],
    val batchScannerCustomizer: BatchScanner => Unit
) {

  def withAccumuloClient(accumuloClient: AccumuloClient): AccumuloReadSettings =
    copy(accumuloClient = accumuloClient)

  def withTable(table: String): AccumuloReadSettings =
    copy(table = table)

  def withAuthorizations(authorizations: Authorizations): AccumuloReadSettings =
    copy(authorizations = Option(authorizations))

  def withNumQueryThreads(numQueryThreads: Int): AccumuloReadSettings =
    copy(numQueryThreads = Option(numQueryThreads))

  /**
   * Java Api
   */
  def withNumQueryThreads(numQueryThreads: Integer): AccumuloReadSettings =
    copy(numQueryThreads = Option(numQueryThreads))

  def withBatchScannerCustomizer(batchScannerCustomizer: BatchScanner => Unit): AccumuloReadSettings =
    copy(batchScannerCustomizer = batchScannerCustomizer)

  /**
   * Java Api
   */
  def withBatchScannerCustomizer(batchScannerCustomizer: function.Function[BatchScanner, Void]): AccumuloReadSettings =
    copy(batchScannerCustomizer = batchScannerCustomizer.asScala(_))

  override def toString: String =
    "AccumuloReadSettings(" +
    s"accumuloClientProperties=${accumuloClient.properties()}," +
    s"table=$table," +
    s"authorizations=$authorizations," +
    s"numQueryThreads=$numQueryThreads" +
    ")"

  private def copy(
      accumuloClient: AccumuloClient = accumuloClient,
      table: String = table,
      authorizations: Option[Authorizations] = authorizations,
      numQueryThreads: Option[Int] = numQueryThreads,
      batchScannerCustomizer: BatchScanner => Unit = batchScannerCustomizer
  ) =
    new AccumuloReadSettings(accumuloClient, table, authorizations, numQueryThreads, batchScannerCustomizer)

}

object AccumuloReadSettings {

  def apply(
      accumuloClient: AccumuloClient,
      table: String,
      authorizations: Option[Authorizations] = None,
      numQueryThreads: Option[Int] = None,
      batchScannerCustomizer: BatchScanner => Unit = _ => ()
  ) =
    new AccumuloReadSettings(accumuloClient, table, authorizations, numQueryThreads, batchScannerCustomizer)

  /**
   * Java Api
   */
  def create(accumuloClient: AccumuloClient,
      table: String,
      authorizations: Authorizations,
      numQueryThreads: java.lang.Integer,
      batchScannerCustomizer: function.Function[BatchScanner, Void]
  ): AccumuloReadSettings =
    new AccumuloReadSettings(accumuloClient, table, Option(authorizations), Option(numQueryThreads),
      batchScannerCustomizer.asScala(_))

  def create(accumuloClient: AccumuloClient,
      table: String,
      authorizations: Authorizations,
      numQueryThreads: java.lang.Integer
  ): AccumuloReadSettings =
    AccumuloReadSettings(accumuloClient, table, Option(authorizations), Option(numQueryThreads))

  def create(accumuloClient: AccumuloClient,
      table: String,
      authorizations: Authorizations
  ): AccumuloReadSettings =
    AccumuloReadSettings(accumuloClient, table, Option(authorizations))

  def create(accumuloClient: AccumuloClient,
      table: String
  ): AccumuloReadSettings =
    AccumuloReadSettings(accumuloClient, table)
}
