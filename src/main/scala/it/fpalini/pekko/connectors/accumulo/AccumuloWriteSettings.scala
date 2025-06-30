package it.fpalini.pekko.connectors.accumulo

import org.apache.accumulo.core.client.{ AccumuloClient, BatchWriterConfig }
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.security.Authorizations
import org.apache.pekko
import org.apache.pekko.util.ccompat.JavaConverters.*

import java.util.Properties
import scala.collection.immutable
import scala.jdk.FunctionConverters.enrichAsScalaFromFunction

final class AccumuloWriteSettings[T] private (val accumuloClient: AccumuloClient,
    val table: String,
    val converter: T => immutable.Seq[Mutation],
    val batchWriterConfig: Option[BatchWriterConfig],
    val flushOnEach: Boolean) {

  def withAccumuloClient(accumuloClient: AccumuloClient): AccumuloWriteSettings[T] =
    copy(accumuloClient = accumuloClient)

  def withTable(table: String): AccumuloWriteSettings[T] =
    copy(table = table)

  def withConverter(converter: T => immutable.Seq[Mutation]): AccumuloWriteSettings[T] =
    copy(converter = converter)

  /**
   * Java Api
   */
  def withConverter(converter: java.util.function.Function[T, java.util.List[Mutation]]): AccumuloWriteSettings[T] =
    copy(converter = converter.asScala(_).asScala.toIndexedSeq)

  def withBatchWriterConfig(batchWriterConfig: Option[BatchWriterConfig]): AccumuloWriteSettings[T] =
    copy(batchWriterConfig = batchWriterConfig)

  def withBatchWriterConfig(flushOnEach: Boolean): AccumuloWriteSettings[T] =
    copy(flushOnEach = flushOnEach)

  /**
   * Java Api
   */
  def withFlushOnEach(flushOnEach: java.lang.Boolean): AccumuloWriteSettings[T] =
    copy(flushOnEach = flushOnEach)

  override def toString: String =
    "AccumuloSettings(" +
    s"accumuloClientProperties=${accumuloClient.properties()}," +
    s"table=$table," +
    s"converter=$converter," +
    s"batchWriterConfig=$batchWriterConfig," +
    s"flushOnEach=$flushOnEach" +
    ")"

  private def copy(
      accumuloClient: AccumuloClient = accumuloClient,
      table: String = table,
      converter: T => immutable.Seq[Mutation] = converter,
      batchWriterConfig: Option[BatchWriterConfig] = batchWriterConfig,
      flushOnEach: Boolean = flushOnEach
  ) =
    new AccumuloWriteSettings[T](accumuloClient, table, converter, batchWriterConfig, flushOnEach)

}

object AccumuloWriteSettings {

  def apply[T](
      accumuloClient: AccumuloClient,
      table: String,
      converter: T => immutable.Seq[Mutation],
      batchWriterConfig: Option[BatchWriterConfig] = None,
      flushOnEach: Boolean = false
  ) =
    new AccumuloWriteSettings(accumuloClient, table, converter, batchWriterConfig, flushOnEach)

  /**
   * Java Api
   */
  def create[T](accumuloClient: AccumuloClient,
      table: String,
      converter: java.util.function.Function[T, java.util.List[Mutation]],
      batchWriterConfig: BatchWriterConfig
  ): AccumuloWriteSettings[T] =
    AccumuloWriteSettings(accumuloClient, table, converter.asScala(_).asScala.toIndexedSeq, Option(batchWriterConfig))

  def create[T](accumuloClient: AccumuloClient,
      table: String,
      converter: java.util.function.Function[T, java.util.List[Mutation]],
      batchWriterConfig: BatchWriterConfig,
      flushOnEach: java.lang.Boolean
  ): AccumuloWriteSettings[T] =
    AccumuloWriteSettings(accumuloClient, table, converter.asScala(_).asScala.toIndexedSeq, Option(batchWriterConfig),
      flushOnEach)

  def create[T](accumuloClient: AccumuloClient,
      table: String,
      converter: java.util.function.Function[T, java.util.List[Mutation]]
  ): AccumuloWriteSettings[T] =
    AccumuloWriteSettings(accumuloClient, table, converter.asScala(_).asScala.toIndexedSeq)
}
