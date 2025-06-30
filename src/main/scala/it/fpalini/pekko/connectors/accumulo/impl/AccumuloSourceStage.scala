package it.fpalini.pekko.connectors.accumulo.impl

import it.fpalini.pekko.connectors.accumulo.AccumuloReadSettings
import org.apache.accumulo.core.data
import org.apache.accumulo.core.data.{Key, Value, Range as aRange}
import org.apache.pekko
import org.apache.pekko.stream.stage.{GraphStage, GraphStageLogic, OutHandler, StageLogging}
import org.apache.pekko.stream.{Attributes, Outlet, SourceShape}

import scala.util.control.NonFatal

private[accumulo] final class AccumuloSourceStage[A](ranges: java.util.Collection[data.Range], settings: AccumuloReadSettings)
    extends GraphStage[SourceShape[java.util.Map.Entry[Key, Value]]] {

  val out: Outlet[java.util.Map.Entry[Key, Value]] = Outlet("AccumuloSource.out")
  override val shape: SourceShape[java.util.Map.Entry[Key, Value]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new AccumuloSourceLogic[A](ranges, settings, out, shape)
}

private[accumulo] final class AccumuloSourceLogic[A](ranges: java.util.Collection[data.Range], settings: AccumuloReadSettings,
    out: Outlet[java.util.Map.Entry[Key, Value]],
    shape: SourceShape[java.util.Map.Entry[Key, Value]])
    extends GraphStageLogic(shape)
    with OutHandler
    with StageLogging {

  val table: String = settings.table

  private val scanner =
    (settings.authorizations, settings.numQueryThreads) match {
      case (Some(auths), Some(threads)) => settings.accumuloClient.createBatchScanner(table, auths, threads)
      case (Some(auths), None)          => settings.accumuloClient.createBatchScanner(table, auths)
      case (None, None)                 => settings.accumuloClient.createBatchScanner(table)
      case (None, Some(threads)) =>
        val myAuths =
          settings.accumuloClient.securityOperations().getUserAuthorizations(settings.accumuloClient.whoami())
        settings.accumuloClient.createBatchScanner(table, myAuths, threads)
    }
  private var results: java.util.Iterator[java.util.Map.Entry[Key, Value]] = _

  setHandler(out, this)

  override def preStart(): Unit =
    try {
      scanner.setRanges(ranges)
      settings.batchScannerCustomizer(scanner)
      results = scanner.iterator()
    } catch {
      case NonFatal(exc) =>
        failStage(exc)
    }

  override def postStop(): Unit =
    try {
      scanner.close()
      log.debug("Accumulo BatchScanner closed")
    } catch {
      case NonFatal(exc) =>
        failStage(exc)
    }

  override def onPull(): Unit =
    if (results.hasNext) {
      emit(out, results.next)
    } else {
      completeStage()
    }

}
