package it.fpalini.pekko.connectors.accumulo.impl

import it.fpalini.pekko.connectors.accumulo.AccumuloWriteSettings
import org.apache.accumulo.core.client.{ AccumuloClient, BatchWriter }
import org.apache.pekko
import pekko.stream.*
import pekko.stream.stage.*

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.control.NonFatal

private[accumulo] class AccumuloFlowStage[A](settings: AccumuloWriteSettings[A]) extends GraphStage[FlowShape[A, A]] {

  override protected def initialAttributes: Attributes =
    super.initialAttributes and Attributes.name("AccumuloFlow") and ActorAttributes.IODispatcher

  private val in = Inlet[A]("messages")
  private val out = Outlet[A]("(key, value)")

  override val shape: FlowShape[A, A] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {

      override protected def logSource: Class[?] = classOf[AccumuloFlowStage[A]]

      val table: String = {
        if (!settings.accumuloClient.tableOperations().exists(settings.table))
          settings.accumuloClient.tableOperations().create(settings.table)

        settings.table
      }

      val writer: BatchWriter = settings.batchWriterConfig match {
        case Some(writerConf) => settings.accumuloClient.createBatchWriter(table, writerConf)
        case None => settings.accumuloClient.createBatchWriter(table)
      }

      setHandler(out,
        new OutHandler {
          override def onPull(): Unit =
            pull(in)
        })

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val msg = grab(in)

            val mutations = settings.converter(msg)

            writer.addMutations(mutations.asJava)
            
            if(settings.flushOnEach) writer.flush()

            push(out, msg)
          }

        })

      override def postStop(): Unit = {
        log.debug("Stage completed")
        try {
          writer.flush()
          writer.close()
          log.debug("Accumulo BatchWriter closed")
        } catch {
          case NonFatal(ex) => log.error(ex, "Problem occurred during table writer close")
        }
      }
    }

}
