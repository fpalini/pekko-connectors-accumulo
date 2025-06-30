package it.fpalini.pekko.connectors.accumulo.javadsl

import java.util.concurrent.CompletionStage
import org.apache.pekko
import it.fpalini.pekko.connectors.accumulo.{ AccumuloReadSettings, AccumuloWriteSettings }
import it.fpalini.pekko.connectors.accumulo.impl.{ AccumuloFlowStage, AccumuloSourceStage }
import org.apache.accumulo.core.data
import org.apache.accumulo.core.data.{ Key, Value }
import pekko.stream.scaladsl.{ Flow, Keep, Sink, Source }
import pekko.{ Done, NotUsed }

import scala.jdk.javaapi.FutureConverters.asJava

object AccumuloStage {

  /**
   * Writes incoming element to Accumulo.
   * Accumulo mutations for every incoming element are derived from the converter functions defined in the config.
   */
  def sink[A](config: AccumuloWriteSettings[A]): pekko.stream.javadsl.Sink[A, CompletionStage[Done]] =
    Flow[A].via(flow(config)).toMat(Sink.ignore)(Keep.right).mapMaterializedValue(asJava).asJava

  /**
   * Writes incoming element to Accumulo.
   * Accumulo mutations for every incoming element are derived from the converter functions defined in the config.
   */
  def flow[A](settings: AccumuloWriteSettings[A]): pekko.stream.javadsl.Flow[A, A, NotUsed] =
    Flow.fromGraph(new AccumuloFlowStage[A](settings)).asJava

  /**
   * Reads an element from Accumulo.
   */
  def source[A](ranges: java.util.Collection[data.Range], settings: AccumuloReadSettings)
      : pekko.stream.javadsl.Source[java.util.Map.Entry[Key, Value], NotUsed] =
    Source.fromGraph(new AccumuloSourceStage[A](ranges, settings)).asJava

}
