/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package it.fpalini.pekko.connectors.accumulo.scaladsl

import it.fpalini.pekko.connectors.accumulo.{ AccumuloReadSettings, AccumuloWriteSettings }
import it.fpalini.pekko.connectors.accumulo.impl.{ AccumuloFlowStage, AccumuloSourceStage }
import org.apache.accumulo.core.data
import org.apache.accumulo.core.data.{ Key, Value }
import org.apache.pekko
import org.apache.pekko.stream.scaladsl.{ Flow, Keep, Sink, Source }
import org.apache.pekko.{ Done, NotUsed }

import scala.concurrent.Future
import scala.jdk.CollectionConverters.SeqHasAsJava

object AccumuloStage {

  /**
   * Writes incoming element to Accumulo.
   * Accumulo mutations for every incoming element are derived from the converter functions defined in the config.
   */
  def sink[A](config: AccumuloWriteSettings[A]): Sink[A, Future[Done]] =
    Flow[A].via(flow(config)).toMat(Sink.ignore)(Keep.right)

  /**
   * Writes incoming element to Accumulo.
   * Accumulo mutations for every incoming element are derived from the converter functions defined in the config.
   */
  def flow[A](settings: AccumuloWriteSettings[A]): Flow[A, A, NotUsed] =
    Flow.fromGraph(new AccumuloFlowStage[A](settings))

  /**
   * Reads elements from Accumulo.
   */
  def source[A](ranges: Seq[data.Range], settings: AccumuloReadSettings): Source[(Key, Value), NotUsed] =
    Source.fromGraph(new AccumuloSourceStage[A](ranges.asJava, settings)).map(entry => (entry.getKey, entry.getValue))

}
