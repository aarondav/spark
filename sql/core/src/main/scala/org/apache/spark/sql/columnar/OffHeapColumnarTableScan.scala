/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.spark.sql.columnar

import org.apache.spark.sql.execution.{LeafNode, SparkPlan}
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql._
import java.nio.ByteBuffer
import java.util.UUID
import tachyon.client.TachyonFS
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.util.ByteBufferInputStream
import io.netty.buffer.{Unpooled, ByteBuf}
import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.hadoop.conf.Configuration
import tachyon.hadoop.TFS

trait ReadBlockAsByteBuffer extends Serializable {
  // TODO: Block
  def readBlockAsByteBuffer(fs: FileSystem, path: Path): ByteBuffer
}



object OffHeapRelation {
  def apply(useCompression: Boolean, child: SparkPlan,
            createFS: () => FileSystem, blockReader: ReadBlockAsByteBuffer,
            sparkContext: SparkContext): OffHeapRelation =
    new OffHeapRelation(child.output, useCompression, child, createFS, blockReader, sparkContext)
}

private[sql] class TachyonRelationReader(
    off: OffHeapRelation,
    dir: String,
    requestedColumns: Seq[Int])
  extends RDD[Array[ByteBuffer]](off.sparkContext, Nil) {

  @transient private val fs = off.createFS()
  private val blockReader = off.blockReader

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  def compute(split: Partition, context: TaskContext): Iterator[Array[ByteBuffer]] = {
    val localFS = off.createFS()
    val partIndex = split.index
    Iterator(requestedColumns.map { colIndex =>
      val path = new Path(dir, s"part${partIndex}_col${colIndex}")
      blockReader.readBlockAsByteBuffer(localFS, path)
    }.toArray)
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getPartitions: Array[Partition] = {
    // TODO: Why 1 file = 1 block??
    val numParts = fs.listStatus(new Path(dir)).filter(_.getPath.getName.contains("col0")).length
    Array.tabulate[Partition](numParts) { i =>
      new Partition {
        override def index = i
        override def toString = s"Partition[$index]"
      }
    }
//    val PART = "part[0-9]+_col[0-9]+".r
//    val parts = fs.listStatus(dir).map(status => (status.getPath.getName, status)).collect {
//      case (PART(partNum, colNum), status) if colNum.toInt == 0 => (partNum.toInt, status)
//    }.sorted.map { case (index, status) =>
//      fs.getFileBlockLocations(status, 0, status.getLen)
//    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val path = new Path(dir, s"part${split.index}_col0")
    val status = fs.getFileStatus(path)
    fs.getFileBlockLocations(status, 0, status.getLen).flatMap(_.getHosts)
  }
}

private[sql] case class OffHeapRelation(
    output: Seq[Attribute],
    useCompression: Boolean,
    child: SparkPlan,
    createFS: () => FileSystem,
    blockReader: ReadBlockAsByteBuffer,
    @transient sparkContext: SparkContext)
  extends LogicalPlan with MultiInstanceRelation {

  override def children = Seq.empty
  override def references = Set.empty

  override def newInstance() =
    new OffHeapRelation(output.map(_.newInstance), useCompression, child, createFS, blockReader,
      sparkContext).asInstanceOf[this.type]

  val offheapBase = "/offheap" // TODO: Configurable
  val baseFolder = UUID.randomUUID().toString
  val baseDir = s"$offheapBase/$baseFolder/"

  var cached: Boolean = false

  def cacheColumnBuffers(): Unit = synchronized {
    if (cached) {
      return
    }

    cached = true
    val fz = createFS()
    fz.mkdirs(new Path(baseDir))

    val output = child.output
    val rdd0 = child.execute().mapPartitions { iterator =>
      val columnBuilders = output.map { attribute =>
        ColumnBuilder(ColumnType(attribute.dataType).typeId, 0, attribute.name, useCompression)
      }.toArray

      var row: Row = null
      while (iterator.hasNext) {
        row = iterator.next()
        var i = 0
        while (i < row.length) {
          columnBuilders(i).appendFrom(row, i)
          i += 1
        }
      }

      Iterator.single(columnBuilders.map(_.build()))
    }
    rdd0.mapPartitionsWithIndex { case (partIndex: Int, partition: Iterator[Array[ByteBuffer]]) =>
      val fs = createFS()
      for (part <- partition;
           (columnBuffer, colIndex) <- part.zipWithIndex) {
        val outStream = fs.create(new Path(baseDir, s"part${partIndex}_col${colIndex}"))
        val byteBuf = Unpooled.wrappedBuffer(columnBuffer)
        while (byteBuf.isReadable) {
          byteBuf.readBytes(outStream, byteBuf.readableBytes())
        }
        outStream.close()
      }

      Iterator[Int]()
    }.count()
  }
}

private[sql] case class OffHeapColumnarTableScan(
    attributes: Seq[Attribute],
    relation: OffHeapRelation)
  extends LeafNode {

  override def output: Seq[Attribute] = attributes

  override def execute() = {
    // Find the ordinals of the requested columns.  If none are requested, use the first.
    val requestedColumns =
      if (attributes.isEmpty) Seq(0) else attributes.map(relation.output.indexOf(_))

    new TachyonRelationReader(relation, relation.baseDir, requestedColumns)
      .mapPartitions { iterator =>
        val columnBuffers = iterator.next()
        assert(!iterator.hasNext)

        new Iterator[Row] {
          val columnAccessors = requestedColumns.map(columnBuffers(_)).map(ColumnAccessor(_))
          val nextRow = new GenericMutableRow(columnAccessors.length)

          override def next() = {
            var i = 0
            while (i < nextRow.length) {
              columnAccessors(i).extractTo(nextRow, i)
              i += 1
            }
            nextRow
          }

          override def hasNext = columnAccessors.head.hasNext
        }
      }
  }
}