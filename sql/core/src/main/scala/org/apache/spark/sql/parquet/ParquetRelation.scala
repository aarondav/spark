/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.parquet

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction

import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.metadata.CompressionCodecName
import parquet.schema.MessageType

import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedException}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical._

import scala.reflect.runtime.universe._

/**
 * Relation that consists of data stored in a Parquet columnar format.
 *
 * Users should interact with parquet files though a SchemaRDD, created by a [[SQLContext]] instead
 * of using this class directly.
 *
 * {{{
 *   val parquetRDD = sqlContext.parquetFile("path/to/parquet.file")
 * }}}
 *
 * @param path The path to the Parquet file.
 */

class HadoopDirectory(path: String) extends PhysicalLocation with HadoopDirectoryLike {
  def this(path: Path) = this(path.toString)

  def asString = path
  def asPath = new Path(path)
  def asHadoopDirectory = this

  override def equals(other: Any) = other match {
    case o: HadoopDirectory =>
      o.asString == asString
    case o: HadoopDirectoryLike =>
      o.asHadoopDirectory.asString == asString
    case _ => false
  }

  override def toString = asString
}

trait HadoopDirectoryLike {
  def asHadoopDirectory: HadoopDirectory
}

class ParquetFormat(sqlContext: SQLContext, conf: Configuration) extends RelationFormat {
  override def createEmptyRelation(
      location: PhysicalLocation,
      output: Seq[Attribute]): LogicalPlan = {
    location match {
      case dir: HadoopDirectoryLike =>
        ParquetRelation.createEmpty(dir.asHadoopDirectory, output, false, conf, sqlContext)
      case _ =>
        throw new IllegalArgumentException("Parquet relation must be a Hadoop Directory")
    }

  }

  override def loadRelation(location: PhysicalLocation): LogicalPlan = {
    location match {
      case dir: HadoopDirectoryLike => new ParquetRelation(dir.asHadoopDirectory, conf, sqlContext)
      case _ => throw new IllegalArgumentException("Parquet relation must be a Hadoop Directory")
    }
  }
}

private[sql] case class ParquetRelation(
    location: HadoopDirectory,
    @transient conf: Configuration,
    @transient sqlContext: SQLContext)
  extends SqlRelation with MultiInstanceRelation {

  self: Product =>

  /** Schema derived from ParquetFile */
  def parquetSchema: MessageType =
    ParquetTypesConverter
      .readMetaData(location.asPath, conf)
      .getFileMetaData
      .getSchema

  /** Attributes */
  override val output = ParquetTypesConverter.readSchemaFromFile(location.asPath, conf)

  override def newInstance = ParquetRelation(location, conf, sqlContext).asInstanceOf[this.type]

  // Equals must also take into account the output attributes so that we can distinguish between
  // different instances of the same relation,
  override def equals(other: Any) = other match {
    case p: ParquetRelation =>
      p.location == location && p.output == output
    case _ => false
  }

  // TODO: Use data from the footers.
  override lazy val statistics = Statistics(sizeInBytes = sqlContext.defaultSizeInBytes)
}

private[sql] object ParquetRelation {

  def enableLogForwarding() {
    // Note: Logger.getLogger("parquet") has a default logger
    // that appends to Console which needs to be cleared.
    val parquetLogger = java.util.logging.Logger.getLogger("parquet")
    parquetLogger.getHandlers.foreach(parquetLogger.removeHandler)
    // TODO(witgo): Need to set the log level ?
    // if(parquetLogger.getLevel != null) parquetLogger.setLevel(null)
    if (!parquetLogger.getUseParentHandlers) parquetLogger.setUseParentHandlers(true)
  }

  // The element type for the RDDs that this relation maps to.
  type RowType = org.apache.spark.sql.catalyst.expressions.GenericMutableRow

  // The compression type
  type CompressionType = parquet.hadoop.metadata.CompressionCodecName

  // The default compression
  val defaultCompression = CompressionCodecName.GZIP

  /**
   * Creates a new ParquetRelation and underlying Parquetfile for the given LogicalPlan. Note that
   * this is used inside [[org.apache.spark.sql.execution.SparkStrategies SparkStrategies]] to
   * create a resolved relation as a data sink for writing to a Parquetfile. The relation is empty
   * but is initialized with ParquetMetadata and can be inserted into.
   *
   * @param pathString The directory the Parquetfile will be stored in.
   * @param child The child node that will be used for extracting the schema.
   * @param conf A configuration to be used.
   * @return An empty ParquetRelation with inferred metadata.
   */
  def create(pathString: String,
             child: LogicalPlan,
             conf: Configuration,
             sqlContext: SQLContext): ParquetRelation = {
    if (!child.resolved) {
      throw new UnresolvedException[LogicalPlan](
        child,
        "Attempt to create Parquet table from unresolved child (when schema is not available)")
    }
    createEmpty(new HadoopDirectory(pathString), child.output, false, conf, sqlContext)
  }

  /**
   * Creates an empty ParquetRelation and underlying Parquetfile that only
   * consists of the Metadata for the given schema.
   *
   * @param origPath The directory the Parquetfile will be stored in.
   * @param attributes The schema of the relation.
   * @param conf A configuration to be used.
   * @return An empty ParquetRelation.
   */
  def createEmpty(origPath: HadoopDirectory,
                  attributes: Seq[Attribute],
                  allowExisting: Boolean,
                  conf: Configuration,
                  sqlContext: SQLContext): ParquetRelation = {
    val path = checkPath(origPath, allowExisting, conf)
    if (conf.get(ParquetOutputFormat.COMPRESSION) == null) {
      conf.set(ParquetOutputFormat.COMPRESSION, ParquetRelation.defaultCompression.name())
    }
    ParquetRelation.enableLogForwarding()
    ParquetTypesConverter.writeMetaData(attributes, path, conf)
    new ParquetRelation(path, conf, sqlContext) {
      override val output = attributes
    }
  }

  private def checkPath(dir: PhysicalLocation, allowExisting: Boolean, conf: Configuration): HadoopDirectory = {
    if (dir == null) {
      throw new IllegalArgumentException("Unable to create ParquetRelation: path is null")
    }
    val origPath = dir match {
      case hadoopDir: HadoopDirectory =>
        hadoopDir.asPath
      case _ =>
        throw new IllegalArgumentException("ParquetRelation can only write to Hadoop directories")
    }
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"Unable to create ParquetRelation: incorrectly formatted path $dir")
    }
    val path = origPath.makeQualified(fs)
    if (!allowExisting && fs.exists(path) && fs.listStatus(path).nonEmpty) {
      sys.error(s"File $dir already exists and is not empty.")
    }

    if (fs.exists(path) &&
        !fs.getFileStatus(path)
        .getPermission
        .getUserAction
        .implies(FsAction.READ_WRITE)) {
      throw new IOException(
        s"Unable to create ParquetRelation: path $path not read-writable")
    }
    new HadoopDirectory(path)
  }
}
