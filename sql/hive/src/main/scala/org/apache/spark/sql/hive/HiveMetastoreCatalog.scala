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

package org.apache.spark.sql.hive

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.FileSinkOperator
import org.apache.hadoop.hive.ql.io.{HiveInputFormat, HiveOutputFormat}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{RecordWriter, JobConf}
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql.Logging
import org.apache.spark.sql.hive.formats.ParquetInputFormat
import org.apache.spark.sql.parquet.{HadoopDirectoryLike, HadoopDirectory, ParquetRelation}

import scala.util.parsing.combinator.RegexParsers

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{FieldSchema, StorageDescriptor, SerDeInfo}
import org.apache.hadoop.hive.metastore.api.{Table => TTable, Partition => TPartition}
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Deserializer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.analysis.{EliminateAnalysisOperators, Catalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.columnar.InMemoryRelation
import org.apache.spark.sql.hive.execution.HiveTableScan

/* Implicit conversions */
import scala.collection.JavaConversions._

private[hive] class HiveMetastoreCatalog(hive: HiveContext) extends Catalog with Logging {
  import HiveMetastoreTypes._

  /** Connection to hive metastore.  Usages should lock on `this`. */
  protected[hive] val client = Hive.get(hive.hiveconf)

  val caseSensitive: Boolean = false

  def lookupRelation(
      db: Option[String],
      tableName: String,
      alias: Option[String]): LogicalPlan = synchronized {

    val (dbName, tblName) = processDatabaseAndTableName(db, tableName)
    val databaseName = dbName.getOrElse(hive.sessionState.getCurrentDatabase)
    val table = client.getTable(databaseName, tblName)
    val partitions: Seq[Partition] =
      if (table.isPartitioned) {
        client.getAllPartitionsForPruner(table).toSeq
      } else {
        Nil
      }

    val location = new HiveTableLocation(databaseName, tblName, alias, table.getTTable, partitions.map(part => part.getTPartition))

//    def toAttribute(f: FieldSchema) = AttributeReference(
//      f.getName,
//      HiveMetastoreTypes.toDataType(f.getType),
//      // Since data can be dumped in randomly with no validation, everything is nullable.
//      nullable = true
//    )(qualifiers = tableName +: alias.toSeq)
//
//    // Must be a stable value since new attributes are born here.
//    val partitionKeys = table.getPartitionKeys.map(toAttribute)
//    /** Non-partitionKey attributes */
//    val attributes = table.getTTable.getSd.getCols.map(toAttribute)

    val relationFormat = Option(table.getTTable.getSd.getParameters.get("zeformat")).getOrElse(classOf[HiveMetastoreFormat].getCanonicalName)
    logger.warn("Found format: " + relationFormat)
    val cls = Class.forName(relationFormat).asInstanceOf[Class[_ <: RelationFormat]]
    val format = hive.instantiateRelationFormat(cls)
    format.loadRelation(location)
//
//    val ParquetInputFormatClass = classOf[ParquetInputFormat]
//    table.getInputFormatClass match {
//      case ParquetInputFormatClass =>
//        ParquetRelation(table.getDataLocation.toString, hive.sparkContext.hadoopConfiguration, hive)
//
//      case _ =>
//        // Since HiveQL is case insensitive for table names we make them all lowercase.
//        MetastoreRelation(
//          databaseName, tblName, alias)(
//          table.getTTable, partitions.map(part => part.getTPartition))(hive)
//    }
  }

  def createTable(
      databaseName: String,
      tableName: String,
      schema: Seq[Attribute],
      format: Class[_ <: RelationFormat],
      location: Option[String],
      allowExisting: Boolean = false): HiveTableLocation = {
    val (dbName, tblName) = processDatabaseAndTableName(databaseName, tableName)
    val table = new Table(dbName, tblName)
    val hiveSchema =
      schema.map(attr => new FieldSchema(attr.name, toMetastoreType(attr.dataType), ""))
    table.setFields(hiveSchema)

    val sd = new StorageDescriptor
    table.getTTable.setSd(sd)
    sd.setCols(hiveSchema)

    // TODO: THESE ARE ALL DEFAULTS, WE NEED TO PARSE / UNDERSTAND the output specs.
    sd.setCompressed(false)
    sd.setParameters(Map[String, String]("zeformat" -> format.getCanonicalName))
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat")
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
    logger.error("PARAMZ> " + sd.getParameters)
    location.foreach(sd.setLocation)
    val serDeInfo = new SerDeInfo()
    serDeInfo.setName(tblName)
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
    serDeInfo.setParameters(Map[String, String]())
    sd.setSerdeInfo(serDeInfo)

    try client.createTable(table) catch {
      case e: org.apache.hadoop.hive.ql.metadata.HiveException
        if e.getCause.isInstanceOf[org.apache.hadoop.hive.metastore.api.AlreadyExistsException] &&
           allowExisting => // Do nothing.
    }

    new HiveTableLocation(databaseName, tblName, None, table.getTTable, Nil)
  }

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   */
  object CreateTables extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case InsertIntoCreatedTable(db, tableName, formatClass, child) =>
        val (dbName, tblName) = processDatabaseAndTableName(db, tableName)
        val databaseName = dbName.getOrElse(hive.sessionState.getCurrentDatabase)

        val location = createTable(databaseName, tblName, child.output, formatClass, None)

        val format = hive.instantiateRelationFormat(formatClass)
        val relation = format.createEmptyRelation(location, child.output)

//        logger.error("FOUND TABLE: " + lookupRelation(Some(databaseName), tblName, None))
        val output = InsertIntoTable(
          EliminateAnalysisOperators(relation),
          Map.empty,
          child,
          overwrite = false)
        logger.error("Got output: " + output.treeString)
        output
    }
  }

  /**
   * Casts input data to correct data types according to table definition before inserting into
   * that table.
   */
  object PreInsertionCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      // Wait until children are resolved
      case p: LogicalPlan if !p.childrenResolved => p

      case p @ InsertIntoTable(table: MetastoreRelation, _, child, _) =>
        castChildOutput(p, table, child)

      case p @ logical.InsertIntoTable(
                 InMemoryRelation(_, _,
                   HiveTableScan(_, table, _)), _, child, _) =>
        castChildOutput(p, table, child)
    }

    def castChildOutput(p: InsertIntoTable, table: MetastoreRelation, child: LogicalPlan) = {
      val childOutputDataTypes = child.output.map(_.dataType)
      // Only check attributes, not partitionKeys since they are always strings.
      // TODO: Fully support inserting into partitioned tables.
      val tableOutputDataTypes = table.attributes.map(_.dataType)

      if (childOutputDataTypes == tableOutputDataTypes) {
        p
      } else {
        // Only do the casting when child output data types differ from table output data types.
        val castedChildOutput = child.output.zip(table.output).map {
          case (input, output) if input.dataType != output.dataType =>
            Alias(Cast(input, output.dataType), input.name)()
          case (input, _) => input
        }

        p.copy(child = logical.Project(castedChildOutput, child))
      }
    }
  }

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory OverrideCatalog.
   */
  override def registerTable(
      databaseName: Option[String], tableName: String, plan: LogicalPlan): Unit = ???

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory OverrideCatalog.
   */
  override def unregisterTable(
      databaseName: Option[String], tableName: String): Unit = ???

  override def unregisterAllTables() = {}
}

/**
 * :: DeveloperApi ::
 * Provides conversions between Spark SQL data types and Hive Metastore types.
 */
@DeveloperApi
object HiveMetastoreTypes extends RegexParsers {
  protected lazy val primitiveType: Parser[DataType] =
    "string" ^^^ StringType |
    "float" ^^^ FloatType |
    "int" ^^^ IntegerType |
    "tinyint" ^^^ ByteType |
    "smallint" ^^^ ShortType |
    "double" ^^^ DoubleType |
    "bigint" ^^^ LongType |
    "binary" ^^^ BinaryType |
    "boolean" ^^^ BooleanType |
    "decimal" ^^^ DecimalType |
    "timestamp" ^^^ TimestampType |
    "varchar\\((\\d+)\\)".r ^^^ StringType

  protected lazy val arrayType: Parser[DataType] =
    "array" ~> "<" ~> dataType <~ ">" ^^ {
      case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    "map" ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    "[a-zA-Z0-9_]*".r ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    "struct" ~> "<" ~> repsep(structField,",") <~ ">"  ^^ {
      case fields => new StructType(fields)
    }

  protected lazy val dataType: Parser[DataType] =
    arrayType |
    mapType |
    structType |
    primitiveType

  def toDataType(metastoreType: String): DataType = parseAll(dataType, metastoreType) match {
    case Success(result, _) => result
    case failure: NoSuccess => sys.error(s"Unsupported dataType: $metastoreType")
  }

  def toMetastoreType(dt: DataType): String = dt match {
    case ArrayType(elementType, _) => s"array<${toMetastoreType(elementType)}>"
    case StructType(fields) =>
      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
    case MapType(keyType, valueType, _) =>
      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
    case StringType => "string"
    case FloatType => "float"
    case IntegerType => "int"
    case ByteType => "tinyint"
    case ShortType => "smallint"
    case DoubleType => "double"
    case LongType => "bigint"
    case BinaryType => "binary"
    case BooleanType => "boolean"
    case DecimalType => "decimal"
    case TimestampType => "timestamp"
  }
}

case class HiveTableLocation(databaseName: String, tableName: String, alias: Option[String], table: TTable, partitions: Seq[TPartition])
  extends TableLocation with HadoopDirectoryLike {
  override def asHadoopDirectory: HadoopDirectory = {
    require(partitions.isEmpty, "A partitioned table cannot be represented as a HadoopDirectory")
    new HadoopDirectory(table.getSd.getLocation)
  }
}

private[hive] case class MetastoreRelation
    (location: HiveTableLocation)
    (@transient hiveContext: HiveContext)
  extends LeafNode {

  self: Product =>

  // TODO: Can we use org.apache.hadoop.hive.ql.metadata.Table as the type of table and
  // use org.apache.hadoop.hive.ql.metadata.Partition as the type of elements of partitions.
  // Right now, using org.apache.hadoop.hive.ql.metadata.Table and
  // org.apache.hadoop.hive.ql.metadata.Partition will cause a NotSerializableException
  // which indicates the SerDe we used is not Serializable.

  @transient lazy val hiveQlTable = new Table(location.table)

  def hiveQlPartitions = location.partitions.map { p =>
    new Partition(hiveQlTable, p)
  }

  @transient override lazy val statistics = Statistics(
    sizeInBytes = {
      // TODO: check if this estimate is valid for tables after partition pruning.
      // NOTE: getting `totalSize` directly from params is kind of hacky, but this should be
      // relatively cheap if parameters for the table are populated into the metastore.  An
      // alternative would be going through Hadoop's FileSystem API, which can be expensive if a lot
      // of RPCs are involved.  Besides `totalSize`, there are also `numFiles`, `numRows`,
      // `rawDataSize` keys that we can look at in the future.
      BigInt(
        Option(hiveQlTable.getParameters.get("totalSize"))
          .map(_.toLong)
          .getOrElse(hiveContext.defaultSizeInBytes))
    }
  )

  val tableDesc = new TableDesc(
    Class.forName(hiveQlTable.getSerializationLib).asInstanceOf[Class[Deserializer]],
    hiveQlTable.getInputFormatClass,
    // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
    // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
    // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
    // HiveSequenceFileOutputFormat.
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata
  )

  implicit class SchemaAttribute(f: FieldSchema) {
    def toAttribute = AttributeReference(
      f.getName,
      HiveMetastoreTypes.toDataType(f.getType),
      // Since data can be dumped in randomly with no validation, everything is nullable.
      nullable = true
    )(qualifiers = location.tableName +: location.alias.toSeq)
  }

  // Must be a stable value since new attributes are born here.
  val partitionKeys = hiveQlTable.getPartitionKeys.map(_.toAttribute)

  /** Non-partitionKey attributes */
  val attributes = location.table.getSd.getCols.map(_.toAttribute)

  val output = attributes ++ partitionKeys
}

class HiveMetastoreFormat(hiveContext: HiveContext, conf: Configuration) extends RelationFormat {
  override def createEmptyRelation(location: TableLocation, output: Seq[Attribute]): LogicalPlan = {
    // TODO: Do
    new MetastoreRelation(location.asInstanceOf[HiveTableLocation])(hiveContext)
  }

  override def loadRelation(location: TableLocation): LogicalPlan = {
    new MetastoreRelation(location.asInstanceOf[HiveTableLocation])(hiveContext)
//    location match {
//      case hadoopDir: HadoopDirectory => new ParquetRelation(hadoopDir, conf, sqlContext)
//      case _ => throw new IllegalArgumentException("Parquet relation must be a Hadoop Directory")
//    }
  }
}
