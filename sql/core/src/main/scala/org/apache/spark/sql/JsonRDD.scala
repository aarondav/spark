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

package org.apache.spark.sql

import scala.collection.JavaConversions._
import scala.util.parsing.json.JSON
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.ExistingRdd
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.StructField
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.SparkLogicalPlan
import org.apache.spark.sql.SchemaRDD
import com.fasterxml.jackson.core.{JsonToken, JsonFactory, JsonParser}
import com.fasterxml.jackson.databind.ObjectMapper

/*
/**
 * :: Experimental ::
 * When to resolve the schema of a json table.
 * EAGER means to infer the schema when a json table is registered as a SchemaRDD.
 * LAZY means to infer the schema based on the submitted query (actually, every query has
 * its own view).
 */
object SchemaResolutionMode extends Enumeration {

  type SchemaResolutionMode = Value
  val EAGER, LAZY = Value
}
*/

sealed trait SchemaResolutionMode

case object EAGER_SCHEMA_RESOLUTION extends SchemaResolutionMode
case class EAGER_SCHEMA_RESOLUTION_WITH_SAMPLING(val fraction: Double) extends SchemaResolutionMode
case object LAZY_SCHEMA_RESOLUTION extends SchemaResolutionMode

/**
 * :: Experimental ::
 * Turns a nested schema in to a flat one.  Does not support "repeated groups", for example, an Array.
 *
 * Possible issues:
 *  - Nullability semantics.
 *  - How are structs different than attributes?
 */
@Experimental
object Flatten extends Serializable {
  def allNestedAttributes(input: Expression): Seq[Expression] = input.dataType match {
    case StructType(fields) =>
      fields.flatMap(f => allNestedAttributes(GetField(input, f.name)))
    case _ => input :: Nil
  }

  def nameUnnesting(input: Expression): String = input match {
    case ar: AttributeReference => ar.name
    case GetField(base, field) => nameUnnesting(base) + "_" + field
  }

  def aliasFlattenedAttribute(input: Expression): NamedExpression = input match {
    case ar: AttributeReference => ar
    case other => Alias(other, nameUnnesting(other))()
  }

  implicit class AddFlatten(plan: SchemaRDD) {
    def flattenSchema: SchemaRDD = {
      val flattenedSchema = plan.queryExecution.logical.output.flatMap(allNestedAttributes)
      plan.select(flattenedSchema.map(aliasFlattenedAttribute) :_*)
    }
  }
}

/**
 * :: Experimental ::
 * Converts a JSON file to a SparkSQL logical query plan.  This implementation is only designed to
 * work on JSON files that have mostly uniform schema.  The conversion suffers from the following
 * limitations:
 *  - The data is optionally sampled to determine all of the possible fields.  Any fields that do
 *    not appear in this sample will not be included in the final output.
 *  - All fields are assumed to be of type string or nested record.  If a single field has multiple
 *    types then the results are unspecified.
 */
@Experimental
object JsonTable extends Serializable {
  def fromRDD(sqlContext: SQLContext, json: RDD[String], sampleSchema: Option[Double] = None): SchemaRDD = {
    val schema = inferSchemaWithJacksonStreaming2(json, sampleSchema)
    new SchemaRDD(sqlContext, schema)
  }

  def inferSchemaWithJacksonStreaming2(json: RDD[String], sampleSchema: Option[Double] = None): LogicalPlan = {
    val schemaData = sampleSchema.map(json.sample(false, _, 1)).getOrElse(json)
    val allKeys =
      schemaData.mapPartitions(iter => {
        val jsonFactory = new JsonFactory()
        iter.map(record => getAllKeysWithType(jsonFactory, record))
      }).reduce((x, y) => (x._1 ++ y._1, x._2 ++ y._2, x._3 ++ y._3))
    val (pSet, sSet, aSet) = allKeys
    // TODO: Resolve conflict
    val primitiveSet = pSet.toSeq.map(key => key.substring(1, key.length - 1).split("`.`").toSeq)
    val structSet = sSet.map(key => key.substring(1, key.length - 1).split("`.`").toSeq)
    val arraySet = aSet.map(key => key.substring(1, key.length - 1).split("`.`").toSeq)

    def makeStruct(values: Seq[Seq[String]], prefix: Seq[String]): StructType = {
      val (atomLikes, structLikes) = values.partition(_.size == 1)

      // Handle primitive types and arrays with primitive elements
      val (atomArrays, atoms) = atomLikes.partition(name => arraySet.contains(prefix ++ name))
      val atomFields = atoms.map(a => StructField(a.head, StringType, nullable = true))
      val atomArrayFields: Seq[StructField] = atomArrays.map {
        a => StructField(a.head, ArrayType(StringType), nullable = true)
      }

      val structFields: Seq[StructField] = structLikes.groupBy(_(0)).map {
        case (name, fields) => {
          val nestedFields = fields.map(_.tail)
          val structType = makeStruct(nestedFields, prefix :+ name)
          if (arraySet.contains(prefix :+ name)) {
            StructField(name, ArrayType(structType), nullable = true)
          } else {
            StructField(name, structType, nullable = true)
          }
        }
      }.toSeq

      StructType(atomFields ++ atomArrayFields ++ structFields)
    }

    val schema = makeStruct(primitiveSet, Nil)
    // println("primitiveSet: " + primitiveSet)
    // println("structSet: " + structSet)
    // println("arraySet: " + arraySet)
    // println(schema)


    // val view = makeStruct(Seq(Seq("text")), Nil)
    /*
    schemaData.collect().toSeq.map(JSON.parseFull(_).getOrElse(Map.empty[String, Any])).
      map(_.asInstanceOf[Map[String, Any]]).map(json => asRow(json, schema)).foreach(println)
    */

    SparkLogicalPlan(
      ExistingRdd(
        asAttributes(schema),
        parseJsonWithJackson(json).map(asRow(_, schema))))

  }

  def inferSchemaWithJacksonStreaming(json: RDD[String], sampleSchema: Option[Double] = None): LogicalPlan = {
    val schemaData = sampleSchema.map(json.sample(false, _, 1)).getOrElse(json)
    val allKeys =
      schemaData.mapPartitions(iter => {
        val jsonFactory = new JsonFactory()
        iter.map(record => getAllKeys(jsonFactory, record))
      }).reduce(_ ++ _)
    // println(allKeys)
    // println(allKeys.toSeq.sorted.map(key => key.substring(1, key.length - 1).split("`.`").toSeq))
    val schema = makeStruct(allKeys.toSeq.map(key => key.substring(1, key.length - 1).split("`.`").toSeq))

    /*
    val view = makeStruct(Seq(Seq("source")))
    schemaData.collect().toSeq.map(JSON.parseFull(_).getOrElse(Map.empty[String, Any])).
      map(_.asInstanceOf[Map[String, Any]]).map(json => asRow(json, schema)).foreach(println)
    */

    SparkLogicalPlan(
      ExistingRdd(
        asAttributes(schema),
        parseJsonWithJackson(json).map(asRow(_, schema))))

  }

  def inferSchemaWithJackson(json: RDD[String], sampleSchema: Option[Double] = None): LogicalPlan = {
    val schemaData = sampleSchema.map(json.sample(false, _, 1)).getOrElse(json)
    val allKeys =
      parseJsonWithJackson(schemaData)
        .map(getAllKeys)
        .reduce(_ ++ _)
    // println(allKeys)
    // println(allKeys.toSeq.sorted.map(key => key.split("\\.").toSeq))
    val schema = makeStruct(allKeys.toSeq.map(_.split("\\.").toSeq))

    SparkLogicalPlan(
      ExistingRdd(
        asAttributes(schema),
        parseJsonWithJackson(json).map(asRow(_, schema))))
  }

  def inferSchemaScalaJson(json: RDD[String], sampleSchema: Option[Double] = None): LogicalPlan = {
    val schemaData = sampleSchema.map(json.sample(false, _, 1)).getOrElse(json)
    val allKeys =
      parseJson(schemaData)
        .map(getAllKeys)
        .reduce(_ ++ _)
    // println(allKeys)
    // println(allKeys.toSeq.sorted.map(key => key.split("\\.").toSeq))
    val schema = makeStruct(allKeys.toSeq.map(_.split("\\.").toSeq))

    SparkLogicalPlan(
      ExistingRdd(
        asAttributes(schema),
        parseJson(json).map(asRow(_, schema))))
  }

  def getAllKeys(jsonFactory: JsonFactory, record: String): Set[String] = {
    val jsonParser: JsonParser = jsonFactory.createParser(record)
    getAllKeys(jsonParser)
  }

  protected def getAllKeys(jsonParser: JsonParser): Set[String] = {
    var nameSet: Set[String] = Set[String]()
    var currentName: String = null
    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME) {
        // The field name is quoted with backticks to because the field name can have
        // dot(.).
        currentName = s"`${jsonParser.getCurrentName}`"
        val nextToken = jsonParser.nextToken()

        if (nextToken == JsonToken.START_OBJECT) {
          // The value of currentName is an object.
          nameSet = nameSet ++ getAllKeys(jsonParser).map(k => s"$currentName.$k")
        } else if (nextToken == JsonToken.START_ARRAY) {
          // The value of currentName is an array.
          while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            if (jsonParser.getCurrentToken == JsonToken.START_OBJECT) {
              nameSet = nameSet ++ getAllKeys(jsonParser).map(k => s"$currentName.$k")
            }
          }
        } else {
          // The value of currentName is a string, a number, a boolean, or a null.
          nameSet = nameSet + currentName
        }
      }
    }
    nameSet
  }

  def getAllKeysWithType(
                          jsonFactory: JsonFactory, record: String): (Set[String], Set[String], Set[String]) = {
    val jsonParser: JsonParser = jsonFactory.createParser(record)
    val (pSet, sSet, aSet) = getAllKeysWithType(jsonParser)
    jsonParser.close()

    (pSet, sSet, aSet)
  }

  protected def getAllKeysWithType(
                                    jsonParser: JsonParser): (Set[String], Set[String], Set[String]) = {
    var nameSet: Set[String] = Set[String]()
    var structSet: Set[String] = Set[String]()
    var arraySet: Set[String] = Set[String]()
    var currentName: String = null

    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME) {
        // The field name is quoted with backticks to because the field name can have
        // dot(.).
        currentName = s"`${jsonParser.getCurrentName}`"
        val nextToken = jsonParser.nextToken()

        if (nextToken == JsonToken.START_OBJECT) {
          // The value of currentName is an object.
          val (pSet, sSet, aSet) = getAllKeysWithType(jsonParser)
          nameSet = nameSet ++ pSet.map(k => s"$currentName.$k")
          structSet = structSet ++ sSet.map(k => s"$currentName.$k")
          arraySet = arraySet ++ aSet.map(k => s"$currentName.$k")
          structSet = structSet + currentName
        } else if (nextToken == JsonToken.START_ARRAY) {
          // The value of currentName is an array.
          var isArrayOfStructs = false
          while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            if (jsonParser.getCurrentToken == JsonToken.START_OBJECT) {
              isArrayOfStructs = true
              val (pSet, sSet, aSet) = getAllKeysWithType(jsonParser)
              nameSet = nameSet ++ pSet.map(k => s"$currentName.$k")
              structSet = structSet ++ sSet.map(k => s"$currentName.$k")
              arraySet = arraySet ++ aSet.map(k => s"$currentName.$k")
            }
          }
          arraySet = arraySet + currentName
          if (!isArrayOfStructs) nameSet = nameSet + currentName
        } else {
          // The value of currentName is a string, a number, a boolean, or a null.
          nameSet = nameSet + currentName
        }
      }
    }

    (nameSet, structSet, arraySet)
  }

  protected def getAllKeys(m: Map[String, Any]): Set[String] = {
    m.flatMap {
      case (key, nestedValues: Map[String, Any]) =>
        getAllKeys(nestedValues).map(k => s"$key.$k")
      case (key: String, _) => key :: Nil
    }.toSet
  }

  protected def parseJsonWithJackson(json: RDD[String]): RDD[Map[String, Any]] = {
    json.mapPartitions(iter => {
      val mapper = new ObjectMapper()
      iter.map { record =>
        val raw = mapper.readValue(record, classOf[java.util.Map[String, Any]])
        scalafy(raw).asInstanceOf[Map[String, Any]]
      }
    })
  }

  def parseJson(json: RDD[String]): RDD[Map[String, Any]] = {
    json
      .map(JSON.parseFull(_).getOrElse(Map.empty[String, Any]))
      .map(_.asInstanceOf[Map[String, Any]])
  }
  def scalafy(obj: Any): Any = obj match {
    case map: java.util.Map[String, Object] =>
      map.toMap.mapValues(scalafy)
    case list: java.util.List[Object] =>
      list.toList.map(scalafy)
    case atom => atom
  }

  protected def asRow(json: Map[String,Any], schema: StructType): Row = {
    val row = new GenericMutableRow(schema.fields.length)
    schema.fields.zipWithIndex.foreach {
      case (StructField(name, StringType, _), i) =>
        row.update(i, json.get(name).flatMap(v => Option(v)).map(_.toString).orNull)
      case (StructField(name, fields: StructType, _), i) =>
        row.update(i,
          json.get(name).flatMap(v => Option(v)).map(v => asRow(v.asInstanceOf[Map[String, Any]], fields)).orNull)
      case (StructField(name, ArrayType(StringType), _), i) =>
        row.update(i,
          json.get(name).flatMap(v => Option(v)).map(v => v.asInstanceOf[Seq[String]]).orNull)
      case (StructField(name, ArrayType(structType: StructType), _), i) =>
        row.update(i,
          json.get(name).flatMap(v => Option(v)).map(
            v => v.asInstanceOf[Seq[Any]].map(
              e => asRow(e.asInstanceOf[Map[String, Any]], structType))).orNull)
    }
    row
  }

  def makeStruct(values: Seq[Seq[String]]): StructType = {
    // TODO: handle array
    val (atoms, structs) = values.partition(_.size == 1)
    val atomFields = atoms.map(a => StructField(a.head, StringType, nullable = true))
    val structFields: Seq[StructField] = structs.groupBy(_(0)).map {
      case (name, fields) =>
        val nestedFields = fields.map(_.tail)
        val structType = makeStruct(nestedFields)
        StructField(name, structType, nullable = true)
    }.toSeq
    StructType(atomFields ++ structFields)
  }

  protected def asAttributes(struct: StructType): Seq[AttributeReference] = {
    struct.fields.map(f => AttributeReference(f.name, f.dataType, nullable = true)())
  }
}