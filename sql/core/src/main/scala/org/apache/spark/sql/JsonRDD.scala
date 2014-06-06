package org.apache.spark.sql

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{ExistingRdd, SparkLogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.Logging
import org.apache.spark.sql.catalyst.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, GetField}

import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConversions._
import scala.math.BigDecimal

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
 * limitation:
 *  - The data is optionally sampled to determine all of the possible fields.  Any fields that do
 *    not appear in this sample will not be included in the final output.
 */
@Experimental
object JsonTable extends Serializable with Logging {

  def fromRDD(
      sqlContext: SQLContext, json: RDD[String],
      sampleSchema: Option[Double] = None): SchemaRDD = {
    val schema = inferSchema(json, sampleSchema)
    new SchemaRDD(sqlContext, schema)
  }

  def inferSchema(
                   json: RDD[String], sampleSchema: Option[Double] = None): LogicalPlan = {
    val schemaData = sampleSchema.map(json.sample(false, _, 1)).getOrElse(json)
    val allKeys = parseJson(schemaData).map(getAllKeysWithValueTypes).reduce(_ ++ _)
    // Resolve type conflicts
    val resolved = allKeys.groupBy {
      case (key, dataType) => key
    }.map {
      // Now, keys and types are organized in the format of
      // key -> Set(type1, type2, ...).
      case (key, typeSet) => {
        val fieldName = key.substring(1, key.length - 1).split("`.`").toSeq
        val dataType = typeSet.map {
          case (_, dataType) => dataType
        }.reduce((type1: DataType, type2: DataType) => getCompitableType(type1, type2))
        dataType match {
          case NullType => (fieldName, StringType)
          case ArrayType(NullType) => (fieldName, ArrayType(StringType))
          case other => (fieldName, other)
        }
      }
    }

    def makeStruct(values: Seq[Seq[String]], prefix: Seq[String]): StructType = {
      val (topLevel, structLike) = values.partition(_.size == 1)
      val topLevelFields = topLevel.filter {
        name => resolved.get(prefix ++ name).get match {
          case ArrayType(StructType(Nil)) => false
          case ArrayType(_) => true
          case struct: StructType => false
          case _ => true
        }
      }.map {
        a => StructField(a.head, resolved.get(prefix ++ a).get, nullable = true)
      }.sortBy {
        case StructField(name, _, _) => name
      }

      val structFields: Seq[StructField] = structLike.groupBy(_(0)).map {
        case (name, fields) => {
          val nestedFields = fields.map(_.tail)
          val structType = makeStruct(nestedFields, prefix :+ name)
          val dataType = resolved.get(prefix :+ name).get
          dataType match {
            case array: ArrayType => StructField(name, ArrayType(structType), nullable = true)
            case struct: StructType => StructField(name, structType, nullable = true)
            // dataType is StringType means that we have resolved type conflicts involving
            // primitive types and complex types. So, the type of name has relaxed to StringType.
            case StringType => StructField(name, StringType, nullable = true)
          }
        }
      }.toSeq.sortBy {
        case StructField(name, _, _) => name
      }

      StructType(topLevelFields ++ structFields)
    }

    val schema = makeStruct(resolved.keySet.toSeq, Nil)

    SparkLogicalPlan(
      ExistingRdd(
        asAttributes(schema),
        parseJson(json).map(asRow(_, schema))))
  }

  // See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types.
  // The conversion for integral and floating point types have a linear widening hierarchy:
  val numericPrecedence =
    Seq(NullType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)
  // Boolean is only wider than Void
  val booleanPrecedence = Seq(NullType, BooleanType)
  val allPromotions: Seq[Seq[DataType]] = numericPrecedence :: booleanPrecedence :: Nil

  /**
   * Returns the most general data type for two given data types.
   */
  protected def getCompitableType(t1: DataType, t2: DataType): DataType = {
    // Try and find a promotion rule that contains both types in question.
    val applicableConversion = allPromotions.find(p => p.contains(t1) && p.contains(t2))

    // If found return the widest common type, otherwise None
    val returnType = applicableConversion.map(_.filter(t => t == t1 || t == t2).last)

    if (returnType.isDefined) {
      returnType.get
    } else {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        case (other: DataType, NullType) if other != NullType => other
        case (NullType, other: DataType) if other != NullType => other
        // TODO: Returns the union of fields1 and fields2?
        case (StructType(fields1), StructType(fields2))
          if (fields1 == fields2) => StructType(fields1)
        case (ArrayType(elementType1), ArrayType(elementType2)) =>
          ArrayType(getCompitableType(elementType1, elementType2))
        case (_, _) => StringType
      }
    }
  }

  protected def getPrimitiveType(value: Any): DataType = {
    value match {
      case value: String => StringType
      case value: Int => IntegerType
      case value: Long => LongType
      case value: Double => DoubleType
      case value: BigDecimal => DecimalType
      case value: Boolean => BooleanType
      case null => NullType
      // We comment out the following line in the development to catch bugs.
      // We need to enable this line in future to handle
      // unexpected data type.
      // case _ => StringType
    }
  }

  /**
   * Returns the element type of an JSON array. We go through all elements of this array
   * to detect any possible type conflict. We use [[getCompitableType]] to resolve
   * type conflicts. Right now, when the element of an array is another array, we
   * treat the element as String.
   */
  protected def getTypeOfArray(l: Seq[Any]): ArrayType = {
    val elements = l.flatMap(v => Option(v))
    if (elements.isEmpty) {
      // If this JSON array is empty, we use NullType as a placeholder.
      // If this array is not empty in other JSON objects, we can resolve
      // the type after we have passed through all JSON objects.
      ArrayType(NullType)
    } else {
      val elementType = elements.map {
        e => e match {
          case map: Map[_, _] => StructType(Nil)
          // We have an array of arrays. If those element arrays do not have the same
          // element types, we will return ArrayType[StringType].
          case seq: Seq[_] =>  getTypeOfArray(seq)
          case value => getPrimitiveType(value)
        }
      }.reduce((type1: DataType, type2: DataType) => getCompitableType(type1, type2))

      ArrayType(elementType)
    }
  }

  /**
   * Figures out all key names and data types of values from a parsed JSON object
   * (in the format of Map[Stirng, Any]). When a value of a key is an object, we
   * only use a placeholder for a struct type (StructType(Nil)) instead of getting
   * all fields of this struct because a field does not appear in this JSON object
   * can appear in other JSON objects.
   */
  protected def getAllKeysWithValueTypes(m: Map[String, Any]): Set[(String, DataType)] = {
    m.map{
      // Quote the key with backticks to handle cases which have dots
      // in the field name.
      case (key, dataType) => (s"`$key`", dataType)
    }.flatMap {
      case (key: String, struct: Map[String, Any]) => {
        // The value associted with the key is an JSON object.
        getAllKeysWithValueTypes(struct).map {
          case (k, dataType) => (s"$key.$k", dataType)
        } ++ Set((key, StructType(Nil)))
      }
      case (key: String, array: List[Any]) => {
        // The value associted with the key is an array.
        getTypeOfArray(array) match {
          case ArrayType(StructType(Nil)) => {
            // The elements of this arrays are structs.
            array.asInstanceOf[List[Map[String, Any]]].flatMap {
              element => getAllKeysWithValueTypes(element)
            }.map {
              case (k, dataType) => (s"$key.$k", dataType)
            } :+ (key, ArrayType(StructType(Nil)))
          }
          case ArrayType(elementType) => (key, ArrayType(elementType)) :: Nil
        }
      }
      case (key: String, value) => (key, getPrimitiveType(value)) :: Nil
    }.toSet
  }

  /**
   * Converts a Java Map/List to a Scala Map/List.
   * We do not use Jackson's scala module at here because
   * DefaultScalaModule in jackson-module-scala will make
   * the parsing very slow.
   */
  protected def scalafy(obj: Any): Any = obj match {
    case map: java.util.Map[String, Object] =>
      // .map(identity) is used as a workaround of non-serializable Map
      // generated by .mapValues.
      // This issue is documented at https://issues.scala-lang.org/browse/SI-7005
      map.toMap.mapValues(scalafy).map(identity)
    case list: java.util.List[Object] =>
      list.toList.map(scalafy)
    // Since we do not have a data type backed by BigInt,
    // when we see a Java BigInteger, we use BigDecimal.
    case bigInteger: java.math.BigInteger => scala.math.BigDecimal(bigInteger)
    case bigDecimal: java.math.BigDecimal => scala.math.BigDecimal(bigDecimal)
    case atom => atom
  }

  protected def parseJson(json: RDD[String]): RDD[Map[String, Any]] = {
    // According to [Jackson-72: https://jira.codehaus.org/browse/JACKSON-72],
    // ObjectMapper will not return BigDecimal when
    // "DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS" is disabled
    // (see NumberDeserializer.deserialize for the logic).
    // But, we do not want to enable this feature because it will use BigDecimal
    // for every float number, which will be slow.
    // So, right now, we will have Infinity for those BigDecimal number.
    // TODO: Support BigDecimal.
    json.mapPartitions(iter => {
      // When there is a key appearing multiple times (a duplicate key),
      // the ObjectMapper will take the last value associated with this duplicate key.
      // For example: for {"key": 1, "key":2}, we will get "key"->2.
      val mapper = new ObjectMapper()
      iter.map(record => mapper.readValue(record, classOf[Object]))
    }).map(scalafy).map(_.asInstanceOf[Map[String, Any]])
  }

  // TODO: we need to do a type conversion based on the schema we have.
  protected def asRow(json: Map[String,Any], schema: StructType): Row = {
    val row = new GenericMutableRow(schema.fields.length)
    schema.fields.zipWithIndex.foreach {
      case (StructField(name, fields: StructType, _), i) =>
        // This field is a struct.
        row.update(i, json.get(name).flatMap(v => Option(v)).map(
          v => asRow(v.asInstanceOf[Map[String, Any]], fields)).orNull)
      case (StructField(name, ArrayType(structType: StructType), _), i) =>
        // This field is an array of structs.
        row.update(i,
          json.get(name).flatMap(v => Option(v)).map(
            v => v.asInstanceOf[Seq[Any]].map(
              e => asRow(e.asInstanceOf[Map[String, Any]], structType))).orNull)
      case (StructField(name, ArrayType(StringType), _), i) =>
        // This field is an array and the element type is String.
        // Because element types may have conflict, we need to explicitly
        // cast the list to a list of String.
        row.update(i,
          json.get(name).flatMap(v => Option(v)).map(
            v => v.asInstanceOf[Seq[String]]).orNull)
      case (StructField(name, ArrayType(_), _), i) =>
        // This field is an array and the element type is not struct and String.
        row.update(i,
          json.get(name).flatMap(v => Option(v)).map(
            v => v.asInstanceOf[Seq[Any]]).orNull)
      case (StructField(name, StringType, _), i) =>
        row.update(i, json.get(name).flatMap(v => Option(v)).map(_.toString).getOrElse(null))
      case (StructField(name, DecimalType, _), i) =>
        row.update(i, json.get(name).flatMap(v => Option(v)).map {
          v => v match {
            case value: Int => BigDecimal(value)
            case value: Long => BigDecimal(value)
            case value: Double => BigDecimal(value)
            case value: BigDecimal => value
          }
        }.getOrElse(null))
      case (StructField(name, _, _), i) =>
        row.update(i, json.get(name).flatMap(v => Option(v)).getOrElse(null))
    }

    row
  }

  protected def asAttributes(struct: StructType): Seq[AttributeReference] = {
    struct.fields.map(f => AttributeReference(f.name, f.dataType, nullable = true)())
  }

  def printSchema(logicalPlan: LogicalPlan): Unit = {
    println(generateSchemaTreeString(logicalPlan.output))
  }

  def generateSchemaTreeString(schema: Seq[Attribute]): String = {
    val builder = new StringBuilder
    builder.append("root\n")
    val prefix = " |"
    schema.foreach {
      attribute => {
        val name = attribute.name
        val dataType = attribute.dataType
        dataType match {
          case fields: StructType =>
            builder.append(s"$prefix-- $name: $StructType\n")
            generateSchemaTreeString(fields, s"$prefix    |", builder)
          case ArrayType(fields: StructType) =>
            builder.append(s"$prefix-- $name: $ArrayType[$StructType]\n")
            generateSchemaTreeString(fields, s"$prefix    |", builder)
          case ArrayType(elementType: DataType) =>
            builder.append(s"$prefix-- $name: $ArrayType[$elementType]\n")
          case _ => builder.append(s"$prefix-- $name: $dataType\n")
        }
      }
    }

    builder.toString()
  }

  def generateSchemaTreeString(
                                schema: StructType,
                                prefix: String,
                                builder: StringBuilder): StringBuilder = {
    schema.fields.foreach {
      case StructField(name, fields: StructType, _) =>
        builder.append(s"$prefix-- $name: $StructType\n")
        generateSchemaTreeString(fields, s"$prefix    |", builder)
      case StructField(name, ArrayType(fields: StructType), _) =>
        builder.append(s"$prefix-- $name: $ArrayType[$StructType]\n")
        generateSchemaTreeString(fields, s"$prefix    |", builder)
      case StructField(name, ArrayType(elementType: DataType), _) =>
        builder.append(s"$prefix-- $name: $ArrayType[$elementType]\n")
      case StructField(name, fieldType: DataType, _) =>
        builder.append(s"$prefix-- $name: $fieldType\n")
    }

    builder
  }
}
