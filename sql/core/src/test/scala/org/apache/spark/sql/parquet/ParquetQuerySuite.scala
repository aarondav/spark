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

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapreduce.Job

import parquet.hadoop.ParquetFileWriter
import parquet.hadoop.util.ContextUtil
import parquet.schema.MessageTypeParser

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.getTempFilePath
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.TestData
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.util.Utils

// Implicits
import org.apache.spark.sql.test.TestSQLContext._

case class TestRDDEntry(key: Int, value: String)

case class NullReflectData(
    intField: java.lang.Integer,
    longField: java.lang.Long,
    floatField: java.lang.Float,
    doubleField: java.lang.Double,
    booleanField: java.lang.Boolean)

case class OptionalReflectData(
    intField: Option[Int],
    longField: Option[Long],
    floatField: Option[Float],
    doubleField: Option[Double],
    booleanField: Option[Boolean])

case class Nested(i: Int, s: String)

case class Data(array: Seq[Int], nested: Nested)

class ParquetQuerySuite extends QueryTest with FunSuite with BeforeAndAfterAll {
  import TestData._
  TestData // Load test data tables.

  var testRDD: SchemaRDD = null

  override def beforeAll() {
    ParquetTestData.writeFile()
    ParquetTestData.writeFilterFile()
    ParquetTestData.writeNestedFile1()
    ParquetTestData.writeNestedFile2()
    testRDD = parquetFile(ParquetTestData.testDir.toString)
    testRDD.registerAsTable("testsource")
    parquetFile(ParquetTestData.testFilterDir.toString)
      .registerAsTable("testfiltersource")
  }

  override def afterAll() {
    Utils.deleteRecursively(ParquetTestData.testDir)
    Utils.deleteRecursively(ParquetTestData.testFilterDir)
    Utils.deleteRecursively(ParquetTestData.testNestedDir1)
    Utils.deleteRecursively(ParquetTestData.testNestedDir2)
    // here we should also unregister the table??
  }

  test("self-join parquet files") {
    val x = ParquetTestData.testData.as('x)
    val y = ParquetTestData.testData.as('y)
    val query = x.join(y).where("x.myint".attr === "y.myint".attr)

    // Check to make sure that the attributes from either side of the join have unique expression
    // ids.
    query.queryExecution.analyzed.output.filter(_.name == "myint") match {
      case Seq(i1, i2) if(i1.exprId == i2.exprId) =>
        fail(s"Duplicate expression IDs found in query plan: $query")
      case Seq(_, _) => // All good
    }

    val result = query.collect()
    assert(result.size === 9, "self-join result has incorrect size")
    assert(result(0).size === 12, "result row has incorrect size")
    result.zipWithIndex.foreach {
      case (row, index) => row.zipWithIndex.foreach {
        case (field, column) => assert(field != null, s"self-join contains null value in row $index field $column")
      }
    }
  }

  test("Import of simple Parquet file") {
    val result = parquetFile(ParquetTestData.testDir.toString).collect()
    assert(result.size === 15)
    result.zipWithIndex.foreach {
      case (row, index) => {
        val checkBoolean =
          if (index % 3 == 0)
            row(0) == true
          else
            row(0) == false
        assert(checkBoolean === true, s"boolean field value in line $index did not match")
        if (index % 5 == 0) assert(row(1) === 5, s"int field value in line $index did not match")
        assert(row(2) === "abc", s"string field value in line $index did not match")
        assert(row(3) === (index.toLong << 33), s"long value in line $index did not match")
        assert(row(4) === 2.5F, s"float field value in line $index did not match")
        assert(row(5) === 4.5D, s"double field value in line $index did not match")
      }
    }
  }

  test("Projection of simple Parquet file") {
    val scanner = new ParquetTableScan(
      ParquetTestData.testData.output,
      ParquetTestData.testData,
      Seq())(TestSQLContext.sparkContext)
    val projected = scanner.pruneColumns(ParquetTypesConverter
      .convertToAttributes(MessageTypeParser
      .parseMessageType(ParquetTestData.subTestSchema)))
    assert(projected.output.size === 2)
    val result = projected
      .execute()
      .map(_.copy())
      .collect()
    result.zipWithIndex.foreach {
      case (row, index) => {
          if (index % 3 == 0)
            assert(row(0) === true, s"boolean field value in line $index did not match (every third row)")
          else
            assert(row(0) === false, s"boolean field value in line $index did not match")
        assert(row(1) === (index.toLong << 33), s"long field value in line $index did not match")
        assert(row.size === 2, s"number of columns in projection in line $index is incorrect")
      }
    }
  }

  test("Writing metadata from scratch for table CREATE") {
    val job = new Job()
    val path = new Path(getTempFilePath("testtable").getCanonicalFile.toURI.toString)
    val fs: FileSystem = FileSystem.getLocal(ContextUtil.getConfiguration(job))
    ParquetTypesConverter.writeMetaData(
      ParquetTestData.testData.output,
      path,
      TestSQLContext.sparkContext.hadoopConfiguration)
    assert(fs.exists(new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)))
    val metaData = ParquetTypesConverter.readMetaData(path)
    assert(metaData != null)
    ParquetTestData
      .testData
      .parquetSchema
      .checkContains(metaData.getFileMetaData.getSchema) // throws exception if incompatible
    metaData
      .getFileMetaData
      .getSchema
      .checkContains(ParquetTestData.testData.parquetSchema) // throws exception if incompatible
    fs.delete(path, true)
  }

  test("Creating case class RDD table") {
    TestSQLContext.sparkContext.parallelize((1 to 100))
      .map(i => TestRDDEntry(i, s"val_$i"))
      .registerAsTable("tmp")
    val rdd = sql("SELECT * FROM tmp").collect().sortBy(_.getInt(0))
    var counter = 1
    rdd.foreach {
      // '===' does not like string comparison?
      row: Row => {
        assert(row.getString(1).equals(s"val_$counter"), s"row $counter value ${row.getString(1)} does not match val_$counter")
        counter = counter + 1
      }
    }
  }

  test("Saving case class RDD table to file and reading it back in") {
    val file = getTempFilePath("parquet")
    val path = file.toString
    val rdd = TestSQLContext.sparkContext.parallelize((1 to 100))
      .map(i => TestRDDEntry(i, s"val_$i"))
    rdd.saveAsParquetFile(path)
    val readFile = parquetFile(path)
    readFile.registerAsTable("tmpx")
    val rdd_copy = sql("SELECT * FROM tmpx").collect()
    val rdd_orig = rdd.collect()
    for(i <- 0 to 99) {
      assert(rdd_copy(i).apply(0) === rdd_orig(i).key,  s"key error in line $i")
      assert(rdd_copy(i).apply(1) === rdd_orig(i).value, s"value in line $i")
    }
    Utils.deleteRecursively(file)
  }

  test("insert (appending) to same table via Scala API") {
    sql("INSERT INTO testsource SELECT * FROM testsource")
    val double_rdd = sql("SELECT * FROM testsource").collect()
    assert(double_rdd != null)
    assert(double_rdd.size === 30)
    for(i <- (0 to 14)) {
      assert(double_rdd(i) === double_rdd(i+15), s"error: lines $i and ${i+15} to not match")
    }
    // let's restore the original test data
    Utils.deleteRecursively(ParquetTestData.testDir)
    ParquetTestData.writeFile()
  }

  test("save and load case class RDD with nulls as parquet") {
    val data = NullReflectData(null, null, null, null, null)
    val rdd = sparkContext.parallelize(data :: Nil)

    val file = getTempFilePath("parquet")
    val path = file.toString
    rdd.saveAsParquetFile(path)
    val readFile = parquetFile(path)

    val rdd_saved = readFile.collect()
    assert(rdd_saved(0) === Seq.fill(5)(null))
    Utils.deleteRecursively(file)
    assert(true)
  }

  test("save and load case class RDD with Nones as parquet") {
    val data = OptionalReflectData(null, null, null, null, null)
    val rdd = sparkContext.parallelize(data :: Nil)

    val file = getTempFilePath("parquet")
    val path = file.toString
    rdd.saveAsParquetFile(path)
    val readFile = parquetFile(path)

    val rdd_saved = readFile.collect()
    assert(rdd_saved(0) === Seq.fill(5)(null))
    Utils.deleteRecursively(file)
    assert(true)
  }

  test("create RecordFilter for simple predicates") {
    val attribute1 = new AttributeReference("first", IntegerType, false)()
    val predicate1 = new Equals(attribute1, new Literal(1, IntegerType))
    val filter1 = ParquetFilters.createFilter(predicate1)
    assert(filter1.isDefined)
    assert(filter1.get.predicate == predicate1, "predicates do not match")
    assert(filter1.get.isInstanceOf[ComparisonFilter])
    val cmpFilter1 = filter1.get.asInstanceOf[ComparisonFilter]
    assert(cmpFilter1.columnName == "first", "column name incorrect")

    val predicate2 = new LessThan(attribute1, new Literal(4, IntegerType))
    val filter2 = ParquetFilters.createFilter(predicate2)
    assert(filter2.isDefined)
    assert(filter2.get.predicate == predicate2, "predicates do not match")
    assert(filter2.get.isInstanceOf[ComparisonFilter])
    val cmpFilter2 = filter2.get.asInstanceOf[ComparisonFilter]
    assert(cmpFilter2.columnName == "first", "column name incorrect")

    val predicate3 = new And(predicate1, predicate2)
    val filter3 = ParquetFilters.createFilter(predicate3)
    assert(filter3.isDefined)
    assert(filter3.get.predicate == predicate3, "predicates do not match")
    assert(filter3.get.isInstanceOf[AndFilter])

    val predicate4 = new Or(predicate1, predicate2)
    val filter4 = ParquetFilters.createFilter(predicate4)
    assert(filter4.isDefined)
    assert(filter4.get.predicate == predicate4, "predicates do not match")
    assert(filter4.get.isInstanceOf[OrFilter])

    val attribute2 = new AttributeReference("second", IntegerType, false)()
    val predicate5 = new GreaterThan(attribute1, attribute2)
    val badfilter = ParquetFilters.createFilter(predicate5)
    assert(badfilter.isDefined === false)
  }

  test("test filter by predicate pushdown") {
    for(myval <- Seq("myint", "mylong", "mydouble", "myfloat")) {
      println(s"testing field $myval")
      val query1 = sql(s"SELECT * FROM testfiltersource WHERE $myval < 150 AND $myval >= 100")
      assert(
        query1.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
        "Top operator should be ParquetTableScan after pushdown")
      val result1 = query1.collect()
      assert(result1.size === 50)
      assert(result1(0)(1) === 100)
      assert(result1(49)(1) === 149)
      val query2 = sql(s"SELECT * FROM testfiltersource WHERE $myval > 150 AND $myval <= 200")
      assert(
        query2.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
        "Top operator should be ParquetTableScan after pushdown")
      val result2 = query2.collect()
      assert(result2.size === 50)
      if (myval == "myint" || myval == "mylong") {
        assert(result2(0)(1) === 151)
        assert(result2(49)(1) === 200)
      } else {
        assert(result2(0)(1) === 150)
        assert(result2(49)(1) === 199)
      }
    }
    for(myval <- Seq("myint", "mylong")) {
      val query3 = sql(s"SELECT * FROM testfiltersource WHERE $myval > 190 OR $myval < 10")
      assert(
        query3.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
        "Top operator should be ParquetTableScan after pushdown")
      val result3 = query3.collect()
      assert(result3.size === 20)
      assert(result3(0)(1) === 0)
      assert(result3(9)(1) === 9)
      assert(result3(10)(1) === 191)
      assert(result3(19)(1) === 200)
    }
    for(myval <- Seq("mydouble", "myfloat")) {
      val result4 =
        if (myval == "mydouble") {
          val query4 = sql(s"SELECT * FROM testfiltersource WHERE $myval > 190.5 OR $myval < 10.0")
          assert(
            query4.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
            "Top operator should be ParquetTableScan after pushdown")
          query4.collect()
        } else {
          // CASTs are problematic. Here myfloat will be casted to a double and it seems there is
          // currently no way to specify float constants in SqlParser?
          sql(s"SELECT * FROM testfiltersource WHERE $myval > 190.5 OR $myval < 10").collect()
        }
      assert(result4.size === 20)
      assert(result4(0)(1) === 0)
      assert(result4(9)(1) === 9)
      assert(result4(10)(1) === 191)
      assert(result4(19)(1) === 200)
    }
    val query5 = sql(s"SELECT * FROM testfiltersource WHERE myboolean = true AND myint < 40")
    assert(
      query5.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
      "Top operator should be ParquetTableScan after pushdown")
    val booleanResult = query5.collect()
    assert(booleanResult.size === 10)
    for(i <- 0 until 10) {
      if (!booleanResult(i).getBoolean(0)) {
        fail(s"Boolean value in result row $i not true")
      }
      if (booleanResult(i).getInt(1) != i * 4) {
        fail(s"Int value in result row $i should be ${4*i}")
      }
    }
    val query6 = sql("SELECT * FROM testfiltersource WHERE mystring = \"100\"")
    assert(
      query6.queryExecution.executedPlan(0)(0).isInstanceOf[ParquetTableScan],
      "Top operator should be ParquetTableScan after pushdown")
    val stringResult = query6.collect()
    assert(stringResult.size === 1)
    assert(stringResult(0).getString(2) == "100", "stringvalue incorrect")
    assert(stringResult(0).getInt(1) === 100)
  }

  test("SPARK-1913 regression: columns only referenced by pushed down filters should remain") {
    val query = sql(s"SELECT mystring FROM testfiltersource WHERE myint < 10")
    assert(query.collect().size === 10)
  }

  test("Importing nested Parquet file (Addressbook)") {
    implicit def anyToRow(value: Any): Row = value.asInstanceOf[Row]
    ParquetTestData.readNestedFile(
      ParquetTestData.testNestedFile1,
      ParquetTestData.testNestedSchema1)
    val result = getRDD(ParquetTestData.testNestedData1).collect()
    assert(result != null)
    assert(result.size === 2)
    val first_record = result(0)
    val second_record = result(1)
    val first_owner_numbers = result(0)(1)
    val first_contacts = result(0)(2)
    assert(first_record.size === 3)
    assert(second_record(1) === null)
    assert(second_record(2) === null)
    assert(second_record(0) === "A. Nonymous")
    assert(first_record(0) === "Julien Le Dem")
    assert(first_owner_numbers(0) === "555 123 4567")
    assert(first_owner_numbers(2) === "XXX XXX XXXX")
    assert(first_contacts(0).size === 2)
    assert(first_contacts(0)(0) === "Dmitriy Ryaboy")
    assert(first_contacts(0)(1) === "555 987 6543")
    assert(first_contacts(1)(0) === "Chris Aniszczyk")
  }

  test("Importing nested Parquet file (nested numbers)") {
    implicit def anyToRow(value: Any): Row = value.asInstanceOf[Row]
    ParquetTestData.readNestedFile(
      ParquetTestData.testNestedFile2,
      ParquetTestData.testNestedSchema2)
    val result = getRDD(ParquetTestData.testNestedData2).collect()
    assert(result.size === 1, "number of top-level rows incorrect")
    assert(result(0).size === 5, "number of fields in row incorrect")
    assert(result(0)(0) === 1)
    assert(result(0)(1) === 7)
    assert(result(0)(2).size === 3)
    assert(result(0)(2)(0) === (1.toLong << 32))
    assert(result(0)(2)(1) === (1.toLong << 33))
    assert(result(0)(2)(2) === (1.toLong << 34))
    assert(result(0)(3).size === 2)
    assert(result(0)(3)(0) === 2.5)
    assert(result(0)(3)(1) === false)
    assert(result(0)(4).size === 2)
    assert(result(0)(4)(0).size === 2)
    assert(result(0)(4)(1).size === 1)
    assert(result(0)(4)(0)(0)(0) === 7)
    assert(result(0)(4)(0)(1)(0) === 8)
    assert(result(0)(4)(1)(0)(0) === 9)
  }

  test("Simple query on addressbook") {
    val data = TestSQLContext.parquetFile(ParquetTestData.testNestedFile1.toString).toSchemaRDD
    val tmp = data.where('owner === "Julien Le Dem").select('owner as 'a, 'contacts as 'c).collect()
    assert(tmp.size === 1)
    assert(tmp(0)(0) === "Julien Le Dem")
  }

  test("Simple query on nested int data") {
    implicit def anyToRow(value: Any): Row = value.asInstanceOf[Row]
    val data = TestSQLContext.parquetFile(ParquetTestData.testNestedFile2.toString).toSchemaRDD
    data.registerAsTable("data")
    val tmp = sql("SELECT booleanNumberPairs.value, booleanNumberPairs.truth FROM data").collect()
    assert(tmp(0)(0) === 2.5)
    assert(tmp(0)(1) === false)
    val result = sql("SELECT outerouter FROM data").collect()
    // TODO: why does this not work?
    //val result = sql("SELECT outerouter.values FROM data").collect()
    // TODO: .. or this:
    // val result = sql("SELECT outerouter[0] FROM data").collect()
    assert(result(0)(0)(0)(0)(0) === 7)
    assert(result(0)(0)(0)(1)(0) === 8)
    assert(result(0)(0)(1)(0)(0) === 9)
  }

  /**
   * Creates an empty SchemaRDD backed by a ParquetRelation.
   *
   * TODO: since this is so experimental it is better to have it here and not
   * in SQLContext. Also note that when creating new AttributeReferences
   * one needs to take care not to create duplicate Attribute ID's.
   */
  private def createParquetFile(path: String, schema: (Tuple2[String, DataType])*): SchemaRDD = {
    val attributes = schema.map(t => new AttributeReference(t._1, t._2)())
    new SchemaRDD(
      TestSQLContext,
      parquet.ParquetRelation.createEmpty(path, attributes, sparkContext.hadoopConfiguration))
  }
}
