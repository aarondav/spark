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

package org.apache.spark.sql.hive.formats

import java.util.Properties

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.FileSinkOperator
import org.apache.hadoop.hive.ql.io.{HiveInputFormat, HiveOutputFormat}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{RecordWriter, JobConf}
import org.apache.hadoop.util.Progressable


class FakeInputFormat extends HiveInputFormat[Nothing, Nothing]

class FakeOutputFormat extends HiveOutputFormat[Nothing, Nothing] {
  override def getHiveRecordWriter(
      jc: JobConf, finalOutPath: Path, valueClass: Class[_ <: Writable],
      isCompressed: Boolean, tableProperties: Properties, progress: Progressable): FileSinkOperator.RecordWriter = {
    throw new UnsupportedOperationException
  }

  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
    throw new UnsupportedOperationException
  }

  override def getRecordWriter(ignored: FileSystem, job: JobConf, name: String, progress: Progressable): RecordWriter[Nothing, Nothing] = {
    throw new UnsupportedOperationException
  }
}

class ParquetInputFormat extends FakeInputFormat
class ParquetOutputFormat extends FakeOutputFormat
