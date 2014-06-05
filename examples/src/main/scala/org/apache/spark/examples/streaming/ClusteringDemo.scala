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

package org.apache.spark.examples.streaming

import twitter4j.auth.{Authorization, OAuthAuthorization}
import twitter4j.conf.ConfigurationBuilder

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.util.IntParam

object ClusteringDemo {

  def main(args: Array[String]) {

    // Process program arguments and set properties
    if (args.length < 2) {
      System.err.println("Usage: " + this.getClass.getSimpleName + " <model file>  <cluster number>")
      System.exit(1)
    }

    val Array(modelFile, IntParam(clusterNumber)) = args

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(conf, Seconds(1))

    val model = new KMeansModel(ssc.sparkContext.objectFile[Vector](modelFile).collect())
    println("Initializing Twitter stream...")
    val tweets = TwitterUtils.createStream(ssc, getAuth())
    val statuses = tweets.map(_.getText)
    val filteredTweets = statuses.filter(t => model.predict(featurize(t)) == clusterNumber)
    filteredTweets.print()

    // Start the streaming computation
    println("Initialization complete.")
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Create feature vectors by turning each tweet into bigrams of characters (an n-gram model)
   * and then hashing those to a length-1000 feature vector that we can pass to MLlib.
   * This is a common way to decrease the number of features in a model while still
   * getting excellent accuracy (otherwise every pair of Unicode characters would
   * potentially be a feature).
   */
  def featurize(s: String): Vector = {
    val n = 1000
    val result = new Array[Double](n)
    val bigrams = s.sliding(2).toArray
    for (h <- bigrams.map(_.hashCode % n)) {
      result(h) += 1.0 / bigrams.length
    }
    Vectors.sparse(n, result.zipWithIndex.filter(_._1 != 0).map(_.swap))
  }

  // SECRET AUTH DETAILS BELOW... SHHHHHH
  def getAuth(): Option[Authorization] = {
    System.setProperty("twitter4j.oauth.consumerKey", "XMazoFOU3CElmwzhK4zUFus0X")
    System.setProperty("twitter4j.oauth.consumerSecret", "RlBzH3JXRY3i1dwIxLPJDErhtFzQKt4h1iWqS4bgjCY71TP6m6")
    System.setProperty("twitter4j.oauth.accessToken", "1669370527-QpHCsRBpQdnGXpfPvo5MlWjf0IyEnpugtORtstH")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "QkalVAyXYMiVnMTJ5WtzSzQVM5vQEJwaKoi9M7Fyu7XKe")
    Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  }
}
