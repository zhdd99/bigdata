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

package org.apache.spark.examples;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public final class JavaInvertedIndex {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaInvertedIndex")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaPairRDD<String, String> jpr =
                jsc.wholeTextFiles("/Users/zhdd99/KuaishouProjects/bigdata/spark/examples/input1");

        JavaRDD<Tuple2<String, String>> word2FileName =
                jpr.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(Tuple2<String, String> file) throws Exception {
                        String fileName = file._1().substring(file._1().length() - 1);
                        return Arrays.stream(file._2().split("\n")).flatMap(line -> Arrays.stream(line.split(" ")))
                                .map(word -> new Tuple2<>(word, fileName)).collect(
                                        Collectors.toList()).iterator();
                    }
                });

        JavaPairRDD<Tuple2<String, String>, Integer> word2FilenameAndCount =
                word2FileName.mapToPair(new PairFunction<Tuple2<String, String>, Tuple2<String, String>, Integer>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, String> word2File)
                            throws Exception {
                        return new Tuple2<>(new Tuple2<>(word2File._1(), word2File._2()), 1);
                    }
                });
        JavaPairRDD<Tuple2<String, String>, Integer> word2FilenameAndCountSum =
                word2FilenameAndCount.reduceByKey(Integer::sum);

        JavaPairRDD<String, Set<Tuple2<String, Integer>>> wordAndFilename2Count =
                word2FilenameAndCountSum.mapToPair(
                        new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String,
                                Set<Tuple2<String, Integer>>>() {
                            @Override
                            public Tuple2<String, Set<Tuple2<String, Integer>>> call(
                                    Tuple2<Tuple2<String, String>, Integer> item) throws Exception {
                                Set<Tuple2<String, Integer>> tuple2s = new CopyOnWriteArraySet<>();
                                tuple2s.add(new Tuple2<>(item._1()._2(), item._2()));
                                return new Tuple2<>(item._1()._1(), tuple2s);
                            }
                        });

        JavaPairRDD<String, Set<Tuple2<String, Integer>>> wordAndFilename2CountSum = wordAndFilename2Count.reduceByKey(
                new Function2<Set<Tuple2<String, Integer>>, Set<Tuple2<String, Integer>>,
                        Set<Tuple2<String, Integer>>>() {
                    @Override
                    public Set<Tuple2<String, Integer>> call(Set<Tuple2<String, Integer>> i1,
                            Set<Tuple2<String, Integer>> i2)
                            throws Exception {
                        i1.addAll(i2);
                        return i1;
                    }
                });

        wordAndFilename2CountSum.saveAsTextFile("/Users/zhdd99/KuaishouProjects/bigdata/spark/examples/output1");
        spark.stop();
    }
}
