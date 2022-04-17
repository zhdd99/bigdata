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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public final class JavaSparkDistCp {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkDistCp")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Configuration conf = new Configuration();
        FileSystem local =FileSystem.getLocal(conf);
        List<Tuple2<Path, Path>> fileList = new ArrayList<>();
        makeDirs(local, new Path("/Users/zhdd99/KuaishouProjects/bigdata/spark/examples/homework2/source"),
                    new Path("/Users/zhdd99/KuaishouProjects/bigdata/spark/examples/homework2/target"), fileList);
        copy(jsc, fileList);
        spark.stop();
    }

    public static void makeDirs(FileSystem fs, Path sourcePath, Path targetPath, List<Tuple2<Path, Path>> fileList) throws Exception {
        for (FileStatus currPath : fs.listStatus(sourcePath)) {
            if (currPath.isDirectory()) {
                String subPath = currPath.getPath().toString().split(sourcePath.toString())[1];
                Path nextTargetPath = new Path(targetPath + subPath);
                fs.mkdirs(nextTargetPath);
                makeDirs(fs, currPath.getPath(), nextTargetPath, fileList);
            } else {
                fileList.add(new Tuple2<>(currPath.getPath(), targetPath));
            }
        }
    }

    public static void copy(JavaSparkContext jsc, List<Tuple2<Path, Path>> fileList) {
        JavaRDD<Tuple2<Path, Path>> fileJobRdds = jsc.parallelize(fileList, 10);
        fileJobRdds.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Path, Path>>, Tuple2<Path, Path>>() {
            @Override
            public Iterator<Tuple2<Path, Path>> call(Iterator<Tuple2<Path, Path>> ite) throws Exception {
                ite.forEachRemaining(tup -> {
                    try {
                        Configuration conf = new Configuration();
                        FileUtil.copy(tup._1().getFileSystem(conf), tup._1(),
                                tup._2().getFileSystem(conf), tup._2(), false, conf);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                return ite;
            }
        }).collect();

    }
}
