package com.zhaotao.SparkCoreHomework

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 合并每个分区下的小文件
  *
  * Created by 陶 on 2017/11/16.
  */
object MergeSmallFile {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MergeSmallFile").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val hadoopConf = new Configuration()
//    val fileSystem = FileSystem.get(new URI("hdfs://192.168.26.131:8020"), hadoopConf)
    val fileSystem = FileSystem.get(hadoopConf)

    val partitionPath = "E:/file/minute=5/"
    val tmpPath = "E:/tmp/minute=5/"
    val outPath = "E:/file/minute=5/"
    val partitionInfo = "minute=5"

    mergeOut(sc, fileSystem, partitionPath, tmpPath, outPath, partitionInfo)

    sc.stop()
  }

  def mergeOut(sc: SparkContext, fileSystem: FileSystem, partitionPath: String, tmpPath: String, outPath: String, partitionInfo:String): Unit ={
    val mergeBeforeNum = sc.longAccumulator("mergeBeforeNum")
    val path = SparkHadoopUtil.get.globPath(new Path(partitionPath + "/part-*"))

    path.foreach(paths => {
      // 统计合并之前有多少文件
      mergeBeforeNum.add(1)
    })

    // 过滤之后，得到小于450字节的文件，得到合并路径
    val mergePath = path.filter(paths => {
      fileSystem.getContentSummary(paths).getLength < 450
    })

    val partitions = new mutable.HashSet[String]()
    val tmpMergePath = new Path("E:/file/tmp_merge/" + partitionInfo)
    if (fileSystem.exists(tmpMergePath)) {
      fileSystem.delete(tmpMergePath, true)
    }
    fileSystem.mkdirs(tmpMergePath)
    // 将过滤之后的小文件存放至新的路径：E:/file/tmp_merge/minute=x 下
    mergePath.foreach(mergePaths => {
      // 将需要 合并的小文件 的路径保存到可变的HashSet中去
      // E:/file/minute=10/part-xxxx
      println(mergePaths)
      partitions.add(mergePaths.toString)
      fileSystem.rename(mergePaths, tmpMergePath)
    })

    val newMergeRDD = sc.textFile("E:/file/tmp_merge/" + partitionInfo)
    deleteDir(tmpPath, fileSystem)
    newMergeRDD.foreach(println)
    newMergeRDD.coalesce(4).saveAsTextFile(tmpPath)

    val num = mergeBeforeNum.value
    println("合并之前的文件数" + num)

    replaceTmpFile(sc, fileSystem, partitions, tmpPath, outPath, num, partitionInfo)
  }

  def deleteDir(tmpPath: String, fileSystem: FileSystem): Unit ={
    if (fileSystem.exists(new Path(tmpPath))) {
      fileSystem.delete(new Path(tmpPath), true)
    }
  }

  def replaceTmpFile(sc: SparkContext, fileSystem: FileSystem, partitions: mutable.HashSet[String], tmpPath: String, outPath: String, mergeBeforeNum: Long, partitionInfo:String): Unit ={
    val mergeAfterNum = sc.longAccumulator("mergeAfterNum")

    partitions.foreach(partition => {
//      println(partition)
      // 删除分区目录下，需要进行合并的小文件
//      SparkHadoopUtil.get.globPath(new Path(outPath + "/part-*")).map(fileSystem.delete(_, false))
      SparkHadoopUtil.get.globPath(new Path(partition)).map(fileSystem.delete(_, false))
    })

    val outPaths = SparkHadoopUtil.get.globPath(new Path(tmpPath + "/part-*"))
    outPaths.map(oldPath => {
      val finalPath = new Path(outPath)
//        if (!fileSystem.exists(finalPath)) {
//          fileSystem.mkdirs(finalPath)
//        }
      fileSystem.rename(oldPath, finalPath)
    })

    SparkHadoopUtil.get.globPath(new Path(outPath + "/part-*")).foreach(outPaths => mergeAfterNum.add(1))

    // 取得当前的年月日
    val now:Date = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val dt = dateFormat.format( now )

    // 取得合并文件之后，缩减的文件数
    println("partition:" + partitionInfo + "_" + dt + "_merge|成功缩减了" + (mergeBeforeNum - mergeAfterNum.value))
  }

}
