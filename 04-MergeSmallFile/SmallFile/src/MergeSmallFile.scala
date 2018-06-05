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
  * 思路：/file/minute=xxx  是存各个分区数据的路径
  *       /tmp/minute=xxx  是存合并之后文件的临时路径
  *      在正式开始合并之前会先做一个判断，将小于450字节的文件拿到，存放到/file/tmp_merge/minute=xxx下
  *           在移动之前会先统计合并之前的文件数目
  *      开始合并小文件的时候，就从/file/tmp_merge目录下读取，合并之后将文件存放到/tmp/minute=xxx/minute=xxx下
  *      之后再将/tmp/minute=xxx下合并的文件拿到/file/minute=xxx/minute=xxx下
  *           在该目录下统计完成操作之后的文件数目
  *      合并多少，通过一个机制去判断，在方法getCoalesceNum()中
  *           实现思路：
  *               先过滤得到小于450字节的所有文件，并计算这些文件的总大小A
  *               计算合并的文件数：A/1000 + 1  ==>  也就是说合出来的文件至少要比450大
  *
  *  重跑有问题，需要进行改进
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
    val coalesceNum = getCoalesceNum(fileSystem, "E:/file/tmp_merge/" + partitionInfo, 1000)
    println("合并" + coalesceNum)
    newMergeRDD.coalesce(coalesceNum).saveAsTextFile(tmpPath)

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

    val outPaths = SparkHadoopUtil.get.globPath(new Path(tmpPath + "/part-*"))
    outPaths.map(oldPath => {
      val finalPath = new Path(outPath)
      fileSystem.rename(oldPath, finalPath)
    })

    // 取得当前的年月日
    val now:Date = new Date()
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val dt = dateFormat.format( now )

    SparkHadoopUtil.get.globPath(new Path(outPath + "/part-*")).foreach(x => mergeAfterNum.add(1))

    // 取得合并文件之后，缩减的文件数
    println("partition:" + partitionInfo + "_" + dt + "_merge|成功缩减了" + (mergeBeforeNum - mergeAfterNum.value))
  }

  // 返回需要合并的个数
  def getCoalesceNum(fileSystem: FileSystem, filePath: String, coalesceSize: Int): Int = {
    var partitionNum = 0l
    // 获得合并路径下所有文件的总大小
    fileSystem.globStatus(new Path(filePath)).map(x => {
      partitionNum += x.getLen
      null
    })
    println("partitions: " + partitionNum)
    (partitionNum  / coalesceSize).toInt + 1
  }

}