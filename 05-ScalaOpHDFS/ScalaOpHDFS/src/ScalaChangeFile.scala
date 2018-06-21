package com.zhaotao.ScalaFileOp

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil

/**
  * Created by 陶 on 2018/6/21.
  */
object ScalaChangeFile {
  def main(args: Array[String]): Unit = {
    val conf=new Configuration()
    conf.set("fs.default.name","hdfs://192.168.31.89:8020")
    val outputPath="hdfs://192.168.31.89:8020/spark/emp/"
    val loadTime="201711112025"
    val partition="/d=20171111/h=20"
    val fileSystem=FileSystem.get(URI.create(outputPath + "temp/" + loadTime + partition),conf)
    changeFileName(fileSystem,outputPath,loadTime,partition)
  }

  def changeFileName(fileSystem: FileSystem, outputPath: String, loadTime: String, partition: String): Unit = {
    //返回一个Path序列（hdfs://192.168.31.89:8020/spark/emp/temp/201711112025/d=20171111/h=20/*.txt）
    val paths = SparkHadoopUtil.get.globPath(new Path(outputPath + "temp/" + loadTime + partition + "/*.txt"))
    var times = 0
    paths.map(x => {
      //将hdfs://192.168.31.89:8020/spark/emp/temp/201711112025替换成hdfs://192.168.31.89:8020/spark/emp/data/
      //hdfs://192.168.31.89:8020/spark/emp/data/d=20171111/h=20/part-r-00002-6ba69620-ba52-4cb1-9ea0-6634ae0e16bc.txt
      var newLocation = x.toString.replace(outputPath + "temp/" + loadTime, outputPath + "data/")
      println("1:"+newLocation)
      //hdfs://192.168.31.89:8020/spark/emp/data/d=20171111/h=20/00002-6ba69620-ba52-4cb1-9ea0-6634ae0e16bc.txt
      newLocation = newLocation.replace("part-r-", "")
      //获取最后一个"/"的索引
      val index = newLocation.lastIndexOf("/")
      times += 1
      //hdfs://192.168.31.89:8020/spark/emp/data/d=20171111/h=20/201711112025-times.txt
      newLocation = newLocation.substring(0, index + 1) + loadTime + "-" + times + ".txt"
      println("2:"+newLocation)
      val officialPath = new Path(newLocation)
      //如果hdfs://192.168.31.89:8020/spark/emp/data/d=20171111/h=20/不存在，就创建它
      if (!fileSystem.exists(officialPath.getParent)) {
        fileSystem.mkdirs(officialPath.getParent)
      }
      fileSystem.rename(x, officialPath)
    })
  }
}
