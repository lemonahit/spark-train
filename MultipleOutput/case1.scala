package com.zhaotao.SparkCoreHomework

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Spark文件输出到指定目录
  * 实现路程：
  *           1. 把key写入了指定文件内容
  *                 解决方法：
  *                     使用了key.asInstanceOf[String]
  *           2. 解决了key写入指定文件中的问题
  *                 解决方法：
  *                     重写generateActualKey方法
  *           3. 完成了多目录输出
  *                 解决方法：
  *                     将key.asInstanceOf[String]改成了(key + "/" + name)
  *
  * Created by 陶 on 2017/11/10.
  */
object case1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("case1").setMaster("local[2]")
    val sc = new SparkContext(conf)

    deleteFile(sc, "E:/multi_output")

    multipleOut(sc)

    sc.stop()
  }

  /**
    * 输出的结构为：
    *   /multi_output/deptno-10
    *               part-xxx
    *   /multi_output/deptno-20
    *               part-xxx
    *   /multi_output/deptno-30
    *               part-xxx
    *
    * @param sc
    */
  def multipleOut(sc:SparkContext): Unit ={
    sc.textFile("E:/data/emp1.txt").map(line => {
      val lines = line.split("\t")
      val key = "deptno-" + lines(7)
      val value = line
      (key, value)
    }).partitionBy(new HashPartitioner(3))    // 以key的hashCode值进行分区，将key相同的扔到一个分区里去。3个key，3个分区
//      .coalesce(3, true)                    // 合并为3个分区，每个分区内的数据不确定(deptno的值不确定)因此会进行拆分，每个文件夹下的文件数为2个      这种写法在这里不可以
      .saveAsHadoopFile("E:/multi_output",
                        classOf[String],
                        classOf[String],
                        classOf[RDDMultipleTextOutputFormat])
  }

  /**
    * 删除路径
    *
    * @param sc
    * @param path
    */
  def deleteFile(sc:SparkContext, path:String): Unit ={
    val deletePath = new Path(path)
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)
    if(hdfs.exists(deletePath)){
      hdfs.delete(deletePath,true)
    }

  }

}

/**
  * 多目录输出
  * 参考blog：http://blog.csdn.net/dkcgx/article/details/52637899
  */
class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  // 根据key指定输出的文件夹
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    key + "/" + name
  }

  // 输出时不输出key
  override def generateActualKey(key: Any, value: Any): Any = {
    NullWritable.get()
  }

}