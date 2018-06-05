package com.zhaotao.ScalaFileOp

/**
  * Created by 陶 on 2017/11/4.
  */
object ScalaHDFSOp {

  def main(args: Array[String]) {
    val HDFSPath:String = "hdfs://192.168.26.131:8020/"
    val targetPath:String = HDFSPath + "/spark/emp/temp/201711112025/d=20171111/h=20/"
    val newTargetPath:String = HDFSPath + "/spark/emp/data/d="
    val deletePath:String = HDFSPath +"/spark/emp/data"
    val getLengthPath:String = "hdfs://192.168.26.131:8020/spark/emp/temp/"
    var n = 1

    try {
      val allFile = HDFSUtil.getFiles(HDFSPath, targetPath)

      // 判断路径是否存在 & 如果存在就删除
      if (HDFSUtil.exist(deletePath) == true) {
        HDFSUtil.delete(deletePath)
      }

      while (allFile.hasNext) {
        val file = allFile.next()
        if (!file.getPath.getName.contains("_SUCCESS")) {
          println("OldPath: " + file.getPath)

          val length = getLengthPath.length
          val dirs: Array[String] = file.getPath.toString.substring(length).split("/")
          val newOriginPath = newTargetPath + dirs(1).substring(4) + "/" + dirs(2)
          var fileName = dirs(0).substring(2).replace(dirs(2).substring(2), "")

          // 判断后缀大于10的文件
          if (n < 10) {
            fileName = fileName + "-" + "0" + n
          } else {
            fileName = fileName + "-" + n
          }

          val finalPath = newOriginPath + "/" + fileName + ".txt"
          println("finalPath: " + finalPath)
          HDFSUtil.mkdir(finalPath)
          n += 1
        }
      }
    }
    finally {
      HDFSUtil.close()
    }
  }
}