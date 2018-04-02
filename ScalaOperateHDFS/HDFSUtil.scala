package com.zhaotao.ScalaFileOp

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

/**
  * Created by 陶 on 2017/11/4.
  */
object HDFSUtil {
  val conf: Configuration = new Configuration()
  var fs: FileSystem = null
  var files: RemoteIterator[LocatedFileStatus] = null

  def getFiles(HDFSPath: String, targetPath: String) = {
    try {
      fs = FileSystem.get(new URI(HDFSPath), conf)
      // 返回指定路径下的所有文件
      files = fs.listFiles(new Path(targetPath), false)
    } catch {
      case e: IOException => {
        e.printStackTrace
      }
    }
    files
  }

  def mkdir(finalPath:String) = {
    fs.create(new Path(finalPath))
  }

  def exist(existPath:String):Boolean = {
    fs.exists(new Path(existPath))
  }

  def delete(deletePath:String) = {
    fs.delete(new Path(deletePath), true)
  }

  def close() =  {
    try {
      if (fs != null) {
        fs.close
      }
    }
    catch {
      case e: IOException => {
        e.printStackTrace
      }
    }
  }
}
