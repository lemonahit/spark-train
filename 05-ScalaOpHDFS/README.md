### Scala操作HDFS文件系统

* 需求

  HDFS路径改变的过程：

  ```
  /spark/emp/temp/201711112025/d=20171111/h=20/_SUCCESS  空文件
  /spark/emp/temp/201711112025/d=20171111/h=20/part-r-00000-6ba69620-ba52-4cb1-9ea0-6634ae0e16bc.txt
  /spark/emp/temp/201711112025/d=20171111/h=20/part-r-00001-6ba69620-ba52-4cb1-9ea0-6634ae0e16bc.txt
  ...
  
  ===>

  /spark/emp/data/d=171111/h=20/17111125-01.txt
  /spark/emp/data/d=171111/h=20/17111125-02.txt
  ...
  ```

* 代码版本

  **v1.0**
  
  HDFSUtil.scala
  
  ScalaHDFSOp.scala
  
  **v2.0**
  
  ScalaChanegFile.scala
