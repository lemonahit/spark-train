### 多目录输出  & 作业重跑

* 需求

  1. 按照分区信息进行多目录输出，每个分区下输出一个文件  
  2. 在需求1的基础上，在某分区目录下输出多个文件
  3. 在需求2的基础上，实现数据的采样，获取造成数据倾斜的key  
  4. 完成作业的重跑 & 对不同的数据进行追加
  

* 数据
  
  需求1、需求2参照spark-train/data/emp1.txt
  
  需求3参照spark-train/data/emp3.txt
  
  需求4参照spark-train/data/emp1.txt emp2.txt
  

* 核心思路

  1. 实现需求1的核心在于继承MultipleTextOutputFormat类，并重写generateFileNameForKeyValue与generateActualKey

     - generateFileNameForKeyValue保证了按分区信息进行多目录输出   

     - generateActualKey保证了不将分区信息写入文件    

  2. 实现需求2的核心在于巧妙的使用union算子 

  3. 实现需求3的核心在于采样，spark为我们提供了算子sample  
  
  4. 实现需求4的核心在于使用hadoop api中的rename方法，实现对新文件的追加、老文件的删除(即重跑机制)  
     需求4为该案例核心，具体思路见代码中注释