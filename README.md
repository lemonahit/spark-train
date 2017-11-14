## spark-train

### 目录结构说明

  Accumulator：案例一代码
  
  Broadcast：案例二代码 
  
  MultipleOutput：案例三代码
  
  data：测试数据
  

### 案例一：Spark Accumulator的使用

* 需求

  使用Accumulators统计emp表中NULL出现的次数以及正常数据的条数 & 打印正常数据的信息

* 数据

  参照/data/emp1.txt

* 遇到的坑 & 解决方法

	现象描述 & 原因分析：
  
	我们都知道，spark中的一系列transform操作会构成一串长的任务链，此时就需要通过一个action操作来触发；
	accumulator也是一样的，只有当action操作执行时，才会触发accumulator的执行；
	因此在一个action操作之前，我们调用accumulator的value方法是无法查看其数值的，肯定是没有任何变化的；
	所以在对normalData进行foreach操作之后，即action操作之后，我们会发现累加器的数值就变成了11；
	之后，我们对normalData再进行一次count操作之后，即又一次的action操作之后，其实这时候，又去执行了一次前面的transform操作；
	因此累加器的值又增加了11，变成了22
	
  解决办法：
  
	经过上面的分析，我们可以知道，使用累加器的时候，我们只有使用一次action操作才能够保证结果的准确性
	因此，我们面对这种情况，是有办法的，做法就是切断它们相互之间的依赖关系即可
	因此对normalData使用cache方法，当RDD第一次被计算出来时，就会被直接缓存起来
	再调用时，相同的计算操作就不会再重新计算一遍

### 案例二：Spark Broadcast的使用

* 需求

  使用Spark实现mapjoin & commonjoin

* 数据

  参照/data/emp1.txt

### 案例三：多目录输出 & 作业重跑

* 需求
  
  1. 按照分区信息进行多目录输出，每个分区下输出一个文件
  2. 在需求1的基础上，在某分区目录下输出多个文件
  3. 在需求2的基础上，实现数据的采样，获取造成数据倾斜的key
  4. 完成作业的重跑 & 对不同的数据进行追加

* 数据
  
  需求1、需求2参照/data/emp1.txt
  需求3参照/data/emp3.txt
  需求4参照/data/emp1.txt emp2.txt

* 核心思路

  1. 实现需求1的核心在于继承MultipleTextOutputFormat类，并重写generateFileNameForKeyValue与generateActualKey
     generateFileNameForKeyValue保证了按分区信息进行多目录输出
     generateActualKey保证了不将分区信息写入文件
  2. 实现需求2的核心在于巧妙的使用union算子
  3. 实现需求3的核心在于采样，spark为我们提供了算子sample
  4. 实现需求4的核心在于使用hadoop api中的rename方法，实现对新文件的追加、老文件的删除(即重跑机制)
     需求4为该案例核心，具体思路见代码中注释
