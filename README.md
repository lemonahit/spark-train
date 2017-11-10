## spark-train
### 案例一：Spark Accumulator的使用

* 需求

1. 使用Spark Accumulators完成Job的数据量处理
2. 统计emp表中NULL出现的次数以及正常数据的条数 & 打印正常数据的信息

* 数据

  7369	SMITH	CLERK	7902	1980-12-17	800.00		20</br>
  7499	ALLEN	SALESMAN	7698	1981-2-20	1600.00	300.00	30</br>
  7521	WARD	SALESMAN	7698	1981-2-22	1250.00	500.00	30</br>
  7566	JONES	MANAGER	7839	1981-4-2	2975.00		20</br>
  7654	MARTIN	SALESMAN	7698	1981-9-28	1250.00	1400.00	30</br>
  7698	BLAKE	MANAGER	7839	1981-5-1	2850.00		30</br>
  7782	CLARK	MANAGER	7839	1981-6-9	2450.00		10</br>
  7788	SCOTT	ANALYST	7566	1987-4-19	3000.00		20</br>
  7839	KING	PRESIDENT		1981-11-17	5000.00		10</br>
  7844	TURNER	SALESMAN	7698	1981-9-8	1500.00	0.00	30</br>
  7876	ADAMS	CLERK	7788	1987-5-23	1100.00		20</br>
  7900	JAMES	CLERK	7698	1981-12-3	950.00		30</br>
  7902	FORD	ANALYST	7566	1981-12-3	3000.00		20</br>
  7934	MILLER	CLERK	7782	1982-1-23	1300.00		10</br>

* 遇到的坑

	现象描述：
  
	我们都知道，spark中的一系列transform操作会构成一串长的任务链，此时就需要通过一个action操作来触发；
	accumulator也是一样的，只有当action操作执行时，才会触发accumulator的执行；
	因此在一个action操作之前，我们调用accumulator的value方法是无法查看其数值的，肯定是没有任何变化的
	所以在对normalData进行foreach操作之后，即action操作之后，我们会发现累加器的数值就变成了11
	之后，我们对normalData再进行一次count操作之后，即又一次的action操作之后，其实这时候，又去执行了一次前面的transform操作
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

  参照案例一的emp表
