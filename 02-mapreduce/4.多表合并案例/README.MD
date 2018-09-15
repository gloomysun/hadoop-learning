订单数据表t_order

| id  | pid   |  amount  |
| -----   | -----  | ----  |
|1001|	01|	1|
|1002|	02|	2|
|1003|	03|	3|

商品信息表t_product

| id        | pname   |
| -----   | -----  |
|01|	小米|	
|02|	华为|	
|03|	格力|	

最终数据形式：

| id  | pname   |  amount  |
| -----   | -----  | ----  |
|1001|	小米|	1|
|1002|	华为|	2|
|1003|	格力|	3|

###  方案1：reduce端表合并（数据倾斜）
通过将关联条件作为map输出的key，将两表满足join条件的数据并携带数据所来源的文件信息，发往同一个reduce task，在reduce中进行数据的串联。
![reduce端表合并](https://github.com/gloomysun/hadoop-learning/blob/master/02-mapreduce/4.%E5%A4%9A%E8%A1%A8%E5%90%88%E5%B9%B6%E6%A1%88%E4%BE%8B/source/reducer%E7%AB%AF%E5%90%88%E5%B9%B6.png)
### 方案2：map端表合并（Distributedcache）

![map端表合并](https://github.com/gloomysun/hadoop-learning/blob/master/02-mapreduce/4.%E5%A4%9A%E8%A1%A8%E5%90%88%E5%B9%B6%E6%A1%88%E4%BE%8B/source/map%E7%AB%AF%E5%90%88%E5%B9%B6.png)

适用于关联表中有小表的情形；  
可以将小表分发到所有的map节点，这样，map节点就可以在本地对自己所读到的大表数据进行合并并输出最终结果，可以大大提高合并操作的并发度，加快处理速度。
  