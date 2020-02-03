## OptimizedApriori
在Spark平台上实现Apriori频繁项集挖掘的并行化算法，利用**事务压缩和布尔矩阵**优化`Apriori`算法，[更多详细信息](https://masterwangzx.com/2020/01/31/apriori/)

### 测试数据
http://fimi.ua.ac.be/data

### 运行环境
- `Java`:JDK8
- `Scala`:2.10.12
- `Spark`:2.4.4
- `HDFS`:2.7.1

### 下载与编译
```
git clone git@github.com:wzx140/OptimizedApriori.git
cd OptimizedApriori
mvn package
```
### 运行
```shell
spark-submit \
    --master <master_url> \
    --class  per.wzx.ar.Main \
    OptimizedApriori-1.0.jar \
    <input> <output> <minSupport>
```
- `input`：输入数据路径
    - 输入数据格式：每一行对应一个事务，事务项之间用空格分隔
- `output`：输出路径
    - 输出数据格式：每一行对应一个频繁项集，频繁项集和对应支持度用`tab`分隔
- `minSupport`：最小支持度
