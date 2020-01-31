## OptimizedApriori
在Spark平台上实现Apriori频繁项集挖掘的并行化算法，利用事务压缩优化`Apriori`算法

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
    --class  per.wzx.experiment4.Main \
    Experiment4-1.0.jar \
    <input> <output> <minSupport>
```
- `input`：输入数据路径
- `output`：输出路径
- `minSupport`：最小支持度