# goleveldb代码分析

> 官方：http://github.com/syndtr/goleveldb/leveldb

## 架构

 ![img](https://leveldb-handbook.readthedocs.io/zh/latest/_images/leveldb_arch.jpeg) 

- cache 缓存
- comparer 比较器
- errors 错误
- filter 过滤器
- iterator 迭代器
- journal 
- memdb 
- opt
- storage 存储
- table
- testutil 测试工具
- util 工具
- 其他

主要看以下几个部分

- cache
- comparer
- filter
- iterator
- journal
- memdb
- opt
- storage
- table
- util

选择一个切入点，因为样例db.PUT开始的，就从db_write.go开始吧

## db_write.go

