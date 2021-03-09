## Demo
- 新增了 `app-test.cc` 和 `level-shell.cc`
- 修改 CMakeLists.txt
```
[root@ceph-node1 leveldb]# git diff
diff --git a/CMakeLists.txt b/CMakeLists.txt
index f8285b8..0b44a07 100644
--- a/CMakeLists.txt
+++ b/CMakeLists.txt
@@ -350,6 +350,8 @@ if(LEVELDB_BUILD_TESTS)
   leveldb_test("util/no_destructor_test.cc")
 
   if(NOT BUILD_SHARED_LIBS)
+    leveldb_test("app/app_test.cc")
+    leveldb_test("app/level-shell.cc")
     leveldb_test("db/autocompact_test.cc")
     leveldb_test("db/corruption_test.cc")
     leveldb_test("db/db_test.cc")
```
### app_test
- 一些简单流程测试

### level-shell
- 一些简单操作的命令交互
- 示例如下：
```cmd
[root@ceph-node1 build]# ./level-shell test
>>> put key1 value1
OK
>>> put key2 v2
OK
>>> scan
invalid operate
>>> scan "" ""
key1:value1
key2:v2
>>> get key1
value1
>>> put l1 v1
OK
>>> scan "" ""
key1:value1
key2:v2
l1:v1
>>> scan "k" "k"
>>> scan "" "v1"
key1:value1
key2:v2
l1:v1
>>> scan "k" "v"
key1:value1
key2:v2
l1:v1
>>> scan "key2" "v"
key1:value1
key2:v2
l1:v1
>>> put m q
OK
```