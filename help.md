## lab1 test

cd build
make extendible_hash_table_test -j$(nproc)
./test/extendible_hash_table_test

-j$(nproc)表示使用多线程进行编译
可以在测试方法的前面加上 DISABLED\_ 进行跳过
