<div align="center">
<strong>
<samp>

[English](https://github.com/kebukeYi/TrainSet/blob/main/README.md) · [简体中文](https://github.com/kebukeYi/TrainSet/blob/main/README_zh.md)

</samp>
</strong>
</div>


# TrainSet
[![C++](https://img.shields.io/badge/C++17-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

TrainSet 是一个用 C++ 实现的高性能、兼容 Redis 的内存数据库。它提供键值存储，支持多种数据类型、持久化机制（AOF 和 RDB）以及主从复制功能。

## 功能特性

- **多种数据类型**: 支持字符串、哈希和有序集合（zsets）
- **Redis 协议**: 兼容 RESP（Redis 序列化协议）
- **持久化选项**:
  - AOF（仅追加文件）支持三种模式：Always、EverySec 和 None
  - RDB（Redis 数据库）快照
- **主从复制**: 支持节点间数据复制
- **线程安全操作**: 并发安全的数据结构
- **可配置设置**: 从文件或命令行加载配置
- **内存高效**: 优化内存使用，支持过期功能

## 架构

### 核心组件

1. **服务器**: 使用 epoll 进行 I/O 多路复用的主服务器实现
2. **KV 存储**: 支持不同数据类型的内存键值存储
3. **AOF 管理器**: 处理具有各种同步模式的仅追加文件持久化
4. **RDB 管理器**: 以 RDB 格式管理数据库快照
5. **RESP 解析器**: 处理用于客户端通信的 Redis 序列化协议
6. **配置加载器**: 从文件或命令行参数解析配置

### 支持的数据类型

- **字符串**: 具有过期支持的简单键值对
- **哈希**: 存储在单个键下的键值映射
- **有序集合（ZSets）**: 按分数排序的唯一成员集合，使用跳表实现

## 配置

服务器可以通过命令行参数或配置文件进行配置：

### 命令行选项

```bash
./train_set [--port <端口>] [--bind <IP>] [--config <文件>]
```

### 配置文件选项

- `port`: 服务器端口（默认：6379）
- `bind_address`: 绑定的 IP 地址（默认：127.0.0.1）
- `aof.enabled`: 启用 AOF 持久化（true/false）
- `aof.mode`: AOF 同步模式（always/everysec/none）
- `aof.dir`: AOF 文件目录
- `aof.filename`: AOF 文件名
- `rdb.enabled`: 启用 RDB 持久化（true/false）
- `rdb.dir`: RDB 文件目录
- `rdb.filename`: RDB 文件名
- `replicate.enabled`: 启用复制（true/false）
- `replicate.master_host`: 复制的主节点主机
- `replicate.master_port`: 复制的主节点端口

## 支持的命令

### 字符串操作
- `SET key value [EX seconds]` - 设置键值对
- `GET key` - 通过键获取值
- `DEL key1 [key2 ...]` - 删除一个或多个键
- `EXISTS key` - 检查键是否存在
- `TTL key` - 获取键的生存时间
- `EXPIRE key seconds` - 设置过期时间

### 哈希操作
- `HSET key field value` - 在哈希中设置字段
- `HGET key field` - 从哈希中获取字段值
- `HDEL key field1 [field2 ...]` - 从哈希中删除字段
- `HEXISTS key field` - 检查字段是否存在于哈希中
- `HLEN key` - 获取哈希中的字段数量
- `HGETALL key` - 获取哈希中的所有字段和值

### 有序集合操作
- `ZADD key score member` - 向有序集合添加成员
- `ZREM key member [member ...]` - 从有序集合中删除成员
- `ZRANGE key start stop` - 按排名获取成员范围
- `ZSCORE key member` - 获取有序集合中成员的分数

### 持久化命令
- `SAVE` - 将数据保存到 RDB 文件
- `BGSAVE` - 在后台将数据保存到 RDB 文件
- `BGREWRITEAOF` - 在后台重写 AOF 文件

### 服务器命令
- `PING` - ping 服务器
- `INFO` - 获取服务器信息
- `FLUSHALL` - 删除所有键
- `KEYS *` - 列出匹配模式的键

## 构建和运行

### 先决条件

- C++17 兼容编译器
- CMake 3.15 或更高版本
- POSIX 兼容系统（Linux）

### 构建说明

```bash
mkdir build
cd build
cmake ..
make
```

### 使用配置运行

```bash
# 使用配置文件
./train_set --config ../conf/always.conf

# 使用命令行参数
./train_set --port 6380 --bind 127.0.0.1
```

### 示例配置文件

仓库包含多个示例配置文件：
- `always.conf`: 启用 AOF 并使用 "always" 同步模式
- `everysec.conf`: 启用 AOF 并使用 "every second" 同步模式
- `none.conf`: 禁用 AOF
- `master.conf`: 主节点配置
- `replica.conf`: 从节点配置

## 性能特性

- **高效 AOF 实现**: 多种同步模式，平衡性能和持久性
- **批量写入**: AOF 支持可配置的批量大小以提高写入性能
- **跳表**: 有序集合使用跳表实现，提供高效的范围操作
- **内存预分配**: AOF 文件可以预分配以减少碎片
- **后台操作**: AOF 重写和 RDB 保存在后台线程中运行

## 复制

TrainSet 支持主从复制：
- 从节点可以连接到主节点以接收数据更新
- 初始同步通过 RDB 转储完成
- 增量更新作为命令通过复制通道发送

## 测试

项目包含对所有主要组件的全面单元测试：
- AOF 功能测试
- RDB 持久化测试
- 键值存储测试
- 配置加载测试
- RESP 协议解析测试
