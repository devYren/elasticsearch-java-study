# Elasticsearch Java API 学习项目

这是一个用于学习和实践 Elasticsearch Java API 的项目，基于 Elasticsearch Java Client 8.14.3 版本开发。

## 项目简介

本项目提供了 Elasticsearch 的基本操作示例，包括索引管理、文档操作、搜索查询等功能，适合初学者学习 Elasticsearch Java API 的使用。

## 技术栈

- **Java**: 8
- **Elasticsearch Java Client**: 8.14.3
- **Jackson**: 2.17.0 (JSON 处理)
- **Log4j**: 2.8.2 (日志记录)
- **Lombok**: 1.18.32 (简化代码)
- **JUnit**: 4.12 (单元测试)

## 项目结构

```
src/
├── main/
│   ├── java/
│   │   └── com/study/elasticsearch/
│   │       ├── Main.java              # 主程序入口
│   │       ├── UserEntity.java        # 用户实体类
│   │       ├── IndexService.java      # 索引操作服务
│   │       └── DocumentService.java   # 文档操作服务
│   └── resources/
└── test/
    └── java/
```

## 主要功能

### 1. 索引操作 (IndexService)
- 创建索引
- 查询索引信息
- 删除索引

### 2. 文档操作 (DocumentService)
- **基础操作**
  - 创建单个文档
  - 批量创建文档
  - 获取文档
  - 更新文档
  - 删除单个文档
  - 批量删除文档

- **搜索功能**
  - 全文搜索
  - 查询所有文档
  - 分页查询
  - 排序查询
  - 多条件查询
  - 范围查询
  - 模糊查询
  - 高亮搜索
  - 聚合查询（最大值、分组）

## 快速开始

### 前置条件

1. 确保已安装 Java 8 或更高版本
2. 确保 Elasticsearch 服务正在运行（默认地址：http://localhost:9200）

### 运行项目

1. 克隆项目到本地
```bash
git clone <repository-url>
cd study_elasticsearch
```

2. 使用 Maven 编译项目
```bash
mvn clean compile
```

3. 运行主程序
```bash
mvn exec:java -Dexec.mainClass="com.study.elasticsearch.Main"
```

### 配置说明

在 `Main.java` 中可以配置 Elasticsearch 连接信息：

```java
public static final String server_Url = "http://localhost:9200";
public static final String API_KEY = ""; // 如需要可配置 API Key
```

## 使用示例

### 创建索引
```java
IndexService indexService = new IndexService(client);
indexService.creatIndex("user");
```

### 添加文档
```java
DocumentService documentService = new DocumentService(client);
UserEntity user = new UserEntity();
user.setId(1).setName("张三").setAge(25).setSex("男");
documentService.creatDocument("user", user);
```

### 搜索文档
```java
// 全文搜索
documentService.searchDocument("user", "name", "张三");

// 分页查询
documentService.searchDocumentPage("user", 0, 10);

// 范围查询
documentService.searchDocRangeByIndex("user", "age", "20", "30");
```

## 学习要点

1. **连接管理**: 学习如何创建和配置 ElasticsearchClient
2. **索引操作**: 掌握索引的创建、查询、删除操作
3. **文档操作**: 理解文档的 CRUD 操作
4. **搜索查询**: 学习各种搜索方式和查询条件
5. **批量操作**: 了解批量操作的最佳实践
6. **聚合查询**: 掌握聚合统计功能

## 注意事项

- 批量操作时，建议单个批量请求的大小不要超过 5-15MB
- 确保 Elasticsearch 服务正常运行
- 根据实际环境调整连接配置

## 贡献

欢迎提交 Issue 和 Pull Request 来改进这个学习项目。

## 许可证

本项目仅用于学习目的。