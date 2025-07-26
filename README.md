# Excel 到 MySQL 通用数据处理系统

这是一个通用的 Excel 到 MySQL 数据处理系统，能够自动分析 Excel 数据结构，生成配置文件，创建 MySQL 表结构并导入数据。

## 🚀 功能特性

- **智能类型推断**: 自动分析 Excel 数据，推断最合适的 MySQL 字段类型
- **HTTPS URL 特殊处理**: 自动识别 URL 字段并设置为 TEXT 类型
- **配置文件驱动**: 生成详细的配置文件，支持自定义表结构
- **索引优化**: 自动生成唯一键、普通索引和全文索引
- **批量数据导入**: 支持大数据量的批量插入
- **数据验证**: 内置 URL、邮箱、手机号等格式验证
- **查询功能**: 支持条件查询和数据统计

## 📦 安装依赖

```bash
pip install pymysql pandas openpyxl
```

## 🔧 使用方法

### 基本用法

```bash
# 处理 Excel 文件（自动推断表名）
python excel_to_mysql_main.py your_file.xlsx

# 指定工作表和表名
python excel_to_mysql_main.py your_file.xlsx -s Sheet2 -t my_table

# 强制重新创建表
python excel_to_mysql_main.py your_file.xlsx -f
```

### 查询数据

```bash
# 查询前 50 行数据
python excel_to_mysql_main.py your_file.xlsx --query --limit 50

# 查看表统计信息
python excel_to_mysql_main.py your_file.xlsx --stats
```

### 自定义数据库连接

```bash
python excel_to_mysql_main.py your_file.xlsx \
  --host localhost \
  --user your_user \
  --password your_password \
  --database your_database
```

## 📁 文件结构

```
├── excel_to_mysql_config.py    # 配置生成器模块
├── universal_mysql_engine.py   # 通用数据库引擎
├── excel_to_mysql_main.py      # 主程序
├── xhs_blogger_data_config.json # 生成的配置文件示例
└── README.md                   # 说明文档
```

## ⚙️ 配置文件结构

生成的配置文件包含以下部分：

### 表配置 (table_config)
```json
{
  "table_name": "your_table",
  "engine": "InnoDB",
  "charset": "utf8mb4",
  "collate": "utf8mb4_unicode_ci",
  "comment": "表注释"
}
```

### 字段配置 (fields)
```json
{
  "field_name": {
    "mysql_type": "VARCHAR(255)",
    "nullable": true,
    "default": null,
    "special_type": "url",
    "comment": "字段注释"
  }
}
```

### 索引配置 (indexes)
```json
{
  "primary_key": ["id"],
  "unique_keys": [["field1"], ["field2", "field3"]],
  "normal_indexes": ["field4", "field5"],
  "fulltext_indexes": ["content_field"]
}
```

## 🎯 特殊字段类型

系统会自动识别以下特殊字段类型：

- **URL 字段**: 自动识别 HTTPS/HTTP 链接，设置为 TEXT 类型
- **邮箱字段**: 识别邮箱格式，设置为 VARCHAR(255)
- **手机号字段**: 识别中国手机号格式，设置为 VARCHAR(20)
- **日期时间字段**: 自动转换为 DATETIME 类型

## 📊 数据类型映射

| Pandas 类型 | MySQL 类型 | 说明 |
|------------|-----------|------|
| int64 | BIGINT | 大整数 |
| int32 | INT | 整数 |
| float64 | DOUBLE | 双精度浮点数 |
| bool | BOOLEAN | 布尔值 |
| object (短文本) | VARCHAR(n) | 可变长字符串 |
| object (长文本) | TEXT | 长文本 |
| datetime64 | DATETIME | 日期时间 |

## 🔍 示例：处理小红书博主数据

```bash
# 处理小红书博主数据
python excel_to_mysql_main.py xhsRPAtest.xlsx -s Sheet2 -t xhs_blogger_data

# 查询数据
python excel_to_mysql_main.py xhsRPAtest.xlsx -t xhs_blogger_data --query --limit 10

# 查看统计信息
python excel_to_mysql_main.py xhsRPAtest.xlsx -t xhs_blogger_data --stats
```

生成的表结构包含：
- 自增主键 `id`
- 博主基本信息字段（名称、头像、主页等）
- URL 字段特殊处理（TEXT 类型）
- 自动时间戳字段（created_at, updated_at）
- 智能索引配置

## 🛠️ 高级功能

### 1. 自定义配置文件

你可以手动编辑生成的配置文件来自定义表结构：

```json
{
  "fields": {
    "custom_field": {
      "mysql_type": "VARCHAR(100)",
      "nullable": false,
      "default": "default_value",
      "comment": "自定义字段"
    }
  }
}
```

### 2. 批量处理

系统支持大数据量的批量插入，默认批次大小为 1000 行。

### 3. 数据验证

内置多种数据验证规则：
- URL 格式验证
- 邮箱格式验证  
- 手机号格式验证
- 空值处理

## 🚨 注意事项

1. **数据库权限**: 确保数据库用户有创建表和索引的权限
2. **字符编码**: 使用 UTF8MB4 编码支持 emoji 等特殊字符
3. **TEXT 字段索引**: TEXT 类型字段的索引会自动限制长度为 255 字符
4. **数据备份**: 使用 `-f` 参数会删除已存在的表，请注意数据备份

## 🔧 故障排除

### 常见问题

1. **连接失败**: 检查数据库连接参数
2. **权限不足**: 确保用户有 CREATE、INSERT、INDEX 权限
3. **字符编码问题**: 确保数据库和表使用 UTF8MB4 编码
4. **索引创建失败**: TEXT 字段索引会自动处理长度限制

### 日志信息

程序会输出详细的日志信息，包括：
- 数据读取进度
- 表创建状态
- 索引创建结果
- 数据插入进度

## 📈 性能优化

- 使用批量插入提高导入速度
- 智能索引配置优化查询性能
- 合理的字段类型减少存储空间
- 事务处理确保数据一致性

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进这个项目！

## 📄 许可证

MIT License