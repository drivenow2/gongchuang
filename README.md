# 通用数据处理系统

这是一个功能强大的数据处理系统，支持 Excel 数据到 MySQL 数据库的智能导入，以及与飞书多维表格的集成。系统能够自动分析数据结构，生成优化的数据库表结构，并提供灵活的数据操作接口。

## 🚀 功能特性

- **智能数据分析**: 自动分析 Excel 数据，推断最合适的 MySQL 字段类型
- **特殊类型识别**: 自动识别 URL、邮箱、手机号等特殊字段类型
- **配置文件驱动**: 生成详细的 JSON 配置文件，支持自定义表结构
- **智能索引优化**: 自动生成主键、唯一键、普通索引和全文索引
- **批量数据处理**: 支持大数据量的高效批量插入
- **飞书集成**: 支持将数据导入到飞书多维表格
- **灵活的数据操作**: 提供完整的 CRUD 操作接口
- **数据验证**: 内置多种数据格式验证规则

## 📦 安装依赖

```bash
pip install -r requirements.txt
```

或手动安装：

```bash
pip install pymysql pandas openpyxl numpy requests
```

## 🔧 核心模块

### DataProcessor (main.py)
通用数据库操作类，提供完整的数据处理功能：

```python
from main import DataProcessor
import pandas as pd

# 初始化处理器
processor = DataProcessor("configs/database_config.json")

# 导入 Excel 数据
df = pd.read_excel('data.xlsx')
success = processor.import_data(df, table_name="my_table", replace=False)

# 查询数据
result = processor.query_data("my_table", limit=100)

# 关闭连接
processor.close()
```

### ConfigGenerator (excel_to_mysql_config.py)
智能配置生成器，包含数据分析和类型推断功能：

```python
from excel_to_mysql_config import ConfigGenerator

generator = ConfigGenerator()
config = generator.generate_config_from_dataframe(df)
```

### UniversalMysqlEngine (engine_mysql.py)
通用 MySQL 数据库引擎，支持配置驱动的表操作：

```python
from engine_mysql import UniversalMysqlEngine

engine = UniversalMysqlEngine(
    host="localhost", port=3306, user="root", 
    password="password", database="test", charset="utf8mb4"
)
```

### SpecialType (special_types.py)
特殊字段类型枚举，定义了各种业务字段类型：

```python
from special_types import SpecialType

# 支持的特殊类型
SpecialType.URL.value        # "url"
SpecialType.EMAIL.value      # "email"
SpecialType.PHONE.value      # "phone"
SpecialType.DATETIME.value   # "datetime"
```

## 📁 项目结构

```
├── main.py                          # 主程序 - DataProcessor 类
├── excel_to_mysql_config.py         # 配置生成器和数据分析器
├── engine_mysql.py                  # MySQL 数据库引擎
├── special_types.py                 # 特殊字段类型定义
├── write.py                         # 飞书多维表格集成
├── requirements.txt                 # 项目依赖
├── xhsRPAtest.xlsx                 # 示例数据文件
├── configs/                         # 配置文件目录
│   ├── xhs_blogger_data_config.json # 小红书博主数据配置示例
│   └── xhsrpatest_config.json      # 测试配置文件
├── test/                           # 测试文件目录
│   └── tt.py                       # 测试脚本
└── README.md                       # 项目说明文档
```

## ⚙️ 配置文件结构

系统生成的 JSON 配置文件包含三个主要部分：

### 表配置 (table_config)
```json
{
  "table_name": "your_table",
  "engine": "InnoDB",
  "charset": "utf8mb4",
  "collate": "utf8mb4_unicode_ci",
  "auto_increment_start": 1,
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
  },
  "id": {
    "mysql_type": "BIGINT",
    "nullable": false,
    "auto_increment": true,
    "special_type": "primary_key",
    "comment": "自增主键"
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

## 🎯 特殊字段类型处理

系统会自动识别以下特殊字段类型：

| 特殊类型 | 识别规则 | MySQL 类型 | 说明 |
|---------|---------|-----------|------|
| **URL** | 包含 http/https 链接 | TEXT | 自动识别网址字段 |
| **EMAIL** | 符合邮箱格式 | VARCHAR(255) | 电子邮件地址 |
| **PHONE** | 中国手机号格式 | VARCHAR(20) | 11位手机号码 |
| **DATETIME** | 日期时间格式 | DATETIME | 日期时间字段 |
| **PRIMARY_KEY** | 自动生成 | BIGINT | 自增主键 |
| **TIMESTAMP** | 时间戳字段 | TIMESTAMP | 创建/更新时间 |

## 📊 数据类型映射

| Pandas 类型 | MySQL 类型 | 说明 |
|------------|-----------|------|
| int64 | BIGINT | 大整数 |
| int32 | INT | 整数 |
| int16 | SMALLINT | 小整数 |
| int8 | TINYINT | 微整数 |
| float64 | DOUBLE | 双精度浮点数 |
| float32 | FLOAT | 单精度浮点数 |
| bool | BOOLEAN | 布尔值 |
| datetime64[ns] | DATETIME | 日期时间 |
| object (短文本) | VARCHAR(n) | 可变长字符串 |
| object (长文本) | TEXT | 长文本 |

## 🔍 使用示例

### 1. 基本数据导入

```python
import pandas as pd
from main import DataProcessor

# 初始化处理器
processor = DataProcessor("configs/database_config.json")

# 读取 Excel 数据
df = pd.read_excel('xhsRPAtest.xlsx', sheet_name='Sheet2')

# 导入数据（自动生成配置）
success = processor.import_data(df, table_name="xhs_blogger_data")

if success:
    print("✅ 数据导入成功")
else:
    print("❌ 数据导入失败")

processor.close()
```

### 2. 使用自定义配置

```python
# 使用现有配置文件
with open('configs/xhs_blogger_data_config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

# 导入数据
success = processor.import_data(df, config=config, replace=False)
```

### 3. 数据查询操作

```python
# 查询所有数据
all_data = processor.query_data("xhs_blogger_data", limit=100)

# 条件查询
conditions = {"粉丝数": ">1000", "地址": "北京"}
filtered_data = processor.query_data("xhs_blogger_data", conditions=conditions)

# 获取表信息
table_info = processor.get_table_info("xhs_blogger_data")
print(f"表信息: {table_info}")
```

### 4. 飞书多维表格集成

```python
# 使用 write.py 将数据导入飞书
from write import write_to_bitable, get_tenant_access_token

# 获取访问令牌
token = get_tenant_access_token()

# 准备数据（字典列表格式）
rows = df.to_dict('records')

# 导入到飞书表格
write_to_bitable(token, field_type_map, rows)
```

## 🛠️ 高级功能

### 1. 自定义字段类型

```python
from special_types import SpecialType

# 检查特殊类型
if SpecialType.is_valid("url"):
    print("URL 类型有效")

# 获取所有支持的类型
all_types = SpecialType.get_all_values()
print(f"支持的类型: {all_types}")
```

### 2. 批量数据处理

系统支持大数据量的批量插入，默认批次大小为 1000 行，可以通过配置调整。

### 3. 智能索引生成

- **主键索引**: 自动为 id 字段创建
- **唯一索引**: 为重要字段创建唯一约束
- **普通索引**: 为常用查询字段创建
- **全文索引**: 为文本字段创建全文搜索索引

## 🚨 注意事项

1. **数据库权限**: 确保数据库用户有 CREATE、INSERT、INDEX 权限
2. **字符编码**: 使用 UTF8MB4 编码支持 emoji 等特殊字符
3. **TEXT 字段索引**: TEXT 类型字段的索引会自动限制长度为 255 字符
4. **数据备份**: 使用 `replace=True` 参数会删除已存在的表，请注意数据备份
5. **配置文件**: 数据库连接配置需要单独创建 `configs/database_config.json`

## 🔧 故障排除

### 常见问题

1. **连接失败**: 检查 `configs/database_config.json` 中的数据库连接参数
2. **权限不足**: 确保用户有 CREATE、INSERT、INDEX 权限
3. **字符编码问题**: 确保数据库和表使用 UTF8MB4 编码
4. **索引创建失败**: TEXT 字段索引会自动处理长度限制
5. **飞书集成失败**: 检查 APP_ID、APP_SECRET 和 APP_TOKEN 配置

### 数据库配置文件示例

创建 `configs/database_config.json`：

```json
{
  "host": "localhost",
  "port": 3306,
  "user": "your_username",
  "password": "your_password",
  "database": "your_database",
  "charset": "utf8mb4"
}
```

## 📈 性能优化

- 使用批量插入提高导入速度
- 智能索引配置优化查询性能
- 合理的字段类型减少存储空间
- 事务处理确保数据一致性
- 连接池管理提高并发性能

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进这个项目！

## 📄 许可证

MIT License