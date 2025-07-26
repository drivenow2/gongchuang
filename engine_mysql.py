import pymysql
import pandas as pd
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import re
from special_types import SpecialType


class UniversalMysqlEngine:
    """通用 MySQL 数据库引擎，基于配置文件驱动"""
    
    def __init__(self, host,port, user,
                 password, database,
                 charset):
        """初始化数据库连接"""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self.port = port
        
        # 建立连接
        self.connection = None
        self.cursor = None
        self._connect()
        
        # 设置日志
        self.logger = self._setup_logger()
    
    def _connect(self):
        """建立数据库连接"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                # charset=self.charset,
                port=self.port,
                autocommit=False
            )
            self.cursor = self.connection.cursor()
            print(f"成功连接到数据库: {self.database}")
        except Exception as e:
            print(f"数据库连接失败: {e}")
            raise
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('UniversalMysqlEngine')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            self.logger.info(f"成功加载配置文件: {config_path}")
            return config
        except Exception as e:
            self.logger.error(f"加载配置文件失败: {e}")
            raise
    
    def create_table_from_config(self, config: Dict[str, Any]) -> bool:
        """根据配置创建表"""
        try:
            table_config = config['table_config']
            fields_config = config['fields']
            indexes_config = config['indexes']
            
            # 保存字段配置供索引创建时使用
            self._current_fields_config = fields_config
            
            # 生成建表 SQL
            create_sql = self._generate_create_table_sql(table_config, fields_config)
            
            # 执行建表
            self.logger.info(f"创建表: {table_config['table_name']}")
            self.cursor.execute(create_sql)
            
            # 创建索引
            self._create_indexes(table_config['table_name'], indexes_config)
            
            self.connection.commit()
            self.logger.info(f"表 {table_config['table_name']} 创建成功")
            return True
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"创建表失败: {e}")
            return False
    
    def _generate_create_table_sql(self, table_config: Dict[str, Any], 
                                 fields_config: Dict[str, Dict[str, Any]]) -> str:
        """生成建表 SQL"""
        table_name = table_config['table_name']
        engine = table_config.get('engine', 'InnoDB')
        charset = table_config.get('charset', 'utf8mb4')
        collate = table_config.get('collate', 'utf8mb4_unicode_ci')
        comment = table_config.get('comment', '')
        
        # 字段定义
        field_definitions = []
        primary_keys = []
        
        for field_name, field_config in fields_config.items():
            field_def = self._generate_field_definition(field_name, field_config)
            field_definitions.append(field_def)
            
            # 收集主键
            if field_config.get('special_type') == SpecialType.PRIMARY_KEY.value:
                primary_keys.append(field_name)
        
        # 添加主键定义
        if primary_keys:
            field_definitions.append(f"PRIMARY KEY ({', '.join([f'`{pk}`' for pk in primary_keys])})")
        
        # 组装 SQL
        sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {','.join(field_definitions)}
        ) ENGINE={engine} 
          DEFAULT CHARSET={charset} 
          COLLATE={collate}
          COMMENT='{comment}'
        """
        
        return sql
    
    def _generate_field_definition(self, field_name: str, field_config: Dict[str, Any]) -> str:
        """生成字段定义"""
        mysql_type = field_config['mysql_type']
        nullable = field_config.get('nullable', True)
        default = field_config.get('default')
        auto_increment = field_config.get('auto_increment', False)
        comment = field_config.get('comment', '')
        
        # 字段定义
        field_def = f"`{field_name}` {mysql_type}"
        
        # 是否可为空
        if not nullable:
            field_def += " NOT NULL"
        
        # 自增
        if auto_increment:
            field_def += " AUTO_INCREMENT"
        
        # 默认值
        if default is not None:
            if isinstance(default, str) and default.upper() in ['CURRENT_TIMESTAMP', 'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP']:
                field_def += f" DEFAULT {default}"
            elif isinstance(default, str):
                field_def += f" DEFAULT '{default}'"
            else:
                field_def += f" DEFAULT {default}"
        
        # 注释
        if comment:
            field_def += f" COMMENT '{comment}'"
        
        return field_def
    
    def _create_indexes(self, table_name: str, indexes_config: Dict[str, List]):
        """创建索引"""
        fields_config = getattr(self, '_current_fields_config', {})
        
        # 唯一索引
        for i, unique_key in enumerate(indexes_config.get('unique_keys', [])):
            if unique_key:  # 确保不是空列表
                index_name = f"uk_{table_name}_{i+1}"
                
                # 处理 TEXT 类型字段的索引长度
                columns_with_length = []
                for col in unique_key:
                    field_config = fields_config.get(col, {})
                    mysql_type = field_config.get('mysql_type', '')
                    
                    if mysql_type in ['TEXT', 'LONGTEXT', 'MEDIUMTEXT']:
                        # TEXT 类型字段需要指定索引长度
                        columns_with_length.append(f'`{col}`(255)')
                    else:
                        columns_with_length.append(f'`{col}`')
                
                columns_str = ', '.join(columns_with_length)
                sql = f"CREATE UNIQUE INDEX `{index_name}` ON `{table_name}` ({columns_str})"
                
                try:
                    self.cursor.execute(sql)
                    self.logger.info(f"创建唯一索引: {index_name}")
                except Exception as e:
                    self.logger.warning(f"创建唯一索引 {index_name} 失败: {e}")
        
        # 普通索引
        for column in indexes_config.get('normal_indexes', []):
            if column:  # 确保不是空字符串
                index_name = f"idx_{table_name}_{column}"
                
                # 处理 TEXT 类型字段的索引长度
                field_config = fields_config.get(column, {})
                mysql_type = field_config.get('mysql_type', '')
                
                if mysql_type in ['TEXT', 'LONGTEXT', 'MEDIUMTEXT']:
                    column_with_length = f'`{column}`(255)'
                else:
                    column_with_length = f'`{column}`'
                
                sql = f"CREATE INDEX `{index_name}` ON `{table_name}` ({column_with_length})"
                
                try:
                    self.cursor.execute(sql)
                    self.logger.info(f"创建普通索引: {index_name}")
                except Exception as e:
                    self.logger.warning(f"创建普通索引 {index_name} 失败: {e}")
        
        # 全文索引
        for column in indexes_config.get('fulltext_indexes', []):
            if column:  # 确保不是空字符串
                index_name = f"ft_{table_name}_{column}"
                sql = f"CREATE FULLTEXT INDEX `{index_name}` ON `{table_name}` (`{column}`)"
                
                try:
                    self.cursor.execute(sql)
                    self.logger.info(f"创建全文索引: {index_name}")
                except Exception as e:
                    self.logger.warning(f"创建全文索引 {index_name} 失败: {e}")
    
    def insert_data_from_dataframe(self, df: pd.DataFrame, config: Dict[str, Any], 
                                 batch_size: int = 1000) -> bool:
        """从 DataFrame 插入数据"""
        try:
            table_name = config['table_config']['table_name']
            fields_config = config['fields']
            
            # 准备数据
            data_columns = [col for col in df.columns if col in fields_config]
            insert_data = df[data_columns].copy()
            
            # 数据预处理
            insert_data = self._preprocess_data(insert_data, fields_config)
            
            # 批量插入
            total_rows = len(insert_data)
            inserted_rows = 0
            
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_data = insert_data.iloc[start_idx:end_idx]
                
                success = self._insert_batch(table_name, batch_data, data_columns)
                if success:
                    inserted_rows += len(batch_data)
                    self.logger.info(f"已插入 {inserted_rows}/{total_rows} 行数据")
                else:
                    self.logger.error(f"批次插入失败: {start_idx}-{end_idx}")
                    return False
            
            self.connection.commit()
            self.logger.info(f"数据插入完成，共插入 {inserted_rows} 行")
            return True
            
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"数据插入失败: {e}")
            return False
    
    def _preprocess_data(self, df: pd.DataFrame, fields_config: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
        """数据预处理"""
        processed_df = df.copy()
        
        for col in processed_df.columns:
            if col in fields_config:
                field_config = fields_config[col]
                special_type = field_config.get('special_type', SpecialType.NORMAL.value)
                
                # URL 字段验证
                if special_type == SpecialType.URL.value:
                    processed_df[col] = processed_df[col].apply(self._validate_url)
                
                # 邮箱字段验证
                elif special_type == SpecialType.EMAIL.value:
                    processed_df[col] = processed_df[col].apply(self._validate_email)
                
                # 手机号字段验证
                elif special_type == SpecialType.PHONE.value:
                    processed_df[col] = processed_df[col].apply(self._validate_phone)
                
                # 处理空值
                if not field_config.get('nullable', True):
                    default_value = field_config.get('default')
                    if default_value is not None:
                        processed_df[col] = processed_df[col].fillna(default_value)
        
        return processed_df
    
    def _validate_url(self, url: Any) -> Optional[str]:
        """验证 URL 格式"""
        if pd.isna(url):
            return None
        
        url_str = str(url)
        url_pattern = re.compile(r'https?://[^\s]+')
        if url_pattern.match(url_str):
            return url_str
        else:
            self.logger.warning(f"无效的 URL 格式: {url_str}")
            return None
    
    def _validate_email(self, email: Any) -> Optional[str]:
        """验证邮箱格式"""
        if pd.isna(email):
            return None
        
        email_str = str(email)
        email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        if email_pattern.match(email_str):
            return email_str
        else:
            self.logger.warning(f"无效的邮箱格式: {email_str}")
            return None
    
    def _validate_phone(self, phone: Any) -> Optional[str]:
        """验证手机号格式"""
        if pd.isna(phone):
            return None
        
        phone_str = str(phone)
        phone_pattern = re.compile(r'1[3-9]\d{9}')
        if phone_pattern.match(phone_str):
            return phone_str
        else:
            self.logger.warning(f"无效的手机号格式: {phone_str}")
            return None
    
    def _insert_batch(self, table_name: str, batch_data: pd.DataFrame, columns: List[str]) -> bool:
        """批量插入数据"""
        try:
            # 生成插入 SQL
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join([f'`{col}`' for col in columns])
            sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
            
            # 准备数据
            values = []
            for _, row in batch_data.iterrows():
                row_values = []
                for col in columns:
                    value = row[col]
                    if pd.isna(value):
                        row_values.append(None)
                    else:
                        row_values.append(value)
                values.append(tuple(row_values))
            
            # 执行插入
            self.cursor.executemany(sql, values)
            return True
            
        except Exception as e:
            self.logger.error(f"批量插入失败: {e}")
            return False
    
    def query_data(self, config: Dict[str, Any], conditions: Dict[str, Any] = None, 
                  limit: int = None, order_by: str = None) -> pd.DataFrame:
        """查询数据"""
        try:
            table_name = config['table_config']['table_name']
            
            # 构建查询 SQL
            sql = f"SELECT * FROM `{table_name}`"
            params = []
            
            # 添加条件
            if conditions:
                where_clauses = []
                for field, value in conditions.items():
                    if isinstance(value, list):
                        placeholders = ', '.join(['%s'] * len(value))
                        where_clauses.append(f"`{field}` IN ({placeholders})")
                        params.extend(value)
                    else:
                        where_clauses.append(f"`{field}` = %s")
                        params.append(value)
                
                if where_clauses:
                    sql += " WHERE " + " AND ".join(where_clauses)
            
            # 添加排序
            if order_by:
                sql += f" ORDER BY {order_by}"
            
            # 添加限制
            if limit:
                sql += f" LIMIT {limit}"
            
            # 执行查询
            self.logger.info(f"执行查询: {sql}")
            if params:
                df = pd.read_sql(sql, self.connection, params=params)
            else:
                df = pd.read_sql(sql, self.connection)
            
            self.logger.info(f"查询完成，返回 {len(df)} 行数据")
            return df
            
        except Exception as e:
            self.logger.error(f"查询失败: {e}")
            return pd.DataFrame()
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            sql = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
            """
            self.cursor.execute(sql, (self.database, table_name))
            result = self.cursor.fetchone()
            return result[0] > 0
        except Exception as e:
            self.logger.error(f"检查表存在性失败: {e}")
            return False
    
    def drop_table(self, table_name: str) -> bool:
        """删除表"""
        try:
            sql = f"DROP TABLE IF EXISTS `{table_name}`"
            self.cursor.execute(sql)
            self.connection.commit()
            self.logger.info(f"表 {table_name} 删除成功")
            return True
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"删除表失败: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息"""
        try:
            # 获取表结构
            sql = f"DESCRIBE `{table_name}`"
            self.cursor.execute(sql)
            columns_info = self.cursor.fetchall()
            
            # 获取表统计信息
            sql = f"SELECT COUNT(*) FROM `{table_name}`"
            self.cursor.execute(sql)
            row_count = self.cursor.fetchone()[0]
            
            return {
                'table_name': table_name,
                'columns': columns_info,
                'row_count': row_count
            }
        except Exception as e:
            self.logger.error(f"获取表信息失败: {e}")
            return {}
    
    def close(self):
        """关闭数据库连接"""
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
            self.logger.info("数据库连接已关闭")
        except Exception as e:
            # 忽略已关闭的连接错误
            pass
    
    def __del__(self):
        """析构函数"""
        try:
            self.close()
        except:
            # 忽略析构时的错误
            pass


def main():
    """主函数示例"""
    # 创建引擎实例
    engine = UniversalMysqlEngine()
    
    try:
        # 加载配置
        config = engine.load_config('config/xhs_blogger_data_config.json')
        
        # 创建表
        if engine.create_table_from_config(config):
            print("表创建成功")
            
            # 读取 Excel 数据
            df = pd.read_excel('xhsRPAtest.xlsx', sheet_name='Sheet2')
            
            # 插入数据
            if engine.insert_data_from_dataframe(df, config):
                print("数据插入成功")
                
                # 查询数据
                result_df = engine.query_data(config, limit=10)
                print(f"查询结果: {len(result_df)} 行")
                print(result_df.head())
        
    except Exception as e:
        print(f"操作失败: {e}")
    
    finally:
        engine.close()


if __name__ == '__main__':
    main()