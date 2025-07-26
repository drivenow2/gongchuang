import pandas as pd
import json
import re
from typing import Dict, List, Any, Tuple
from datetime import datetime
import numpy as np
from special_types import SpecialType


class DataAnalyzer:
    """数据分析器 - 合并了原SchemaAnalyzer和TypeInferencer的功能"""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.type_mapping = {
            'int8': 'TINYINT',
            'int16': 'SMALLINT', 
            'int32': 'INT',
            'int64': 'BIGINT',
            'float32': 'FLOAT',
            'float64': 'DOUBLE',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'DATETIME',
            'object': 'VARCHAR'
        }
        
    def analyze(self) -> Dict[str, Any]:
        """分析 DataFrame 的结构和特征"""
        analysis_result = {
            'basic_info': self._get_basic_info(),
            'column_analysis': self._analyze_columns()
        }
        return analysis_result
    
    def _get_basic_info(self) -> Dict[str, Any]:
        """获取基本信息"""
        return {
            'row_count': len(self.df),
            'column_count': len(self.df.columns),
            'memory_usage_mb': self.df.memory_usage(deep=True).sum() / 1024**2,
            'columns': list(self.df.columns)
        }
    
    def _analyze_columns(self) -> Dict[str, Dict[str, Any]]:
        """分析每个列的特征并推断MySQL类型"""
        column_analysis = {}
        
        for col in self.df.columns:
            # 基础统计信息
            col_stats = {
                'pandas_dtype': str(self.df[col].dtype),
                'null_count': self.df[col].isnull().sum(),
                'null_percentage': (self.df[col].isnull().sum() / len(self.df)) * 100,
                'unique_count': self.df[col].nunique(),
                'unique_percentage': (self.df[col].nunique() / len(self.df)) * 100,
                'sample_values': self._get_sample_values(col),
                'data_patterns': self._detect_patterns(col),
                'value_length_stats': self._get_length_stats(col)
            }
            
            # 推断MySQL类型
            mysql_config = self._infer_mysql_type(col, col_stats)
            col_stats.update(mysql_config)
            
            column_analysis[col] = col_stats
            
        return column_analysis
    
    def _get_sample_values(self, col: str, sample_size: int = 5) -> List[Any]:
        """获取列的样本值"""
        non_null_values = self.df[col].dropna()
        if len(non_null_values) == 0:
            return []
        
        sample_size = min(sample_size, len(non_null_values))
        return non_null_values.head(sample_size).tolist()
    
    def _detect_patterns(self, col: str) -> Dict[str, Any]:
        """检测数据模式"""
        patterns = {
            'is_url': False,
            'is_email': False,
            'is_phone': False,
            'url_count': 0,
            'email_count': 0,
            'phone_count': 0
        }
        
        # 只分析字符串类型的列
        if self.df[col].dtype == 'object':
            non_null_values = self.df[col].dropna().astype(str)
            
            if len(non_null_values) > 0:
                # URL 模式检测
                url_pattern = re.compile(r'https?://[^\s]+')
                url_matches = non_null_values.str.contains(url_pattern, na=False)
                patterns['url_count'] = url_matches.sum()
                patterns['is_url'] = patterns['url_count'] > len(non_null_values) * 0.8
                
                # 邮箱模式检测
                email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
                email_matches = non_null_values.str.contains(email_pattern, na=False)
                patterns['email_count'] = email_matches.sum()
                patterns['is_email'] = patterns['email_count'] > len(non_null_values) * 0.8
                
                # 手机号模式检测（中国手机号）
                phone_pattern = re.compile(r'1[3-9]\d{9}')
                phone_matches = non_null_values.str.contains(phone_pattern, na=False)
                patterns['phone_count'] = phone_matches.sum()
                patterns['is_phone'] = patterns['phone_count'] > len(non_null_values) * 0.8
            
        return patterns
    
    def _get_length_stats(self, col: str) -> Dict[str, Any]:
        """获取字符串长度统计"""
        if self.df[col].dtype == 'object':
            lengths = self.df[col].dropna().astype(str).str.len()
            if len(lengths) > 0:
                return {
                    'min_length': int(lengths.min()),
                    'max_length': int(lengths.max()),
                    'avg_length': float(lengths.mean()),
                    'median_length': float(lengths.median())
                }
        return {'min_length': 0, 'max_length': 0, 'avg_length': 0, 'median_length': 0}
    
    def _infer_mysql_type(self, col_name: str, col_stats: Dict[str, Any]) -> Dict[str, Any]:
        """推断 MySQL 字段类型"""
        pandas_dtype = col_stats['pandas_dtype']
        patterns = col_stats['data_patterns']
        length_stats = col_stats['value_length_stats']
        
        # 基础类型推断
        if pandas_dtype.startswith('int'):
            mysql_type, special_type = self._infer_integer_type(pandas_dtype)
        elif pandas_dtype.startswith('float'):
            mysql_type, special_type = self._infer_float_type(pandas_dtype)
        elif pandas_dtype == 'bool':
            mysql_type, special_type = 'BOOLEAN', SpecialType.NORMAL.value
        elif pandas_dtype.startswith('datetime'):
            mysql_type, special_type = 'DATETIME', SpecialType.DATETIME.value
        elif pandas_dtype == 'object':
            mysql_type, special_type = self._infer_string_type(length_stats)
        else:
            mysql_type, special_type = 'TEXT', SpecialType.NORMAL.value
        
        # 使用_detect_patterns结果设置special_type
        if patterns['is_url']:
            mysql_type = 'TEXT'
            special_type = SpecialType.URL.value
        elif patterns['is_email']:
            mysql_type = 'VARCHAR(255)'
            special_type = SpecialType.EMAIL.value
        elif patterns['is_phone']:
            mysql_type = 'VARCHAR(20)'
            special_type = SpecialType.PHONE.value
        
        # 确定是否可为空
        nullable = col_stats['null_count'] > 0
        
        # 确定默认值
        default_value = None
        if not nullable and special_type == SpecialType.NORMAL.value:
            if 'int' in pandas_dtype or 'float' in pandas_dtype:
                default_value = 0
            elif pandas_dtype == 'bool':
                default_value = False
            elif pandas_dtype == 'object':
                default_value = ''
        
        return {
            'mysql_type': mysql_type,
            'nullable': nullable,
            'default': default_value,
            'special_type': special_type,
            'comment': col_name  # 使用字段名称作为注释
        }
    
    def _infer_integer_type(self, pandas_dtype: str) -> Tuple[str, str]:
        """推断整数类型"""
        if pandas_dtype == 'int64':
            return 'BIGINT', SpecialType.NORMAL.value
        elif pandas_dtype == 'int32':
            return 'INT', SpecialType.NORMAL.value
        elif pandas_dtype == 'int16':
            return 'SMALLINT', SpecialType.NORMAL.value
        else:
            return 'TINYINT', SpecialType.NORMAL.value
    
    def _infer_float_type(self, pandas_dtype: str) -> Tuple[str, str]:
        """推断浮点数类型"""
        if pandas_dtype == 'float64':
            return 'DOUBLE', SpecialType.NORMAL.value
        else:
            return 'FLOAT', SpecialType.NORMAL.value
    
    def _infer_string_type(self, length_stats: Dict[str, Any]) -> Tuple[str, str]:
        """推断字符串类型"""
        max_length = length_stats['max_length']
        avg_length = length_stats['avg_length']
        
        if max_length == 0:
            return 'VARCHAR(255)', SpecialType.NORMAL.value
        elif max_length <= 50:
            return f'VARCHAR({max(max_length + 10, 50)})', SpecialType.NORMAL.value
        elif max_length <= 255:
            return f'VARCHAR({max_length + 50})', SpecialType.NORMAL.value
        elif avg_length > 500:
            return 'LONGTEXT', SpecialType.NORMAL.value
        else:
            return 'TEXT', SpecialType.NORMAL.value


class ConfigGenerator:
    """配置文件生成器"""
    
    def __init__(self, table_name: str = None):
        self.table_name = table_name
        self.analyzer = None
    
    def generate_config_from_excel(self, excel_path: str, sheet_name: str = None) -> Dict[str, Any]:
        """从 Excel 文件生成配置"""
        # 读取 Excel
        if sheet_name:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
        else:
            df = pd.read_excel(excel_path)
        
        return self.generate_config_from_dataframe(df)
    
    def generate_config_from_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """从 DataFrame 生成配置"""
        # 分析数据结构
        self.analyzer = DataAnalyzer(df)
        analysis_result = self.analyzer.analyze()
        
        # 生成配置
        config = {
            'table_config': self._generate_table_config(),
            'fields': self._generate_fields_config(analysis_result['column_analysis']),
            'indexes': self._generate_indexes_config(analysis_result['column_analysis'])
        }
        
        return config
    
    def _generate_table_config(self) -> Dict[str, Any]:
        """生成表配置"""
        return {
            'table_name': self.table_name or 'auto_generated_table',
            'engine': 'InnoDB',
            'charset': 'utf8mb4',
            'collate': 'utf8mb4_unicode_ci',
            'auto_increment_start': 1,
            'comment': '自动生成的表结构'
        }
    
    def _generate_fields_config(self, column_analysis: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """生成字段配置"""
        fields_config = {}
        
        # 添加自增主键
        fields_config['id'] = {
            'mysql_type': 'BIGINT',
            'nullable': False,
            'auto_increment': True,
            'default': None,
            'comment': '自增主键',
            'special_type': SpecialType.PRIMARY_KEY.value
        }
        
        # 处理数据字段
        for col_name, col_analysis in column_analysis.items():
            field_config = {
                'mysql_type': col_analysis['mysql_type'],
                'nullable': col_analysis['nullable'],
                'default': col_analysis['default'],
                'special_type': col_analysis['special_type'],
                'comment': col_analysis['comment']
            }
            fields_config[col_name] = field_config
        
        # 添加时间戳字段
        fields_config['created_at'] = {
            'mysql_type': 'TIMESTAMP',
            'nullable': False,
            'default': 'CURRENT_TIMESTAMP',
            'comment': '创建时间',
            'special_type': SpecialType.TIMESTAMP.value
        }
        
        fields_config['updated_at'] = {
            'mysql_type': 'TIMESTAMP',
            'nullable': False,
            'default': 'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
            'comment': '更新时间',
            'special_type': SpecialType.TIMESTAMP.value
        }
        
        return fields_config
    
    def _generate_indexes_config(self, column_analysis: Dict[str, Dict[str, Any]]) -> Dict[str, List]:
        """生成索引配置"""
        indexes = {
            'primary_key': ['id'],
            'unique_keys': [],
            'normal_indexes': ['created_at', 'updated_at'],
            'fulltext_indexes': []
        }
        
        for col_name, col_analysis in column_analysis.items():
            unique_percentage = col_analysis['unique_percentage']
            mysql_type = col_analysis['mysql_type']
            
            # 唯一键候选（唯一值超过95%）
            if unique_percentage > 95:
                indexes['unique_keys'].append([col_name])
            
            # 普通索引候选（适合建索引的字段）
            elif unique_percentage > 10 and unique_percentage < 95:
                indexes['normal_indexes'].append(col_name)
            
            # 全文索引候选（长文本字段）
            if mysql_type in ['TEXT', 'LONGTEXT']:
                indexes['fulltext_indexes'].append(col_name)
        
        return indexes
    
    def save_config(self, config: Dict[str, Any], file_path: str):
        """保存配置到文件"""
        # 自定义JSON编码器，处理numpy数据类型
        class NumpyEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.bool_):
                    return bool(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                return super().default(obj)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2, cls=NumpyEncoder)
        print(f"配置文件已保存到: {file_path}")


def main():
    """主函数示例"""
    # 示例用法
    generator = ConfigGenerator(table_name='xhs_blogger_info')
    
    # 从 Excel 生成配置
    config = generator.generate_config_from_excel('xhsRPAtest.xlsx', sheet_name='Sheet2')
    
    # 保存配置
    generator.save_config(config, 'xhs_blogger_config.json')
    
    # 打印配置预览
    print("生成的配置预览:")
    print(json.dumps(config, ensure_ascii=False, indent=2)[:1000] + "...")


if __name__ == '__main__':
    main()