#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用数据库操作类 - 支持DataFrame和配置的数据入库和查询
"""

import json
import pandas as pd
from typing import Dict, Any, Optional
from excel_to_mysql_config import ConfigGenerator
from engine_mysql import UniversalMysqlEngine


class DataProcessor:
    """通用数据库操作类"""
    
    def __init__(self, db_config_file: str = "configs/database_config.json"):
        """初始化数据库连接"""
        with open(db_config_file, 'r', encoding='utf-8') as f:
            db_config = json.load(f)
        
        # 过滤掉UniversalMysqlEngine不支持的参数
        supported_params = ['host', 'port', 'user', 'password', 'database', 'charset']
        filtered_config = {k: v for k, v in db_config.items() if k in supported_params}
        
        self.engine = UniversalMysqlEngine(**filtered_config)
        self.config_generator = ConfigGenerator()
    
    def import_data(self, df: pd.DataFrame, config: Dict[str, Any] = None,
                 table_name: str= None, replace: bool = False) -> bool:
        
        """
        导入DataFrame数据到数据库
        
        Args:
            df: 数据DataFrame
            table_name: 表名
            config: 表配置
            replace: 是否删除并替换已存在表
            
        Returns:
            bool: 导入是否成功
        """
        try:
            # 生成配置（如果未提供）
            if config is None:
                assert table_name!=None, "未传入config, 需要传入table_name重新生成配置文件！！"
                self.config_generator.table_name = table_name
                config = self.config_generator.generate_config_from_dataframe(df)
            else:
                table_name = config['table_config']['table_name']
            
            # 处理表存在的情况
            if self.engine.table_exists(table_name) and replace:
                self.engine.drop_table(table_name)
            
            # 创建表（如果不存在）
            if not self.engine.table_exists(table_name):
                self.engine.create_table_from_config(config)
            
            # 插入数据
            return self.engine.insert_data_from_dataframe(df, config)
        except Exception as e:
            print(f"❌ 数据导入失败: {e}")
            return False
    
    def query_data(self, table_name: str, conditions: Dict[str, Any] = None,
                  limit: int = 100, order_by: str = None) -> pd.DataFrame:
        """查询表数据"""
        config = {'table_config': {'table_name': table_name}}
        return self.engine.query_data(config, conditions, limit, order_by)
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表信息"""
        return self.engine.get_table_info(table_name)
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return self.engine.table_exists(table_name)
    
    def drop_table(self, table_name: str) -> bool:
        """删除表"""
        return self.engine.drop_table(table_name)
    
    def close(self):
        """关闭数据库连接"""
        self.engine.close()

if __name__ == '__main__':
    processor = DataProcessor()
    df = pd.read_excel('xhsRPAtest.xlsx', sheet_name='Sheet2')
    config = processor.config_generator.generate_config_from_dataframe(df)
    processor.import_data(df, config=config, replace=False)
    processor.close()