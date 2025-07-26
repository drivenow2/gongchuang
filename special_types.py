from enum import Enum
from typing import Union


class SpecialType(Enum):
    """字段特殊类型枚举
    
    定义了数据库字段的特殊类型，用于标识字段的业务含义和处理方式
    """
    
    # 基础类型
    NORMAL = "normal"  # 普通字段
    PRIMARY_KEY = "primary_key"  # 主键字段
    
    # 数据类型
    URL = "url"  # URL地址字段
    EMAIL = "email"  # 邮箱地址字段
    PHONE = "phone"  # 手机号码字段
    
    # 时间类型
    DATETIME = "datetime"  # 日期时间字段
    TIMESTAMP = "timestamp"  # 时间戳字段
    
    @property
    def description(self) -> str:
        """获取类型描述"""
        descriptions = {
            self.NORMAL: "普通字段，无特殊处理要求",
            self.PRIMARY_KEY: "主键字段，唯一标识记录",
            self.URL: "URL地址字段，存储网址链接",
            self.EMAIL: "邮箱地址字段，存储电子邮件地址",
            self.PHONE: "手机号码字段，存储电话号码",
            self.DATETIME: "日期时间字段，存储日期和时间信息",
            self.TIMESTAMP: "时间戳字段，记录创建或更新时间"
        }
        return descriptions.get(self, "未知类型")
    
    @classmethod
    def from_string(cls, value: str) -> 'SpecialType':
        """从字符串值创建枚举实例
        
        Args:
            value: 字符串值
            
        Returns:
            SpecialType: 对应的枚举实例
            
        Raises:
            ValueError: 当字符串值不匹配任何枚举值时
        """
        for special_type in cls:
            if special_type.value == value:
                return special_type
        raise ValueError(f"无效的 special_type 值: {value}")
    
    @classmethod
    def is_valid(cls, value: str) -> bool:
        """检查字符串值是否为有效的 special_type
        
        Args:
            value: 要检查的字符串值
            
        Returns:
            bool: 如果值有效返回 True，否则返回 False
        """
        try:
            cls.from_string(value)
            return True
        except ValueError:
            return False
    
    @classmethod
    def get_all_values(cls) -> list:
        """获取所有枚举值的字符串列表
        
        Returns:
            list: 所有枚举值的字符串列表
        """
        return [special_type.value for special_type in cls]
    
    @classmethod
    def get_data_types(cls) -> list:
        """获取数据类型相关的枚举值
        
        Returns:
            list: 数据类型相关的枚举值列表
        """
        return [cls.URL, cls.EMAIL, cls.PHONE]
    
    @classmethod
    def get_time_types(cls) -> list:
        """获取时间类型相关的枚举值
        
        Returns:
            list: 时间类型相关的枚举值列表
        """
        return [cls.DATETIME, cls.TIMESTAMP]
    
    def __str__(self) -> str:
        """返回枚举值的字符串表示"""
        return self.value
    
    def __repr__(self) -> str:
        """返回枚举的详细表示"""
        return f"SpecialType.{self.name}('{self.value}')"


# 便利函数
def validate_special_type(value: Union[str, SpecialType]) -> str:
    """验证并标准化 special_type 值
    
    Args:
        value: special_type 值，可以是字符串或 SpecialType 枚举
        
    Returns:
        str: 标准化后的字符串值
        
    Raises:
        ValueError: 当值无效时
    """
    if isinstance(value, SpecialType):
        return value.value
    elif isinstance(value, str):
        if SpecialType.is_valid(value):
            return value
        else:
            raise ValueError(f"无效的 special_type 值: {value}")
    else:
        raise TypeError(f"special_type 必须是字符串或 SpecialType 枚举，得到: {type(value)}")