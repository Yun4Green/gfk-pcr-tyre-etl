"""
工具函数模块

提供各种通用的工具函数，包括日期处理、文件操作、日志记录等。
"""

import os
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional


def generate_timestamp() -> str:
    """
    生成时间戳字符串
    
    Returns:
        格式为 YYYYMMDD_HHMMSS 的时间戳
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_directory_exists(directory: str) -> None:
    """
    确保目录存在，如果不存在则创建
    
    Args:
        directory: 目录路径
    """
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def get_file_size_mb(file_path: str) -> float:
    """
    获取文件大小（MB）
    
    Args:
        file_path: 文件路径
        
    Returns:
        文件大小（MB）
    """
    if not os.path.exists(file_path):
        return 0.0
    return os.path.getsize(file_path) / (1024 * 1024)


def validate_file_exists(file_path: str, file_description: str = "文件") -> bool:
    """
    验证文件是否存在
    
    Args:
        file_path: 文件路径
        file_description: 文件描述（用于错误信息）
        
    Returns:
        文件是否存在
    """
    exists = os.path.exists(file_path)
    if not exists:
        print(f"警告: {file_description} '{file_path}' 不存在")
    return exists


def safe_create_filename(pattern: str, **kwargs) -> str:
    """
    安全地创建文件名，替换模板中的占位符
    
    Args:
        pattern: 文件名模式，如 'GFK_{region}_PROCESSED_{timestamp}.csv'
        **kwargs: 替换参数
        
    Returns:
        生成的文件名
    """
    # 设置默认参数
    default_kwargs = {
        'timestamp': generate_timestamp(),
        'region': 'DATA'
    }
    default_kwargs.update(kwargs)
    
    # 替换占位符
    try:
        return pattern.format(**default_kwargs)
    except KeyError as e:
        print(f"警告: 文件名模式中缺少参数 {e}")
        return f"GFK_PROCESSED_{generate_timestamp()}.csv"


def get_dataframe_info(df: pd.DataFrame, name: str = "数据") -> Dict[str, Any]:
    """
    获取DataFrame的基本信息
    
    Args:
        df: pandas DataFrame
        name: 数据名称
        
    Returns:
        包含数据信息的字典
    """
    return {
        'name': name,
        'rows': len(df),
        'columns': len(df.columns),
        'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
        'column_names': list(df.columns),
        'missing_values': df.isnull().sum().sum(),
        'dtypes': df.dtypes.to_dict()
    }


def print_dataframe_summary(df: pd.DataFrame, title: str = "数据摘要") -> None:
    """
    打印DataFrame摘要信息
    
    Args:
        df: pandas DataFrame
        title: 摘要标题
    """
    info = get_dataframe_info(df, title)
    
    print(f"\n=== {title} ===")
    print(f"行数: {info['rows']:,}")
    print(f"列数: {info['columns']}")
    print(f"内存使用: {info['memory_usage_mb']:.2f} MB")
    print(f"缺失值总数: {info['missing_values']}")
    
    if info['columns'] <= 10:
        print(f"列名: {', '.join(info['column_names'])}")
    else:
        print(f"列名: {', '.join(info['column_names'][:5])}... (共{info['columns']}列)")


def filter_existing_files(file_dict: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    过滤出存在的文件
    
    Args:
        file_dict: 文件配置字典
        
    Returns:
        过滤后的文件字典
    """
    existing_files = {}
    
    for key, config in file_dict.items():
        if 'file' in config:
            file_path = config['file']
            if validate_file_exists(file_path, f"{key}数据文件"):
                existing_files[key] = config
            else:
                print(f"跳过 {key}: 文件不存在")
        elif 'files' in config:
            # 处理多个文件的情况
            existing_file_list = []
            for file_path in config['files']:
                if validate_file_exists(file_path, f"{key}数据文件"):
                    existing_file_list.append(file_path)
            
            if existing_file_list:
                config_copy = config.copy()
                config_copy['files'] = existing_file_list
                existing_files[key] = config_copy
    
    return existing_files


def create_progress_logger(total_steps: int):
    """
    创建进度记录器
    
    Args:
        total_steps: 总步骤数
        
    Returns:
        进度记录函数
    """
    def log_progress(current_step: int, description: str = ""):
        percentage = (current_step / total_steps) * 100
        print(f"进度: {percentage:.1f}% ({current_step}/{total_steps}) {description}")
    
    return log_progress


def handle_exception(func):
    """
    异常处理装饰器
    
    Args:
        func: 要装饰的函数
        
    Returns:
        装饰后的函数
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"错误: {func.__name__} 执行失败 - {str(e)}")
            raise
    
    return wrapper


def validate_required_columns(df: pd.DataFrame, required_columns: List[str], 
                            data_name: str = "数据") -> bool:
    """
    验证DataFrame是否包含必需的列
    
    Args:
        df: pandas DataFrame
        required_columns: 必需的列名列表
        data_name: 数据名称（用于错误信息）
        
    Returns:
        是否包含所有必需列
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"错误: {data_name} 缺少必需的列: {missing_columns}")
        print(f"当前可用列: {list(df.columns)}")
        return False
    
    return True


def safe_numeric_conversion(series: pd.Series, column_name: str) -> pd.Series:
    """
    安全地将Series转换为数值类型
    
    Args:
        series: pandas Series
        column_name: 列名（用于错误信息）
        
    Returns:
        转换后的Series
    """
    try:
        return pd.to_numeric(series, errors='coerce')
    except Exception as e:
        print(f"警告: 无法将列 '{column_name}' 转换为数值类型: {e}")
        return series