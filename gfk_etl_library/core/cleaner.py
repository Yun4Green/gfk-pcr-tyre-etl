"""
数据清洗器模块

负责数据清洗操作，包括删除汇总行、处理缺失值、异常值检测等。
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from ..utils import print_dataframe_summary


class DataCleaner:
    """数据清洗器类"""
    
    def __init__(self, cleaning_config: Dict[str, Any]):
        """
        初始化数据清洗器
        
        Args:
            cleaning_config: 清洗配置字典
        """
        self.config = cleaning_config
        self.remove_total_rows = cleaning_config.get('remove_total_rows', True)
        self.total_patterns = cleaning_config.get('total_patterns', [r'\.TOTAL', r'^TOTAL$', r'\.TOTAL\.'])
        self.columns_to_drop = cleaning_config.get('columns_to_drop', [])
    
    def clean_dataframe(self, df: pd.DataFrame, data_name: str = "数据") -> pd.DataFrame:
        """
        执行完整的数据清洗流程
        
        Args:
            df: 要清洗的DataFrame
            data_name: 数据名称（用于日志）
            
        Returns:
            清洗后的DataFrame
        """
        if df is None or df.empty:
            print(f"警告: {data_name} 为空，跳过清洗")
            return df
        
        print(f"\n=== 清洗 {data_name} ===")
        original_rows = len(df)
        
        # 1. 删除指定列
        df_cleaned = self.drop_columns(df.copy())
        
        # 2. 删除TOTAL行
        if self.remove_total_rows:
            df_cleaned = self.remove_total_rows_func(df_cleaned)
        
        # 3. 基本数据清洗
        df_cleaned = self.basic_cleaning(df_cleaned)
        
        cleaned_rows = len(df_cleaned)
        removed_rows = original_rows - cleaned_rows
        
        print(f"清洗完成: {original_rows} → {cleaned_rows} 行 (删除 {removed_rows} 行)")
        
        return df_cleaned
    
    def drop_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        删除指定的列
        
        Args:
            df: DataFrame
            
        Returns:
            删除列后的DataFrame
        """
        if not self.columns_to_drop:
            return df
        
        existing_columns_to_drop = [col for col in self.columns_to_drop if col in df.columns]
        
        if existing_columns_to_drop:
            print(f"删除列: {existing_columns_to_drop}")
            df = df.drop(columns=existing_columns_to_drop)
        else:
            print("无需删除列（指定的列不存在）")
        
        return df
    
    def remove_total_rows_func(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        删除包含TOTAL的汇总行
        
        Args:
            df: DataFrame
            
        Returns:
            删除TOTAL行后的DataFrame
        """
        print(f"清洗前行数: {len(df)}")
        
        # 需要检查TOTAL的列
        columns_to_check = ['Seasonality', 'Rim Diameter', 'DIMENSION (Car Tires)', 
                           'Dimension', 'SpeedIndex', 'Speed Index', 'LoadIndex', 
                           'Load Index', 'Brandlines', 'Brand']
        
        # 只检查存在的列
        existing_columns = [col for col in columns_to_check if col in df.columns]
        
        if not existing_columns:
            print("无可检查的列，跳过TOTAL行删除")
            return df
        
        # 创建过滤条件
        mask = pd.Series([True] * len(df))
        
        for col in existing_columns:
            for pattern in self.total_patterns:
                # 使用字符串方法进行模式匹配
                col_mask = ~df[col].astype(str).str.contains(pattern, na=False, regex=True)
                mask = mask & col_mask
        
        cleaned_df = df[mask].copy()
        removed_count = len(df) - len(cleaned_df)
        
        print(f"清洗后行数: {len(cleaned_df)}")
        print(f"删除了 {removed_count} 行包含TOTAL的数据")
        
        return cleaned_df
    
    def basic_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        基本数据清洗
        
        Args:
            df: DataFrame
            
        Returns:
            清洗后的DataFrame
        """
        original_rows = len(df)
        
        # 1. 删除完全空的行
        df = df.dropna(how='all')
        empty_rows_removed = original_rows - len(df)
        if empty_rows_removed > 0:
            print(f"删除 {empty_rows_removed} 行完全空的数据")
        
        # 2. 重置索引
        df = df.reset_index(drop=True)
        
        # 3. 清理字符串列的空白
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                # 将'nan'字符串转换为实际的NaN
                df[col] = df[col].replace(['nan', 'None', ''], np.nan)
        
        return df
    
    def handle_missing_values(self, df: pd.DataFrame, 
                            strategy: str = 'report') -> pd.DataFrame:
        """
        处理缺失值
        
        Args:
            df: DataFrame
            strategy: 处理策略 ('report', 'drop', 'fill')
            
        Returns:
            处理后的DataFrame
        """
        missing_count = df.isnull().sum()
        total_missing = missing_count.sum()
        
        print(f"\n=== 缺失值处理 ===")
        print(f"总缺失值数量: {total_missing}")
        
        if total_missing == 0:
            print("✅ 无缺失值")
            return df
        
        # 显示缺失值统计
        missing_columns = missing_count[missing_count > 0]
        print(f"有缺失值的列:")
        for col, count in missing_columns.items():
            percentage = (count / len(df)) * 100
            print(f"  {col}: {count} ({percentage:.1f}%)")
        
        if strategy == 'drop':
            df_result = df.dropna()
            dropped_rows = len(df) - len(df_result)
            print(f"删除了 {dropped_rows} 行包含缺失值的数据")
            return df_result
        elif strategy == 'fill':
            # 简单的填充策略
            df_result = df.copy()
            for col in missing_columns.index:
                if df_result[col].dtype in ['int64', 'float64']:
                    df_result[col] = df_result[col].fillna(0)
                else:
                    df_result[col] = df_result[col].fillna('Unknown')
            print(f"已填充缺失值")
            return df_result
        else:
            # 只报告，不处理
            return df
    
    def detect_outliers(self, df: pd.DataFrame, 
                       columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        检测异常值
        
        Args:
            df: DataFrame
            columns: 要检测的列名列表，None表示所有数值列
            
        Returns:
            异常值检测结果
        """
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        outlier_info = {}
        
        print(f"\n=== 异常值检测 ===")
        
        for col in columns:
            if col not in df.columns:
                continue
            
            series = df[col]
            if series.dtype not in [np.number] and not pd.api.types.is_numeric_dtype(series):
                continue
            
            # 使用IQR方法检测异常值
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = series[(series < lower_bound) | (series > upper_bound)]
            
            outlier_info[col] = {
                'count': len(outliers),
                'percentage': (len(outliers) / len(series)) * 100,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'min_outlier': outliers.min() if len(outliers) > 0 else None,
                'max_outlier': outliers.max() if len(outliers) > 0 else None
            }
            
            if len(outliers) > 0:
                print(f"{col}: {len(outliers)} 个异常值 ({outlier_info[col]['percentage']:.1f}%)")
        
        return outlier_info
    
    def validate_data_types(self, df: pd.DataFrame, 
                          expected_types: Optional[Dict[str, str]] = None) -> bool:
        """
        验证数据类型
        
        Args:
            df: DataFrame
            expected_types: 期望的数据类型字典
            
        Returns:
            是否通过验证
        """
        if expected_types is None:
            # 基本的数据类型检查
            print("\n=== 数据类型检查 ===")
            for col in df.columns:
                dtype = df[col].dtype
                null_count = df[col].isnull().sum()
                print(f"{col}: {dtype} (缺失值: {null_count})")
            return True
        
        validation_passed = True
        
        for col, expected_type in expected_types.items():
            if col not in df.columns:
                print(f"警告: 列 '{col}' 不存在")
                validation_passed = False
                continue
            
            actual_type = str(df[col].dtype)
            if expected_type not in actual_type:
                print(f"警告: 列 '{col}' 类型不匹配 - 期望: {expected_type}, 实际: {actual_type}")
                validation_passed = False
        
        return validation_passed
    
    def __str__(self) -> str:
        """返回清洗器的字符串表示"""
        return f"DataCleaner(remove_total_rows={self.remove_total_rows})"