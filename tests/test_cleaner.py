"""
数据清洗器测试
"""

import pytest
import pandas as pd
import numpy as np
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gfk_etl_library.core.cleaner import DataCleaner


class TestDataCleaner:
    """数据清洗器测试类"""
    
    def create_sample_data(self):
        """创建示例数据"""
        data = {
            'Seasonality': ['Summer', 'Winter', 'TOTAL', 'Summer.TOTAL', 'Winter'],
            'Brandlines': ['Brand A', 'Brand B', 'Brand.TOTAL', 'Brand C', 'Brand D'],
            'Rim Diameter': [15, 16, '.TOTAL.', 17, 18],
            'Value': [100, 200, 300, 400, 500],
            'MAT JUN 24': [10, 20, 30, 40, 50],  # 要删除的列
            'YTD JUN 25': [1, 2, 3, 4, 5],      # 要删除的列
            'Keep_Column': ['A', 'B', 'C', 'D', 'E']
        }
        return pd.DataFrame(data)
    
    def create_cleaner(self):
        """创建清洗器实例"""
        cleaning_config = {
            'remove_total_rows': True,
            'total_patterns': [r'\.TOTAL', r'^TOTAL$', r'\.TOTAL\.'],
            'columns_to_drop': ['MAT JUN 24', 'YTD JUN 25']
        }
        return DataCleaner(cleaning_config)
    
    def test_remove_total_rows(self):
        """测试删除TOTAL行"""
        df = self.create_sample_data()
        cleaner = self.create_cleaner()
        
        original_rows = len(df)
        cleaned_df = cleaner.remove_total_rows_func(df)
        
        # 检查TOTAL行是否被删除
        assert len(cleaned_df) < original_rows
        
        # 检查特定的TOTAL行是否被删除
        assert not cleaned_df['Seasonality'].str.contains('TOTAL').any()
        assert not cleaned_df['Brandlines'].str.contains('TOTAL').any()
        assert not cleaned_df['Rim Diameter'].str.contains('TOTAL').any()
        
        # 确保非TOTAL行被保留
        assert 'Summer' in cleaned_df['Seasonality'].values
        assert 'Winter' in cleaned_df['Seasonality'].values
    
    def test_drop_columns(self):
        """测试删除指定列"""
        df = self.create_sample_data()
        cleaner = self.create_cleaner()
        
        original_columns = set(df.columns)
        cleaned_df = cleaner.drop_columns(df)
        
        # 检查指定的列是否被删除
        assert 'MAT JUN 24' not in cleaned_df.columns
        assert 'YTD JUN 25' not in cleaned_df.columns
        
        # 检查其他列是否保留
        assert 'Seasonality' in cleaned_df.columns
        assert 'Value' in cleaned_df.columns
        assert 'Keep_Column' in cleaned_df.columns
    
    def test_clean_dataframe_complete(self):
        """测试完整的清洗流程"""
        df = self.create_sample_data()
        cleaner = self.create_cleaner()
        
        original_rows = len(df)
        original_columns = len(df.columns)
        
        cleaned_df = cleaner.clean_dataframe(df, "测试数据")
        
        # 检查行数减少（删除了TOTAL行）
        assert len(cleaned_df) < original_rows
        
        # 检查列数减少（删除了指定列）
        assert len(cleaned_df.columns) < original_columns
        
        # 检查数据完整性
        assert not cleaned_df.empty
        assert len(cleaned_df) > 0
    
    def test_handle_missing_values_report(self):
        """测试缺失值处理 - 报告模式"""
        # 创建带缺失值的数据
        data = {
            'Col1': [1, 2, None, 4, 5],
            'Col2': ['A', None, 'C', 'D', None],
            'Col3': [10.1, 20.2, 30.3, None, 50.5]
        }
        df = pd.DataFrame(data)
        
        cleaner = self.create_cleaner()
        result_df = cleaner.handle_missing_values(df, strategy='report')
        
        # 报告模式不应该改变数据
        assert len(result_df) == len(df)
        assert result_df.isnull().sum().sum() == df.isnull().sum().sum()
    
    def test_handle_missing_values_drop(self):
        """测试缺失值处理 - 删除模式"""
        data = {
            'Col1': [1, 2, None, 4, 5],
            'Col2': ['A', None, 'C', 'D', 'E'],
            'Col3': [10.1, 20.2, 30.3, 40.4, 50.5]
        }
        df = pd.DataFrame(data)
        
        cleaner = self.create_cleaner()
        result_df = cleaner.handle_missing_values(df, strategy='drop')
        
        # 删除模式应该移除有缺失值的行
        assert len(result_df) < len(df)
        assert result_df.isnull().sum().sum() == 0
    
    def test_handle_missing_values_fill(self):
        """测试缺失值处理 - 填充模式"""
        data = {
            'Col1': [1, 2, None, 4, 5],
            'Col2': ['A', None, 'C', 'D', None],
            'Col3': [10.1, 20.2, None, 40.4, 50.5]
        }
        df = pd.DataFrame(data)
        
        cleaner = self.create_cleaner()
        result_df = cleaner.handle_missing_values(df, strategy='fill')
        
        # 填充模式应该没有缺失值
        assert result_df.isnull().sum().sum() == 0
        
        # 检查填充值
        assert (result_df['Col1'] == 0).any()  # 数值列填充0
        assert (result_df['Col2'] == 'Unknown').any()  # 字符串列填充'Unknown'
    
    def test_detect_outliers(self):
        """测试异常值检测"""
        # 创建带异常值的数据
        data = {
            'Normal_Col': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'Outlier_Col': [1, 2, 3, 4, 5, 6, 7, 8, 9, 1000],  # 1000是异常值
            'String_Col': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']
        }
        df = pd.DataFrame(data)
        
        cleaner = self.create_cleaner()
        outlier_info = cleaner.detect_outliers(df)
        
        # 检查是否检测到异常值
        assert 'Outlier_Col' in outlier_info
        assert outlier_info['Outlier_Col']['count'] > 0
        
        # Normal_Col应该没有异常值
        assert outlier_info['Normal_Col']['count'] == 0
        
        # String_Col不应该在结果中（非数值列）
        assert 'String_Col' not in outlier_info
    
    def test_validate_data_types(self):
        """测试数据类型验证"""
        data = {
            'Int_Col': [1, 2, 3, 4, 5],
            'Float_Col': [1.1, 2.2, 3.3, 4.4, 5.5],
            'String_Col': ['A', 'B', 'C', 'D', 'E'],
            'Mixed_Col': [1, '2', 3.0, '4', 5]
        }
        df = pd.DataFrame(data)
        
        cleaner = self.create_cleaner()
        
        # 基本数据类型检查（无期望类型）
        result = cleaner.validate_data_types(df)
        assert result == True  # 基本检查总是返回True
        
        # 带期望类型的检查
        expected_types = {
            'Int_Col': 'int',
            'Float_Col': 'float',
            'String_Col': 'object',
            'Nonexistent_Col': 'int'  # 不存在的列
        }
        result = cleaner.validate_data_types(df, expected_types)
        assert result == False  # 因为有不存在的列
    
    def test_basic_cleaning(self):
        """测试基本清洗功能"""
        # 创建带各种问题的数据
        data = {
            'Col1': [1, 2, None, 4, 5],
            'Col2': ['  A  ', 'B', '  C  ', None, 'E'],  # 带空白的字符串
            'Col3': [10, 20, 30, 40, 50],
            'Empty_Row': [None, None, None, None, None]  # 完全空的行
        }
        df = pd.DataFrame(data)
        
        # 添加一行完全空的数据
        empty_row = pd.DataFrame([[None, None, None, None]], columns=df.columns)
        df = pd.concat([df, empty_row], ignore_index=True)
        
        cleaner = self.create_cleaner()
        cleaned_df = cleaner.basic_cleaning(df)
        
        # 检查空白是否被清理
        non_null_strings = cleaned_df['Col2'].dropna()
        for val in non_null_strings:
            assert not val.startswith(' ') and not val.endswith(' ')
        
        # 检查索引是否重置
        assert cleaned_df.index.tolist() == list(range(len(cleaned_df)))
    
    def test_empty_dataframe_handling(self):
        """测试空DataFrame的处理"""
        empty_df = pd.DataFrame()
        cleaner = self.create_cleaner()
        
        # 清洗空DataFrame应该返回原DataFrame
        result = cleaner.clean_dataframe(empty_df, "空数据")
        assert result.empty
        assert len(result) == 0
    
    def test_no_total_patterns_config(self):
        """测试没有TOTAL模式配置的情况"""
        config = {
            'remove_total_rows': False,
            'total_patterns': [],
            'columns_to_drop': []
        }
        cleaner = DataCleaner(config)
        
        df = self.create_sample_data()
        original_len = len(df)
        
        # 不应该删除任何行
        cleaned_df = cleaner.clean_dataframe(df)
        assert len(cleaned_df) == original_len
    
    def test_custom_total_patterns(self):
        """测试自定义TOTAL模式"""
        config = {
            'remove_total_rows': True,
            'total_patterns': [r'CUSTOM_TOTAL'],  # 自定义模式
            'columns_to_drop': []
        }
        cleaner = DataCleaner(config)
        
        data = {
            'Col1': ['A', 'B', 'CUSTOM_TOTAL', 'C'],
            'Col2': ['X', 'Y', 'Z', 'W'],
            'Value': [1, 2, 3, 4]
        }
        df = pd.DataFrame(data)
        
        cleaned_df = cleaner.remove_total_rows_func(df)
        
        # 应该删除包含CUSTOM_TOTAL的行
        assert 'CUSTOM_TOTAL' not in cleaned_df['Col1'].values
        assert len(cleaned_df) == 3


if __name__ == '__main__':
    pytest.main([__file__])