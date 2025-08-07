"""
数据转换器测试
"""

import pytest
import pandas as pd
import numpy as np
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gfk_etl_library.core.transformer import DataTransformer


class TestDataTransformer:
    """数据转换器测试类"""
    
    def create_sample_wide_data(self):
        """创建宽格式示例数据"""
        data = {
            'Seasonality': ['Summer', 'Winter', 'Summer'],
            'Brandlines': ['Brand A', 'Brand B', 'Brand C'],
            'Rim Diameter': [15, 16, 17],
            'Dimension': ['205/55 R16', '225/60 R17', '195/65 R15'],
            'Load Index': [91, 94, 87],
            'Speed Index': ['V', 'H', 'H'],
            'car_type': ['PASSENGER CAR', 'PASSENGER CAR', 'PASSENGER CAR'],
            'country': ['Germany', 'Germany', 'Germany'],
            'Facts': ['SALES UNITS', 'PRICE EUR', 'SALES THS. VALUE EUR'],
            'JUN 24': [100, 50.5, 5050],
            'JUL 24': [120, 52.0, 6240],
            'AUG 24': [90, 49.5, 4455],
            'SEP 24': [None, None, None],  # 空值
            'OCT 24': [0, 0, 0]  # 零值
        }
        return pd.DataFrame(data)
    
    def create_sample_long_data(self):
        """创建长格式示例数据用于透视测试"""
        data = {
            'Seasonality': ['Summer'] * 6 + ['Winter'] * 6,
            'Brandlines': ['Brand A'] * 6 + ['Brand B'] * 6,
            'Rim Diameter': [15] * 6 + [16] * 6,
            'Dimension': ['205/55 R16'] * 6 + ['225/60 R17'] * 6,
            'Load Index': [91] * 6 + [94] * 6,
            'Speed Index': ['V'] * 6 + ['H'] * 6,
            'car_type': ['PASSENGER CAR'] * 12,
            'country': ['Germany'] * 12,
            'Date': ['2024-06-01', '2024-07-01'] * 6,
            'Facts': ['SALES UNITS', 'PRICE EUR', 'SALES THS. VALUE EUR'] * 4,
            'Value': [100, 50.5, 5050, 120, 52.0, 6240, 90, 49.5, 4455, 110, 51.0, 5610]
        }
        return pd.DataFrame(data)
    
    def create_transformer(self):
        """创建转换器实例"""
        transform_config = {
            'column_mapping': {
                'DIMENSION (Car Tires)': 'Dimension',
                'LoadIndex': 'Load Index',
                'SpeedIndex': 'Speed Index',
                'Type of Vehicle': 'car_type'
            },
            'date_mapping': {
                'JUN 24': '2024-06-01',
                'JUL 24': '2024-07-01',
                'AUG 24': '2024-08-01',
                'SEP 24': '2024-09-01',
                'OCT 24': '2024-10-01'
            }
        }
        return DataTransformer(transform_config)
    
    def test_rename_columns(self):
        """测试列重命名"""
        # 创建带有需要重命名列的数据
        data = {
            'DIMENSION (Car Tires)': ['205/55 R16', '225/60 R17'],
            'LoadIndex': [91, 94],
            'SpeedIndex': ['V', 'H'],
            'Keep_Column': ['A', 'B']
        }
        df = pd.DataFrame(data)
        
        transformer = self.create_transformer()
        renamed_df = transformer.rename_columns(df)
        
        # 检查列是否被正确重命名
        assert 'Dimension' in renamed_df.columns
        assert 'Load Index' in renamed_df.columns
        assert 'Speed Index' in renamed_df.columns
        assert 'Keep_Column' in renamed_df.columns
        
        # 检查旧列名是否不存在
        assert 'DIMENSION (Car Tires)' not in renamed_df.columns
        assert 'LoadIndex' not in renamed_df.columns
        assert 'SpeedIndex' not in renamed_df.columns
    
    def test_wide_to_long_conversion(self):
        """测试宽转长格式转换"""
        df = self.create_sample_wide_data()
        transformer = self.create_transformer()
        
        long_df = transformer.wide_to_long(df)
        
        # 检查转换结果
        assert len(long_df) > 0
        assert 'Date' in long_df.columns
        assert 'Value' in long_df.columns
        
        # 检查日期映射是否正确
        unique_dates = long_df['Date'].unique()
        expected_dates = ['2024-06-01', '2024-07-01', '2024-08-01']
        for date in expected_dates:
            assert date in unique_dates
        
        # 检查空值和零值是否被正确过滤
        assert not long_df['Value'].isnull().any()
        assert not (long_df['Value'] == 0).any()
        
        # 检查数据完整性
        assert all(col in long_df.columns for col in 
                  ['Seasonality', 'Brandlines', 'Facts', 'Date', 'Value'])
    
    def test_pivot_by_facts(self):
        """测试Facts透视操作"""
        df = self.create_sample_long_data()
        transformer = self.create_transformer()
        
        pivot_config = {
            'index_columns': ['Seasonality', 'Brandlines', 'Rim Diameter', 'Dimension', 
                             'Load Index', 'Speed Index', 'car_type', 'country', 'Date'],
            'pivot_column': 'Facts',
            'value_column': 'Value'
        }
        
        pivoted_df = transformer.pivot_by_facts(df, pivot_config)
        
        # 检查透视结果
        assert len(pivoted_df) > 0
        
        # 检查Facts列是否变成了列名
        fact_columns = ['PRICE EUR', 'SALES THS. VALUE EUR', 'SALES UNITS']
        for col in fact_columns:
            assert col in pivoted_df.columns
        
        # 检查索引列是否保留
        for col in pivot_config['index_columns']:
            assert col in pivoted_df.columns
    
    def test_transform_dataframe_complete(self):
        """测试完整的转换流程"""
        df = self.create_sample_wide_data()
        transformer = self.create_transformer()
        
        original_rows = len(df)
        transformed_df = transformer.transform_dataframe(df, "测试数据")
        
        # 检查转换结果
        assert not transformed_df.empty
        assert 'Date' in transformed_df.columns
        assert 'Value' in transformed_df.columns
        
        # 长格式应该有更多行（每个时间点一行）
        assert len(transformed_df) >= original_rows
    
    def test_add_calculated_columns(self):
        """测试添加计算列"""
        data = {
            'Price EUR': [50.0, 51.0, 52.0],
            'Units': [100, 120, 90],
            'Other_Column': ['A', 'B', 'C']
        }
        df = pd.DataFrame(data)
        
        transformer = self.create_transformer()
        result_df = transformer.add_calculated_columns(df)
        
        # 检查是否添加了计算列
        assert 'Calculated_Value' in result_df.columns
        
        # 检查计算是否正确
        expected_values = [5000.0, 6120.0, 4680.0]  # Price * Units
        for i, expected in enumerate(expected_values):
            assert abs(result_df.iloc[i]['Calculated_Value'] - expected) < 0.01
    
    def test_standardize_date_format(self):
        """测试日期格式标准化"""
        data = {
            'Date': ['2024-06-01', '2024-07-01', '2024-08-01'],
            'Value': [100, 200, 300]
        }
        df = pd.DataFrame(data)
        
        transformer = self.create_transformer()
        result_df = transformer.standardize_date_format(df)
        
        # 检查日期列是否转换为datetime类型
        assert pd.api.types.is_datetime64_any_dtype(result_df['Date'])
    
    def test_filter_data(self):
        """测试数据过滤"""
        data = {
            'Value': [10, 20, 30, 40, 50],
            'Category': ['A', 'B', 'A', 'C', 'B'],
            'Score': [1.5, 2.5, 3.5, 4.5, 5.5]
        }
        df = pd.DataFrame(data)
        
        transformer = self.create_transformer()
        
        # 测试简单过滤
        filters = {'Category': 'A'}
        filtered_df = transformer.filter_data(df, filters)
        assert len(filtered_df) == 2
        assert all(filtered_df['Category'] == 'A')
        
        # 测试范围过滤
        filters = {'Value': {'min': 25, 'max': 45}}
        filtered_df = transformer.filter_data(df, filters)
        assert len(filtered_df) == 2
        assert all(filtered_df['Value'] >= 25)
        assert all(filtered_df['Value'] <= 45)
        
        # 测试值列表过滤
        filters = {'Category': {'values': ['A', 'B']}}
        filtered_df = transformer.filter_data(df, filters)
        assert len(filtered_df) == 4
        assert all(filtered_df['Category'].isin(['A', 'B']))
    
    def test_validate_transformation(self):
        """测试转换验证"""
        original_df = self.create_sample_wide_data()
        transformer = self.create_transformer()
        
        # 正常转换
        transformed_df = transformer.transform_dataframe(original_df)
        result = transformer.validate_transformation(original_df, transformed_df)
        assert result == True
        
        # 空DataFrame验证
        empty_df = pd.DataFrame()
        result = transformer.validate_transformation(original_df, empty_df)
        assert result == False
        
        # None DataFrame验证
        result = transformer.validate_transformation(original_df, None)
        assert result == False
    
    def test_empty_dataframe_handling(self):
        """测试空DataFrame的处理"""
        empty_df = pd.DataFrame()
        transformer = self.create_transformer()
        
        # 转换空DataFrame应该返回原DataFrame
        result = transformer.transform_dataframe(empty_df, "空数据")
        assert result.empty
    
    def test_missing_date_mapping(self):
        """测试缺少日期映射的情况"""
        config = {
            'column_mapping': {},
            'date_mapping': {}  # 空的日期映射
        }
        transformer = DataTransformer(config)
        
        df = self.create_sample_wide_data()
        result = transformer.wide_to_long(df)
        
        # 没有日期映射时应该跳过宽转长
        assert len(result) == len(df)  # 长度应该保持不变
    
    def test_pivot_with_missing_columns(self):
        """测试透视时缺少必需列的情况"""
        data = {
            'Col1': ['A', 'B'],
            'Col2': ['X', 'Y'],
            'Value': [1, 2]
        }
        df = pd.DataFrame(data)
        
        transformer = self.create_transformer()
        
        # 透视配置中包含不存在的列
        pivot_config = {
            'index_columns': ['Col1', 'NonexistentCol'],
            'pivot_column': 'Facts',  # 不存在
            'value_column': 'Value'
        }
        
        result = transformer.pivot_by_facts(df, pivot_config)
        
        # 应该返回原DataFrame（透视失败）
        assert len(result) == len(df)
    
    def test_no_column_mapping(self):
        """测试没有列映射配置的情况"""
        config = {
            'column_mapping': {},
            'date_mapping': {'JUN 24': '2024-06-01'}
        }
        transformer = DataTransformer(config)
        
        df = self.create_sample_wide_data()
        original_columns = list(df.columns)
        
        renamed_df = transformer.rename_columns(df)
        
        # 没有映射配置时，列名应该保持不变
        assert list(renamed_df.columns) == original_columns


if __name__ == '__main__':
    pytest.main([__file__])