"""
数据验证器测试
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gfk_etl_library.core.validator import DataValidator


class TestDataValidator:
    """数据验证器测试类"""
    
    def create_validator(self):
        """创建验证器实例"""
        validation_config = {
            'consistency_check': {
                'enabled': True,
                'tolerance': 0.01,
                'price_column': 'Price EUR',
                'units_column': 'Units',
                'value_column': 'Value EUR'
            },
            'negative_values': {
                'check_enabled': True,
                'report_threshold': 10
            }
        }
        return DataValidator(validation_config)
    
    def create_sample_data_with_consistency(self):
        """创建包含一致性数据的示例"""
        data = {
            'Price EUR': [50.0, 51.0, 52.0, 53.0, 54.0],
            'Units': [100, 120, 90, 110, 95],
            'Value EUR': [5000.0, 6120.0, 4680.0, 5830.0, 5130.0],  # 完全一致
            'Country': ['Germany', 'France', 'Spain', 'Italy', 'Poland'],
            'Date': ['2024-06-01'] * 5
        }
        return pd.DataFrame(data)
    
    def create_sample_data_with_inconsistency(self):
        """创建包含不一致数据的示例"""
        data = {
            'Price EUR': [50.0, 51.0, 52.0, 53.0, 54.0],
            'Units': [100, 120, 90, 110, 95],
            'Value EUR': [5000.0, 6000.0, 4680.0, 6000.0, 5130.0],  # 部分不一致
            'Country': ['Germany', 'France', 'Spain', 'Italy', 'Poland'],
            'Date': ['2024-06-01'] * 5
        }
        return pd.DataFrame(data)
    
    def create_sample_data_with_negatives(self):
        """创建包含负值的示例数据"""
        data = {
            'Price EUR': [50.0, -10.0, 52.0, 53.0, 54.0],  # 一个负值
            'Units': [100, 120, -5, 110, 95],              # 一个负值
            'Value EUR': [5000.0, 6120.0, 4680.0, 5830.0, -100.0],  # 一个负值
            'Country': ['Germany', 'France', 'Spain', 'Italy', 'Poland'],
            'Date': ['2024-06-01'] * 5
        }
        return pd.DataFrame(data)
    
    def test_check_data_completeness(self):
        """测试数据完整性检查"""
        # 创建带缺失值和重复行的数据
        data = {
            'Col1': [1, 2, None, 4, 1],  # 缺失值和重复
            'Col2': ['A', 'B', 'C', None, 'A'],  # 缺失值和重复
            'Col3': [10, 20, 30, 40, 10]
        }
        df = pd.DataFrame(data)
        
        validator = self.create_validator()
        result = validator.check_data_completeness(df)
        
        # 检查缺失值检测
        assert 'missing_values' in result
        assert len(result['missing_values']) > 0
        
        # 检查重复行检测
        assert 'duplicate_rows' in result
        assert result['duplicate_rows'] > 0
    
    def test_price_consistency_check_perfect(self):
        """测试完美一致性检查"""
        df = self.create_sample_data_with_consistency()
        validator = self.create_validator()
        
        result = validator.check_price_consistency(df)
        
        # 检查一致性结果
        assert 'consistency_check' in result
        cc = result['consistency_check']
        assert cc['enabled'] == True
        assert cc['columns_available'] == True
        assert cc['consistency_rate'] == 100.0  # 完美一致
        assert cc['inconsistent_rows'] == 0
    
    def test_price_consistency_check_with_issues(self):
        """测试有一致性问题的检查"""
        df = self.create_sample_data_with_inconsistency()
        validator = self.create_validator()
        
        result = validator.check_price_consistency(df)
        
        # 检查一致性结果
        cc = result['consistency_check']
        assert cc['consistency_rate'] < 100.0  # 不完美一致
        assert cc['inconsistent_rows'] > 0
        
        # 应该有问题报告
        assert 'issues' in result
        assert len(result['issues']) > 0
    
    def test_price_consistency_missing_columns(self):
        """测试缺少必需列的一致性检查"""
        # 创建缺少必需列的数据
        data = {
            'Price EUR': [50.0, 51.0],
            'Other_Column': ['A', 'B']
            # 缺少 Units 和 Value EUR 列
        }
        df = pd.DataFrame(data)
        
        validator = self.create_validator()
        result = validator.check_price_consistency(df)
        
        # 应该标记为列不可用
        cc = result['consistency_check']
        assert cc['columns_available'] == False
        assert 'issues' in result
    
    def test_negative_values_check(self):
        """测试负值检查"""
        df = self.create_sample_data_with_negatives()
        validator = self.create_validator()
        
        result = validator.check_negative_values(df)
        
        # 检查负值检测结果
        assert 'negative_values' in result
        nv = result['negative_values']
        assert nv['enabled'] == True
        assert nv['total_negative_rows'] > 0
        
        # 检查各列的负值统计
        assert 'columns_with_negatives' in nv
        columns_with_negatives = nv['columns_with_negatives']
        
        assert 'Price EUR' in columns_with_negatives
        assert 'Units' in columns_with_negatives
        assert 'Value EUR' in columns_with_negatives
        
        # 检查每列的负值计数
        for col_info in columns_with_negatives.values():
            assert col_info['count'] > 0
            assert 'percentage' in col_info
            assert 'min_value' in col_info
    
    def test_validate_dataframe_complete(self):
        """测试完整的数据验证流程"""
        df = self.create_sample_data_with_consistency()
        validator = self.create_validator()
        
        results = validator.validate_dataframe(df, "测试数据")
        
        # 检查验证结果结构
        assert 'data_name' in results
        assert 'total_rows' in results
        assert 'total_columns' in results
        assert 'passed' in results
        assert 'issues' in results
        
        # 检查数据统计
        assert results['total_rows'] == len(df)
        assert results['total_columns'] == len(df.columns)
        
        # 完美数据应该通过验证
        assert results['passed'] == True
        assert len(results['issues']) == 0
    
    def test_validate_dataframe_with_issues(self):
        """测试有问题的数据验证"""
        df = self.create_sample_data_with_inconsistency()
        validator = self.create_validator()
        
        results = validator.validate_dataframe(df, "问题数据")
        
        # 有问题的数据应该不通过验证
        assert results['passed'] == False
        assert len(results['issues']) > 0
    
    def test_check_data_types(self):
        """测试数据类型检查"""
        data = {
            'Int_Col': [1, 2, 3],
            'Float_Col': [1.1, 2.2, 3.3],
            'String_Col': ['A', 'B', 'C'],
            'Numeric_String': ['1', '2', '3']  # 应该是数值的字符串
        }
        df = pd.DataFrame(data)
        
        validator = self.create_validator()
        result = validator.check_data_types(df)
        
        # 检查数据类型信息
        assert 'data_types' in result
        data_types = result['data_types']
        
        for col in df.columns:
            assert col in data_types
            assert 'dtype' in data_types[col]
            assert 'null_count' in data_types[col]
            assert 'unique_count' in data_types[col]
        
        # 应该检测到类型问题
        assert 'type_issues' in result
        # Numeric_String 应该被识别为可能的数值类型
        type_issues = result['type_issues']
        numeric_string_issue = any('Numeric_String' in issue for issue in type_issues)
        assert numeric_string_issue
    
    def test_generate_validation_report(self):
        """测试验证报告生成"""
        df = self.create_sample_data_with_inconsistency()
        validator = self.create_validator()
        
        validation_results = validator.validate_dataframe(df, "报告测试数据")
        report = validator.generate_validation_report(validation_results)
        
        # 检查报告内容
        assert '数据验证报告' in report
        assert '报告测试数据' in report
        assert '数据行数' in report
        assert '数据列数' in report
        
        # 如果有一致性检查，应该在报告中
        if 'consistency_check' in validation_results:
            assert '价格一致性' in report
    
    def test_save_validation_report(self):
        """测试验证报告保存"""
        df = self.create_sample_data_with_consistency()
        validator = self.create_validator()
        
        validation_results = validator.validate_dataframe(df, "文件测试数据")
        
        # 创建临时文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            validator.save_validation_report(validation_results, temp_path)
            
            # 检查文件是否被创建
            assert os.path.exists(temp_path)
            
            # 检查文件内容
            with open(temp_path, 'r', encoding='utf-8') as f:
                content = f.read()
                assert '数据验证报告' in content
                assert '文件测试数据' in content
        
        finally:
            # 清理临时文件
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_empty_dataframe_validation(self):
        """测试空DataFrame的验证"""
        empty_df = pd.DataFrame()
        validator = self.create_validator()
        
        results = validator.validate_dataframe(empty_df, "空数据")
        
        # 空数据应该返回失败状态
        assert results['passed'] == False
        assert results.get('reason') == 'empty_data'
    
    def test_disabled_validations(self):
        """测试禁用的验证"""
        config = {
            'consistency_check': {'enabled': False},
            'negative_values': {'check_enabled': False}
        }
        validator = DataValidator(config)
        
        df = self.create_sample_data_with_negatives()
        results = validator.validate_dataframe(df, "禁用验证测试")
        
        # 禁用的验证不应该运行
        assert 'consistency_check' not in results or not results['consistency_check'].get('enabled', False)
    
    def test_large_differences_detection(self):
        """测试大差异检测"""
        # 创建有大差异的数据
        data = {
            'Price EUR': [50.0, 51.0, 52.0],
            'Units': [100, 120, 90],
            'Value EUR': [5000.0, 10000.0, 4680.0],  # 第二行有大差异
            'Country': ['Germany', 'France', 'Spain']
        }
        df = pd.DataFrame(data)
        
        validator = self.create_validator()
        result = validator.check_price_consistency(df)
        
        # 应该检测到大差异
        cc = result['consistency_check']
        assert cc['large_differences'] > 0
    
    def test_validation_with_missing_values(self):
        """测试带缺失值的验证"""
        data = {
            'Price EUR': [50.0, None, 52.0],
            'Units': [100, 120, None],
            'Value EUR': [5000.0, 6120.0, 4680.0],
            'Country': ['Germany', 'France', 'Spain']
        }
        df = pd.DataFrame(data)
        
        validator = self.create_validator()
        result = validator.check_price_consistency(df)
        
        # 验证应该处理缺失值
        cc = result['consistency_check']
        assert cc['total_rows'] < len(df)  # 应该排除有缺失值的行


if __name__ == '__main__':
    pytest.main([__file__])