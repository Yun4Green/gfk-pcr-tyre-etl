"""
配置管理器测试
"""

import pytest
import tempfile
import os
import yaml
from pathlib import Path

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gfk_etl_library.config import ConfigManager


class TestConfigManager:
    """配置管理器测试类"""
    
    def create_temp_config(self, config_data):
        """创建临时配置文件"""
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False)
        yaml.dump(config_data, temp_file, default_flow_style=False)
        temp_file.close()
        return temp_file.name
    
    def test_basic_config_loading(self):
        """测试基本配置加载"""
        config_data = {
            'project': {
                'name': 'Test Project',
                'version': '1.0'
            },
            'data_sources': {
                'region': 'TEST'
            }
        }
        
        config_path = self.create_temp_config(config_data)
        
        try:
            config = ConfigManager(config_path)
            
            assert config.get('project.name') == 'Test Project'
            assert config.get('project.version') == '1.0'
            assert config.get('data_sources.region') == 'TEST'
            
        finally:
            os.unlink(config_path)
    
    def test_nested_config_access(self):
        """测试嵌套配置访问"""
        config_data = {
            'processing': {
                'cleaning': {
                    'remove_total_rows': True,
                    'tolerance': 0.01
                }
            }
        }
        
        config_path = self.create_temp_config(config_data)
        
        try:
            config = ConfigManager(config_path)
            
            assert config.get('processing.cleaning.remove_total_rows') == True
            assert config.get('processing.cleaning.tolerance') == 0.01
            assert config.get('processing.cleaning.nonexistent') is None
            assert config.get('processing.cleaning.nonexistent', 'default') == 'default'
            
        finally:
            os.unlink(config_path)
    
    def test_country_config_access(self):
        """测试国家配置访问"""
        config_data = {
            'data_sources': {
                'countries': {
                    'Germany': {
                        'code': 'DE',
                        'file': 'germany.csv'
                    },
                    'France': {
                        'code': 'FR',
                        'file': 'france.csv'
                    }
                }
            }
        }
        
        config_path = self.create_temp_config(config_data)
        
        try:
            config = ConfigManager(config_path)
            
            countries = config.get_countries()
            assert len(countries) == 2
            assert 'Germany' in countries
            assert countries['Germany']['code'] == 'DE'
            assert countries['Germany']['file'] == 'germany.csv'
            
        finally:
            os.unlink(config_path)
    
    def test_spain_files_config(self):
        """测试西班牙文件配置"""
        config_data = {
            'data_sources': {
                'spain_files': {
                    'LIGHT TRUCK': {
                        'file': 'spain_lt.csv'
                    },
                    'PASSENGER CAR': {
                        'file': 'spain_pas.csv'
                    }
                }
            }
        }
        
        config_path = self.create_temp_config(config_data)
        
        try:
            config = ConfigManager(config_path)
            
            spain_files = config.get_spain_files()
            assert len(spain_files) == 2
            assert 'LIGHT TRUCK' in spain_files
            assert spain_files['PASSENGER CAR']['file'] == 'spain_pas.csv'
            
        finally:
            os.unlink(config_path)
    
    def test_column_mapping_config(self):
        """测试列映射配置"""
        config_data = {
            'processing': {
                'column_mapping': {
                    'OldColumn1': 'NewColumn1',
                    'OldColumn2': 'NewColumn2'
                }
            }
        }
        
        config_path = self.create_temp_config(config_data)
        
        try:
            config = ConfigManager(config_path)
            
            mapping = config.get_column_mapping()
            assert len(mapping) == 2
            assert mapping['OldColumn1'] == 'NewColumn1'
            assert mapping['OldColumn2'] == 'NewColumn2'
            
        finally:
            os.unlink(config_path)
    
    def test_date_mapping_config(self):
        """测试日期映射配置"""
        config_data = {
            'processing': {
                'date_mapping': {
                    'JUN 24': '2024-06-01',
                    'JUL 24': '2024-07-01'
                }
            }
        }
        
        config_path = self.create_temp_config(config_data)
        
        try:
            config = ConfigManager(config_path)
            
            date_mapping = config.get_date_mapping()
            assert len(date_mapping) == 2
            assert date_mapping['JUN 24'] == '2024-06-01'
            assert date_mapping['JUL 24'] == '2024-07-01'
            
        finally:
            os.unlink(config_path)
    
    def test_pivot_config(self):
        """测试透视配置"""
        config_data = {
            'processing': {
                'pivot': {
                    'index_columns': ['Col1', 'Col2'],
                    'pivot_column': 'Facts',
                    'value_column': 'Value'
                }
            }
        }
        
        config_path = self.create_temp_config(config_data)
        
        try:
            config = ConfigManager(config_path)
            
            pivot_config = config.get_pivot_config()
            assert pivot_config['index_columns'] == ['Col1', 'Col2']
            assert pivot_config['pivot_column'] == 'Facts'
            assert pivot_config['value_column'] == 'Value'
            
        finally:
            os.unlink(config_path)
    
    def test_validation_enabled_check(self):
        """测试验证启用检查"""
        config_data = {
            'validation': {
                'consistency_check': {
                    'enabled': True
                },
                'negative_values': {
                    'enabled': False
                }
            }
        }
        
        config_path = self.create_temp_config(config_data)
        
        try:
            config = ConfigManager(config_path)
            
            assert config.is_validation_enabled('consistency_check') == True
            assert config.is_validation_enabled('negative_values') == False
            assert config.is_validation_enabled('nonexistent') == False
            
        finally:
            os.unlink(config_path)
    
    def test_output_pattern(self):
        """测试输出文件名模式"""
        config_data = {
            'output': {
                'filename_pattern': 'GFK_{region}_PROCESSED_{timestamp}.csv'
            }
        }
        
        config_path = self.create_temp_config(config_data)
        
        try:
            config = ConfigManager(config_path)
            
            pattern = config.get_output_pattern()
            assert pattern == 'GFK_{region}_PROCESSED_{timestamp}.csv'
            
        finally:
            os.unlink(config_path)
    
    def test_missing_config_file(self):
        """测试缺失配置文件"""
        with pytest.raises(FileNotFoundError):
            ConfigManager('nonexistent_config.yml')
    
    def test_invalid_yaml_file(self):
        """测试无效的YAML文件"""
        # 创建无效的YAML文件
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False)
        temp_file.write("invalid: yaml: content:\n  - missing bracket")
        temp_file.close()
        
        try:
            with pytest.raises(yaml.YAMLError):
                ConfigManager(temp_file.name)
        finally:
            os.unlink(temp_file.name)
    
    def test_config_inheritance(self):
        """测试配置继承机制"""
        # 创建基础配置
        base_config = {
            'project': {'name': 'Base Project'},
            'data_sources': {'region': 'BASE'},
            'processing': {'cleaning': {'remove_total_rows': True}}
        }
        base_path = self.create_temp_config(base_config)
        
        # 创建继承配置
        child_config = {
            'include': os.path.basename(base_path),
            'data_sources': {'region': 'CHILD'},  # 覆盖
            'processing': {'cleaning': {'tolerance': 0.01}}  # 添加
        }
        
        # 确保子配置在同一目录
        config_dir = os.path.dirname(base_path)
        child_path = os.path.join(config_dir, 'child_config.yml')
        
        try:
            with open(child_path, 'w') as f:
                yaml.dump(child_config, f, default_flow_style=False)
            
            config = ConfigManager(child_path)
            
            # 检查继承和覆盖
            assert config.get('project.name') == 'Base Project'  # 继承
            assert config.get('data_sources.region') == 'CHILD'  # 覆盖
            assert config.get('processing.cleaning.remove_total_rows') == True  # 继承
            assert config.get('processing.cleaning.tolerance') == 0.01  # 添加
            
        finally:
            os.unlink(base_path)
            if os.path.exists(child_path):
                os.unlink(child_path)


if __name__ == '__main__':
    pytest.main([__file__])