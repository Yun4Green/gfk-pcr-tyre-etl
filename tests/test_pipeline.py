"""
管道集成测试
"""

import pytest
import tempfile
import os
import yaml
import pandas as pd
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gfk_etl_library.pipeline import GFKDataPipeline


class TestGFKDataPipeline:
    """GFK数据处理管道测试类"""
    
    def create_sample_csv_files(self, temp_dir):
        """创建示例CSV文件"""
        # 创建示例数据
        data = {
            'Seasonality': ['Summer', 'Winter', 'Summer', 'TOTAL'],
            'Brandlines': ['Brand A', 'Brand B', 'Brand C', 'Brand.TOTAL'],
            'Rim Diameter': [15, 16, 17, '.TOTAL.'],
            'DIMENSION (Car Tires)': ['205/55 R16', '225/60 R17', '195/65 R15', 'ALL'],
            'LoadIndex': [91, 94, 87, 0],
            'SpeedIndex': ['V', 'H', 'H', 'X'],
            'Type of Vehicle': ['PASSENGER CAR'] * 4,
            'Facts': ['SALES UNITS', 'PRICE EUR', 'SALES THS. VALUE EUR', 'TOTAL'],
            'JUN 24': [100, 50.5, 5050, 0],
            'JUL 24': [120, 52.0, 6240, 0],
            'AUG 24': [90, 49.5, 4455, 0],
            'MAT JUN 24': [1000, 500, 50000, 0],  # 要删除的列
            'YTD JUN 25': [2000, 1000, 100000, 0]  # 要删除的列
        }
        
        # 创建德国数据文件
        germany_file = os.path.join(temp_dir, 'germany_data.csv')
        pd.DataFrame(data).to_csv(germany_file, index=False)
        
        # 创建法国数据文件（稍微不同的数据）
        france_data = data.copy()
        france_data['JUN 24'] = [110, 51.0, 5610, 0]
        france_file = os.path.join(temp_dir, 'france_data.csv')
        pd.DataFrame(france_data).to_csv(france_file, index=False)
        
        return germany_file, france_file
    
    def create_test_config(self, temp_dir, germany_file, france_file):
        """创建测试配置文件"""
        config = {
            'project': {
                'name': 'Test GFK ETL Pipeline',
                'version': '2.0'
            },
            'data_sources': {
                'region': 'TEST',
                'input_directory': temp_dir,
                'output_directory': temp_dir,
                'countries': {
                    'Germany': {
                        'code': 'DE',
                        'file': os.path.basename(germany_file)
                    },
                    'France': {
                        'code': 'FR',
                        'file': os.path.basename(france_file)
                    }
                }
            },
            'processing': {
                'cleaning': {
                    'remove_total_rows': True,
                    'total_patterns': [r'\.TOTAL', r'^TOTAL$', r'\.TOTAL\.'],
                    'columns_to_drop': ['MAT JUN 24', 'YTD JUN 25']
                },
                'column_mapping': {
                    'DIMENSION (Car Tires)': 'Dimension',
                    'LoadIndex': 'Load Index',
                    'SpeedIndex': 'Speed Index',
                    'Type of Vehicle': 'car_type'
                },
                'date_mapping': {
                    'JUN 24': '2024-06-01',
                    'JUL 24': '2024-07-01',
                    'AUG 24': '2024-08-01'
                },
                'pivot': {
                    'index_columns': ['Seasonality', 'Brandlines', 'Rim Diameter', 'Dimension', 
                                     'Load Index', 'Speed Index', 'car_type', 'country', 'Date'],
                    'value_column': 'Value',
                    'pivot_column': 'Facts'
                }
            },
            'validation': {
                'consistency_check': {
                    'enabled': True,
                    'tolerance': 0.01,
                    'price_column': 'PRICE EUR',
                    'units_column': 'SALES UNITS',
                    'value_column': 'SALES THS. VALUE EUR'
                },
                'negative_values': {
                    'check_enabled': True,
                    'report_threshold': 5
                }
            },
            'output': {
                'filename_pattern': 'GFK_{region}_PROCESSED_{timestamp}.csv',
                'include_timestamp': True,
                'save_validation_report': True
            }
        }
        
        config_file = os.path.join(temp_dir, 'test_config.yml')
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False)
        
        return config_file
    
    def test_pipeline_initialization(self):
        """测试管道初始化"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试文件
            germany_file, france_file = self.create_sample_csv_files(temp_dir)
            config_file = self.create_test_config(temp_dir, germany_file, france_file)
            
            # 初始化管道
            pipeline = GFKDataPipeline(config_file)
            
            # 检查管道属性
            assert pipeline.config is not None
            assert pipeline.loader is not None
            assert pipeline.cleaner is not None
            assert pipeline.transformer is not None
            assert pipeline.validator is not None
            assert pipeline.exporter is not None
            
            # 检查结果初始化
            assert 'region' in pipeline.results
            assert pipeline.results['region'] == 'TEST'
    
    def test_pipeline_run_complete(self):
        """测试完整管道运行"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试文件
            germany_file, france_file = self.create_sample_csv_files(temp_dir)
            config_file = self.create_test_config(temp_dir, germany_file, france_file)
            
            # 运行管道
            pipeline = GFKDataPipeline(config_file)
            results = pipeline.run(export_data=True, export_validation=True)
            
            # 检查结果
            assert results['success'] == True
            assert 'final_data' in results
            assert 'validation_results' in results
            assert 'processing_stages' in results
            assert 'export_results' in results
            
            # 检查最终数据
            final_data = results['final_data']
            assert isinstance(final_data, pd.DataFrame)
            assert not final_data.empty
            assert len(final_data) > 0
            
            # 检查列结构
            expected_columns = ['Seasonality', 'Brandlines', 'Rim Diameter', 'Dimension',
                               'Load Index', 'Speed Index', 'car_type', 'country', 'Date']
            for col in expected_columns:
                assert col in final_data.columns
            
            # 检查Facts列是否被正确透视
            fact_columns = ['PRICE EUR', 'SALES THS. VALUE EUR', 'SALES UNITS']
            for col in fact_columns:
                assert col in final_data.columns
    
    def test_pipeline_processing_stages(self):
        """测试管道处理阶段"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试文件
            germany_file, france_file = self.create_sample_csv_files(temp_dir)
            config_file = self.create_test_config(temp_dir, germany_file, france_file)
            
            # 运行管道
            pipeline = GFKDataPipeline(config_file)
            results = pipeline.run()
            
            # 检查处理阶段
            stages = results['processing_stages']
            
            # 数据加载阶段
            assert 'data_loading' in stages
            loading = stages['data_loading']
            assert loading['files_loaded'] == 2  # 德国和法国
            assert 'Germany' in loading['data_sources']
            assert 'France' in loading['data_sources']
            
            # 数据清洗阶段
            assert 'data_cleaning' in stages
            cleaning = stages['data_cleaning']
            assert cleaning['files_cleaned'] == 2
            assert cleaning['rows_removed'] > 0  # 应该删除了TOTAL行
            
            # 数据转换阶段
            assert 'data_transformation' in stages
            transform = stages['data_transformation']
            assert transform['files_transformed'] == 2
            assert transform['final_rows'] > 0
            
            # 数据验证阶段
            assert 'data_validation' in stages
            validation = stages['data_validation']
            assert 'validation_passed' in validation
            
            # 数据导出阶段
            assert 'data_export' in stages
            export = stages['data_export']
            assert export['files_exported'] >= 1
    
    def test_pipeline_validation_results(self):
        """测试管道验证结果"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试文件
            germany_file, france_file = self.create_sample_csv_files(temp_dir)
            config_file = self.create_test_config(temp_dir, germany_file, france_file)
            
            # 运行管道
            pipeline = GFKDataPipeline(config_file)
            results = pipeline.run()
            
            # 检查验证结果
            validation_results = results['validation_results']
            assert 'passed' in validation_results
            assert 'total_rows' in validation_results
            assert 'total_columns' in validation_results
            
            # 如果启用了一致性检查
            if validation_results.get('consistency_check', {}).get('enabled', False):
                cc = validation_results['consistency_check']
                assert 'consistency_rate' in cc
                assert 'total_rows' in cc
    
    def test_pipeline_export_files(self):
        """测试管道导出文件"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试文件
            germany_file, france_file = self.create_sample_csv_files(temp_dir)
            config_file = self.create_test_config(temp_dir, germany_file, france_file)
            
            # 运行管道
            pipeline = GFKDataPipeline(config_file)
            results = pipeline.run(export_data=True, export_validation=True)
            
            # 检查导出结果
            export_results = results['export_results']
            assert 'data' in export_results
            
            # 检查数据文件是否存在
            data_file = export_results['data']
            assert os.path.exists(data_file)
            assert data_file.endswith('.csv')
            assert 'GFK_TEST_PROCESSED_' in data_file
            
            # 验证导出的数据
            exported_df = pd.read_csv(data_file)
            assert not exported_df.empty
            assert len(exported_df) > 0
    
    def test_pipeline_no_export(self):
        """测试不导出数据的管道运行"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试文件
            germany_file, france_file = self.create_sample_csv_files(temp_dir)
            config_file = self.create_test_config(temp_dir, germany_file, france_file)
            
            # 运行管道但不导出
            pipeline = GFKDataPipeline(config_file)
            results = pipeline.run(export_data=False, export_validation=False)
            
            # 应该成功但没有导出文件
            assert results['success'] == True
            assert 'final_data' in results
            assert results.get('export_results', {}) == {}
    
    def test_pipeline_missing_data_files(self):
        """测试缺失数据文件的处理"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建配置但不创建数据文件
            config = {
                'data_sources': {
                    'region': 'TEST',
                    'input_directory': temp_dir,
                    'output_directory': temp_dir,
                    'countries': {
                        'Germany': {
                            'code': 'DE',
                            'file': 'nonexistent_germany.csv'
                        },
                        'France': {
                            'code': 'FR',
                            'file': 'nonexistent_france.csv'
                        }
                    }
                },
                'processing': {'cleaning': {}, 'column_mapping': {}, 'date_mapping': {}},
                'validation': {},
                'output': {}
            }
            
            config_file = os.path.join(temp_dir, 'test_config.yml')
            with open(config_file, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False)
            
            # 运行管道
            pipeline = GFKDataPipeline(config_file)
            results = pipeline.run()
            
            # 应该失败
            assert results['success'] == False
            assert 'error' in results
    
    def test_pipeline_summary_report(self):
        """测试管道总结报告"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试文件
            germany_file, france_file = self.create_sample_csv_files(temp_dir)
            config_file = self.create_test_config(temp_dir, germany_file, france_file)
            
            # 运行管道
            pipeline = GFKDataPipeline(config_file)
            results = pipeline.run()
            
            # 获取总结报告
            summary_report = pipeline.get_summary_report()
            
            # 检查报告内容
            assert 'GFK数据处理管道执行报告' in summary_report
            assert 'TEST' in summary_report  # 区域名称
            assert '成功' in summary_report or '失败' in summary_report
            
            # 导出总结报告
            summary_file = pipeline.export_summary_report()
            assert os.path.exists(summary_file)
            
            # 检查文件内容
            with open(summary_file, 'r', encoding='utf-8') as f:
                content = f.read()
                assert '管道执行报告' in content
    
    def test_pipeline_invalid_config(self):
        """测试无效配置的处理"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建无效配置文件
            invalid_config = "invalid: yaml: content:\n  - missing"
            config_file = os.path.join(temp_dir, 'invalid_config.yml')
            
            with open(config_file, 'w') as f:
                f.write(invalid_config)
            
            # 应该抛出异常
            with pytest.raises(Exception):
                GFKDataPipeline(config_file)
    
    def test_pipeline_string_representation(self):
        """测试管道字符串表示"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试文件
            germany_file, france_file = self.create_sample_csv_files(temp_dir)
            config_file = self.create_test_config(temp_dir, germany_file, france_file)
            
            # 创建管道
            pipeline = GFKDataPipeline(config_file)
            
            # 检查字符串表示
            str_repr = str(pipeline)
            assert 'GFKDataPipeline' in str_repr
            assert 'TEST' in str_repr
            
            repr_str = repr(pipeline)
            assert 'GFKDataPipeline' in repr_str
            assert config_file in repr_str


if __name__ == '__main__':
    pytest.main([__file__])