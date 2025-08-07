"""
GFK数据处理管道

这是核心Pipeline类，协调所有数据处理步骤，提供统一的ETL接口。
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

from .config import ConfigManager
from .core import DataLoader, DataCleaner, DataTransformer, DataValidator, DataExporter
from .utils import print_dataframe_summary, create_progress_logger


class GFKDataPipeline:
    """GFK数据处理管道主类"""
    
    def __init__(self, config_path: str):
        """
        初始化数据处理管道
        
        Args:
            config_path: 配置文件路径
            
        Raises:
            FileNotFoundError: 当配置文件不存在时
            Exception: 当初始化失败时
        """
        try:
            print(f"🚀 初始化GFK数据处理管道")
            print(f"📋 配置文件: {config_path}")
            
            # 加载配置
            self.config = ConfigManager(config_path)
            
            # 初始化各个处理模块
            self._initialize_modules()
            
            # 初始化结果存储
            self.results = {
                'pipeline_start_time': datetime.now(),
                'config_path': config_path,
                'region': self.config.get('data_sources.region', 'UNKNOWN'),
                'processing_stages': {}
            }
            
            print(f"✅ 管道初始化完成")
            
        except Exception as e:
            print(f"❌ 管道初始化失败: {str(e)}")
            raise
    
    def _initialize_modules(self) -> None:
        """初始化所有处理模块"""
        
        # 初始化数据加载器
        input_dir = self.config.get('data_sources.input_directory', '.')
        self.loader = DataLoader(input_dir)
        
        # 初始化数据清洗器
        cleaning_config = self.config.get('processing.cleaning', {})
        self.cleaner = DataCleaner(cleaning_config)
        
        # 初始化数据转换器
        transform_config = self.config.get('processing', {})
        self.transformer = DataTransformer(transform_config)
        
        # 初始化数据验证器
        validation_config = self.config.get('validation', {})
        self.validator = DataValidator(validation_config)
        
        # 初始化数据导出器
        output_config = self.config.get('output', {})
        output_config['output_directory'] = self.config.get('data_sources.output_directory', './data/processed')
        self.exporter = DataExporter(output_config)
        
        print(f"📦 所有处理模块初始化完成")
    
    def run(self, export_data: bool = True, 
            export_validation: bool = True) -> Dict[str, Any]:
        """
        运行完整的数据处理管道
        
        Args:
            export_data: 是否导出处理后的数据
            export_validation: 是否导出验证报告
            
        Returns:
            处理结果字典
        """
        try:
            print(f"\n{'='*80}")
            print(f" 🚀 开始执行GFK数据处理管道")
            print(f" 📅 开始时间: {self.results['pipeline_start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f" 🌍 处理区域: {self.results['region']}")
            print(f"{'='*80}")
            
            # 创建进度跟踪器
            total_steps = 5  # 加载、清洗、转换、验证、导出
            progress_logger = create_progress_logger(total_steps)
            
            # 第1步：数据加载
            progress_logger(1, "数据加载")
            raw_data = self._load_data()
            if not raw_data:
                raise Exception("数据加载失败，无法继续处理")
            
            # 第2步：数据清洗
            progress_logger(2, "数据清洗")
            cleaned_data = self._clean_data(raw_data)
            
            # 第3步：数据转换
            progress_logger(3, "数据转换")
            transformed_data = self._transform_data(cleaned_data)
            
            # 第4步：数据验证
            progress_logger(4, "数据验证")
            validation_results = self._validate_data(transformed_data)
            
            # 第5步：数据导出
            progress_logger(5, "数据导出")
            if export_data or export_validation:
                export_results = self._export_data(transformed_data, validation_results,
                                                 export_data, export_validation)
                self.results['export_results'] = export_results
            
            # 完成处理
            self.results['pipeline_end_time'] = datetime.now()
            self.results['pipeline_duration'] = (
                self.results['pipeline_end_time'] - self.results['pipeline_start_time']
            ).total_seconds()
            
            self.results['final_data'] = transformed_data
            self.results['validation_results'] = validation_results
            self.results['success'] = True
            
            self._print_final_summary()
            
            return self.results
            
        except Exception as e:
            print(f"\n❌ 管道执行失败: {str(e)}")
            self.results['success'] = False
            self.results['error'] = str(e)
            self.results['pipeline_end_time'] = datetime.now()
            return self.results
    
    def _load_data(self) -> Optional[Dict[str, pd.DataFrame]]:
        """
        数据加载步骤
        
        Returns:
            加载的数据字典
        """
        try:
            print(f"\n📥 第1步：数据加载")
            
            # 根据配置类型选择加载方式
            if self.config.get('data_sources.countries'):
                # 欧洲多国数据
                countries_config = self.config.get_countries()
                data_dict = self.loader.load_country_files(countries_config)
                
            elif self.config.get('data_sources.spain_files'):
                # 西班牙多文件数据
                spain_config = self.config.get_spain_files()
                data_dict = self.loader.load_spain_files(spain_config)
                
            else:
                print("❌ 未找到有效的数据源配置")
                return None
            
            if not data_dict:
                print("❌ 没有成功加载任何数据文件")
                return None
            
            # 记录加载结果
            load_summary = {
                'files_loaded': len(data_dict),
                'total_rows': sum(len(df) for df in data_dict.values()),
                'data_sources': list(data_dict.keys())
            }
            
            self.results['processing_stages']['data_loading'] = load_summary
            print(f"✅ 数据加载完成: {load_summary['files_loaded']} 个文件，共 {load_summary['total_rows']:,} 行")
            
            return data_dict
            
        except Exception as e:
            print(f"❌ 数据加载失败: {str(e)}")
            return None
    
    def _clean_data(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        数据清洗步骤
        
        Args:
            raw_data: 原始数据字典
            
        Returns:
            清洗后的数据字典
        """
        print(f"\n🧹 第2步：数据清洗")
        
        cleaned_data = {}
        cleaning_summary = {
            'files_cleaned': 0,
            'total_rows_before': 0,
            'total_rows_after': 0,
            'rows_removed': 0
        }
        
        for name, df in raw_data.items():
            if df is None or df.empty:
                continue
            
            rows_before = len(df)
            cleaned_df = self.cleaner.clean_dataframe(df, f"{name}数据")
            rows_after = len(cleaned_df)
            
            if cleaned_df is not None and not cleaned_df.empty:
                cleaned_data[name] = cleaned_df
                cleaning_summary['files_cleaned'] += 1
                cleaning_summary['total_rows_before'] += rows_before
                cleaning_summary['total_rows_after'] += rows_after
                cleaning_summary['rows_removed'] += (rows_before - rows_after)
        
        self.results['processing_stages']['data_cleaning'] = cleaning_summary
        print(f"✅ 数据清洗完成: {cleaning_summary['total_rows_before']:,} → {cleaning_summary['total_rows_after']:,} 行")
        
        return cleaned_data
    
    def _transform_data(self, cleaned_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        数据转换步骤
        
        Args:
            cleaned_data: 清洗后的数据字典
            
        Returns:
            转换并合并后的DataFrame
        """
        print(f"\n🔄 第3步：数据转换")
        
        transformed_data_list = []
        transform_summary = {
            'files_transformed': 0,
            'total_rows_before_transform': 0,
            'total_rows_after_transform': 0
        }
        
        # 转换每个数据集
        for name, df in cleaned_data.items():
            if df is None or df.empty:
                continue
            
            rows_before = len(df)
            transformed_df = self.transformer.transform_dataframe(df, f"{name}数据")
            
            if transformed_df is not None and not transformed_df.empty:
                transformed_data_list.append(transformed_df)
                transform_summary['files_transformed'] += 1
                transform_summary['total_rows_before_transform'] += rows_before
                transform_summary['total_rows_after_transform'] += len(transformed_df)
        
        # 合并所有转换后的数据
        if transformed_data_list:
            print(f"\n🔗 合并 {len(transformed_data_list)} 个数据集")
            combined_df = pd.concat(transformed_data_list, ignore_index=True)
            print_dataframe_summary(combined_df, "合并后数据")
            
            # 执行透视操作
            pivot_config = self.config.get_pivot_config()
            if pivot_config:
                final_df = self.transformer.pivot_by_facts(combined_df, pivot_config)
            else:
                final_df = combined_df
            
            transform_summary['final_rows'] = len(final_df)
            transform_summary['final_columns'] = len(final_df.columns)
            
        else:
            print("❌ 没有可合并的转换数据")
            final_df = pd.DataFrame()
        
        self.results['processing_stages']['data_transformation'] = transform_summary
        print(f"✅ 数据转换完成: 最终 {len(final_df):,} 行 × {len(final_df.columns)} 列")
        
        return final_df
    
    def _validate_data(self, transformed_data: pd.DataFrame) -> Dict[str, Any]:
        """
        数据验证步骤
        
        Args:
            transformed_data: 转换后的数据
            
        Returns:
            验证结果字典
        """
        print(f"\n✅ 第4步：数据验证")
        
        if transformed_data is None or transformed_data.empty:
            print("⚠️  数据为空，跳过验证")
            return {'passed': False, 'reason': 'empty_data'}
        
        # 执行验证
        validation_results = self.validator.validate_dataframe(
            transformed_data, 
            f"{self.results['region']}最终数据"
        )
        
        self.results['processing_stages']['data_validation'] = {
            'validation_passed': validation_results.get('passed', False),
            'issues_found': len(validation_results.get('issues', [])),
            'consistency_rate': validation_results.get('consistency_check', {}).get('consistency_rate', 0)
        }
        
        return validation_results
    
    def _export_data(self, transformed_data: pd.DataFrame, 
                    validation_results: Dict[str, Any],
                    export_data: bool = True,
                    export_validation: bool = True) -> Dict[str, str]:
        """
        数据导出步骤
        
        Args:
            transformed_data: 要导出的数据
            validation_results: 验证结果
            export_data: 是否导出数据
            export_validation: 是否导出验证报告
            
        Returns:
            导出文件路径字典
        """
        print(f"\n💾 第5步：数据导出")
        
        export_results = {}
        
        if export_data and transformed_data is not None and not transformed_data.empty:
            # 导出数据
            if export_validation and self.config.get('output.save_validation_report', False):
                # 同时导出数据和验证报告
                export_paths = self.exporter.export_with_validation_report(
                    transformed_data, validation_results, self.results['region']
                )
                export_results.update(export_paths)
            else:
                # 只导出数据
                data_path = self.exporter.export_dataframe(
                    transformed_data, region=self.results['region']
                )
                if data_path:
                    export_results['data'] = data_path
        
        elif export_validation:
            # 只导出验证报告
            report_path = self.exporter._export_validation_report(
                validation_results, self.results['region']
            )
            if report_path:
                export_results['validation_report'] = report_path
        
        self.results['processing_stages']['data_export'] = {
            'files_exported': len(export_results),
            'export_paths': export_results
        }
        
        print(f"✅ 数据导出完成: {len(export_results)} 个文件")
        for file_type, path in export_results.items():
            print(f"   {file_type}: {path}")
        
        return export_results
    
    def _print_final_summary(self) -> None:
        """打印最终处理总结"""
        print(f"\n{'='*80}")
        print(f" 🎉 GFK数据处理管道执行完成")
        print(f"{'='*80}")
        
        duration = self.results.get('pipeline_duration', 0)
        print(f"⏱️  总耗时: {duration:.2f} 秒")
        print(f"📊 处理状态: {'✅ 成功' if self.results.get('success', False) else '❌ 失败'}")
        
        # 各阶段总结
        for stage, summary in self.results.get('processing_stages', {}).items():
            print(f"\n📋 {stage}:")
            for key, value in summary.items():
                if isinstance(value, (int, float)):
                    if 'rows' in key.lower():
                        print(f"   {key}: {value:,}")
                    else:
                        print(f"   {key}: {value}")
                else:
                    print(f"   {key}: {value}")
        
        # 最终数据统计
        final_data = self.results.get('final_data')
        if final_data is not None and not final_data.empty:
            print(f"\n📈 最终数据:")
            print(f"   行数: {len(final_data):,}")
            print(f"   列数: {len(final_data.columns)}")
            
            # 显示列信息
            if len(final_data.columns) <= 15:
                print(f"   列名: {', '.join(final_data.columns)}")
            else:
                print(f"   前5列: {', '.join(final_data.columns[:5])}...")
        
        print(f"\n{'='*80}")
    
    def get_summary_report(self) -> str:
        """
        获取处理总结报告
        
        Returns:
            格式化的总结报告字符串
        """
        lines = []
        lines.append("GFK数据处理管道执行报告")
        lines.append("="*50)
        lines.append(f"配置文件: {self.results.get('config_path', 'Unknown')}")
        lines.append(f"处理区域: {self.results.get('region', 'Unknown')}")
        lines.append(f"开始时间: {self.results.get('pipeline_start_time', 'Unknown')}")
        lines.append(f"结束时间: {self.results.get('pipeline_end_time', 'Unknown')}")
        lines.append(f"总耗时: {self.results.get('pipeline_duration', 0):.2f} 秒")
        lines.append(f"执行状态: {'成功' if self.results.get('success', False) else '失败'}")
        lines.append("")
        
        # 各阶段详情
        for stage, summary in self.results.get('processing_stages', {}).items():
            lines.append(f"【{stage}】")
            for key, value in summary.items():
                lines.append(f"  {key}: {value}")
            lines.append("")
        
        return "\n".join(lines)
    
    def export_summary_report(self, filename: Optional[str] = None) -> str:
        """
        导出处理总结报告
        
        Args:
            filename: 文件名（可选）
            
        Returns:
            导出文件路径
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"GFK_{self.results['region']}_PIPELINE_SUMMARY_{timestamp}.txt"
        
        summary_content = self.get_summary_report()
        return self.exporter.create_summary_export({'pipeline_summary': summary_content}, filename)
    
    def __str__(self) -> str:
        """返回管道的字符串表示"""
        return f"GFKDataPipeline(region='{self.results.get('region', 'Unknown')}')"
    
    def __repr__(self) -> str:
        """返回管道的详细表示"""
        return f"GFKDataPipeline(config='{self.results.get('config_path', 'Unknown')}', region='{self.results.get('region', 'Unknown')}')"