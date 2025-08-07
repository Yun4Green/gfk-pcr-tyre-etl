"""
数据导出器模块

负责数据导出操作，支持多种格式和输出配置。
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from ..utils import ensure_directory_exists, safe_create_filename, get_file_size_mb


class DataExporter:
    """数据导出器类"""
    
    def __init__(self, output_config: Dict[str, Any]):
        """
        初始化数据导出器
        
        Args:
            output_config: 输出配置字典
        """
        self.config = output_config
        self.output_directory = output_config.get('output_directory', './data/processed')
        self.filename_pattern = output_config.get('filename_pattern', 'GFK_PROCESSED_{timestamp}.csv')
        self.include_timestamp = output_config.get('include_timestamp', True)
        
        # 确保输出目录存在
        ensure_directory_exists(self.output_directory)
    
    def export_dataframe(self, df: pd.DataFrame, 
                        filename: Optional[str] = None,
                        region: str = "DATA",
                        **kwargs) -> str:
        """
        导出DataFrame到CSV文件
        
        Args:
            df: 要导出的DataFrame
            filename: 自定义文件名（可选）
            region: 区域名称（用于文件名）
            **kwargs: 传递给DataFrame.to_csv的额外参数
            
        Returns:
            导出文件的完整路径
        """
        if df is None or df.empty:
            print("警告: 数据为空，跳过导出")
            return ""
        
        # 生成文件名
        if filename is None:
            filename = safe_create_filename(
                self.filename_pattern,
                region=region.upper(),
                timestamp=datetime.now().strftime("%Y%m%d_%H%M%S") if self.include_timestamp else ""
            )
        
        # 构建完整路径
        output_path = os.path.join(self.output_directory, filename)
        
        try:
            # 默认导出参数
            default_kwargs = {
                'index': False,
                'encoding': 'utf-8'
            }
            default_kwargs.update(kwargs)
            
            print(f"\n=== 导出数据 ===")
            print(f"文件路径: {output_path}")
            print(f"数据维度: {len(df)} 行 × {len(df.columns)} 列")
            
            # 导出数据
            df.to_csv(output_path, **default_kwargs)
            
            # 验证导出
            file_size = get_file_size_mb(output_path)
            print(f"✅ 导出成功，文件大小: {file_size:.1f} MB")
            
            return output_path
            
        except Exception as e:
            print(f"❌ 导出失败: {str(e)}")
            return ""
    
    def export_multiple_dataframes(self, data_dict: Dict[str, pd.DataFrame],
                                 prefix: str = "GFK") -> Dict[str, str]:
        """
        导出多个DataFrame
        
        Args:
            data_dict: 数据字典，格式: {'名称': DataFrame}
            prefix: 文件名前缀
            
        Returns:
            导出文件路径字典，格式: {'名称': '文件路径'}
        """
        export_results = {}
        
        print(f"\n=== 批量导出 {len(data_dict)} 个数据集 ===")
        
        for name, df in data_dict.items():
            if df is None or df.empty:
                print(f"跳过 {name}: 数据为空")
                continue
            
            # 为每个数据集生成特定的文件名
            filename = safe_create_filename(
                f"{prefix}_{name}_{{timestamp}}.csv",
                timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")
            )
            
            output_path = self.export_dataframe(df, filename=filename)
            if output_path:
                export_results[name] = output_path
        
        print(f"批量导出完成: {len(export_results)}/{len(data_dict)} 个文件")
        return export_results
    
    def export_with_validation_report(self, df: pd.DataFrame,
                                    validation_results: Dict[str, Any],
                                    region: str = "DATA") -> Dict[str, str]:
        """
        导出数据和验证报告
        
        Args:
            df: 要导出的DataFrame
            validation_results: 验证结果字典
            region: 区域名称
            
        Returns:
            导出文件路径字典
        """
        export_paths = {}
        
        # 导出数据
        data_path = self.export_dataframe(df, region=region)
        if data_path:
            export_paths['data'] = data_path
        
        # 生成并导出验证报告
        if self.config.get('save_validation_report', False):
            report_path = self._export_validation_report(validation_results, region)
            if report_path:
                export_paths['validation_report'] = report_path
        
        return export_paths
    
    def _export_validation_report(self, validation_results: Dict[str, Any],
                                region: str = "DATA") -> str:
        """
        导出验证报告
        
        Args:
            validation_results: 验证结果字典
            region: 区域名称
            
        Returns:
            报告文件路径
        """
        # 生成报告文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"GFK_{region}_VALIDATION_REPORT_{timestamp}.txt"
        report_path = os.path.join(self.output_directory, "reports", report_filename)
        
        # 确保报告目录存在
        ensure_directory_exists(os.path.dirname(report_path))
        
        try:
            # 生成报告内容
            report_content = self._generate_detailed_report(validation_results)
            
            # 写入文件
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            print(f"✅ 验证报告已导出: {report_path}")
            return report_path
            
        except Exception as e:
            print(f"❌ 验证报告导出失败: {str(e)}")
            return ""
    
    def _generate_detailed_report(self, validation_results: Dict[str, Any]) -> str:
        """
        生成详细的验证报告
        
        Args:
            validation_results: 验证结果字典
            
        Returns:
            格式化的报告内容
        """
        lines = []
        lines.append("="*80)
        lines.append(f" GFK 数据验证详细报告")
        lines.append("="*80)
        lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"数据集名称: {validation_results.get('data_name', '未知')}")
        lines.append("")
        
        # 基本统计
        lines.append("【基本统计】")
        lines.append(f"总行数: {validation_results.get('total_rows', 0):,}")
        lines.append(f"总列数: {validation_results.get('total_columns', 0)}")
        lines.append(f"整体状态: {'✅ 通过' if validation_results.get('passed', False) else '⚠️  存在问题'}")
        lines.append("")
        
        # 缺失值统计
        if 'missing_values' in validation_results:
            missing = validation_results['missing_values']
            if missing:
                lines.append("【缺失值统计】")
                for col, info in missing.items():
                    lines.append(f"{col}: {info['count']} ({info['percentage']:.1f}%)")
                lines.append("")
        
        # 一致性检查详情
        if 'consistency_check' in validation_results:
            cc = validation_results['consistency_check']
            if cc.get('enabled', False) and cc.get('columns_available', False):
                lines.append("【价格一致性检查】")
                lines.append(f"检查行数: {cc.get('total_rows', 0):,}")
                lines.append(f"一致行数: {cc.get('consistent_rows', 0):,}")
                lines.append(f"不一致行数: {cc.get('inconsistent_rows', 0):,}")
                lines.append(f"一致性比例: {cc.get('consistency_rate', 0):.2f}%")
                lines.append(f"大差异行数: {cc.get('large_differences', 0)}")
                lines.append("")
        
        # 负值检查详情
        if 'negative_values' in validation_results:
            nv = validation_results['negative_values']
            if nv.get('enabled', False):
                lines.append("【负值检查】")
                lines.append(f"总负值行数: {nv.get('total_negative_rows', 0)}")
                
                if nv.get('columns_with_negatives'):
                    lines.append("负值列详情:")
                    for col, info in nv['columns_with_negatives'].items():
                        lines.append(f"  {col}: {info['count']} 个负值 ({info['percentage']:.1f}%)")
                        lines.append(f"    最小值: {info['min_value']}")
                lines.append("")
        
        # 数据类型信息
        if 'data_types' in validation_results:
            lines.append("【数据类型信息】")
            for col, info in validation_results['data_types'].items():
                lines.append(f"{col}: {info['dtype']} (唯一值: {info['unique_count']}, 缺失: {info['null_count']})")
            lines.append("")
        
        # 问题汇总
        issues = validation_results.get('issues', [])
        if issues:
            lines.append("【发现的问题】")
            for i, issue in enumerate(issues, 1):
                lines.append(f"{i}. {issue}")
            lines.append("")
        
        # 建议
        lines.append("【数据质量建议】")
        if validation_results.get('passed', False):
            lines.append("✅ 数据质量良好，可以直接用于分析")
        else:
            lines.append("⚠️  建议在分析前处理发现的数据质量问题")
            
            if 'consistency_check' in validation_results:
                cc = validation_results['consistency_check']
                if cc.get('consistency_rate', 100) < 80:
                    lines.append("- 价格一致性较低，建议检查计算逻辑或数据来源")
            
            if validation_results.get('missing_values'):
                lines.append("- 存在缺失值，建议根据业务需求进行填充或删除")
            
            if validation_results.get('negative_values', {}).get('total_negative_rows', 0) > 0:
                lines.append("- 存在负值，建议确认是否为正常业务场景（如退货调整）")
        
        lines.append("")
        lines.append("="*80)
        
        return "\n".join(lines)
    
    def create_summary_export(self, all_results: Dict[str, Any],
                            summary_filename: str = "processing_summary.txt") -> str:
        """
        创建处理过程总结文件
        
        Args:
            all_results: 所有处理结果字典
            summary_filename: 总结文件名
            
        Returns:
            总结文件路径
        """
        summary_path = os.path.join(self.output_directory, summary_filename)
        
        try:
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write("GFK ETL 处理总结\n")
                f.write("="*50 + "\n")
                f.write(f"处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                for stage, result in all_results.items():
                    f.write(f"【{stage}】\n")
                    if isinstance(result, dict):
                        for key, value in result.items():
                            f.write(f"  {key}: {value}\n")
                    else:
                        f.write(f"  {result}\n")
                    f.write("\n")
            
            print(f"✅ 处理总结已保存: {summary_path}")
            return summary_path
            
        except Exception as e:
            print(f"❌ 总结文件保存失败: {str(e)}")
            return ""
    
    def __str__(self) -> str:
        """返回导出器的字符串表示"""
        return f"DataExporter(output_directory='{self.output_directory}')"