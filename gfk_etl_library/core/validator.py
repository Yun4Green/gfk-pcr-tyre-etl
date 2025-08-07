"""
数据验证器模块

负责数据质量验证，包括一致性检查、负值检测、数据完整性验证等。
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from ..utils import print_dataframe_summary


class DataValidator:
    """数据验证器类"""
    
    def __init__(self, validation_config: Dict[str, Any]):
        """
        初始化数据验证器
        
        Args:
            validation_config: 验证配置字典
        """
        self.config = validation_config
        self.consistency_config = validation_config.get('consistency_check', {})
        self.negative_config = validation_config.get('negative_values', {})
        
    def validate_dataframe(self, df: pd.DataFrame, data_name: str = "数据") -> Dict[str, Any]:
        """
        执行完整的数据验证流程
        
        Args:
            df: 要验证的DataFrame
            data_name: 数据名称（用于日志）
            
        Returns:
            验证结果字典
        """
        if df is None or df.empty:
            print(f"警告: {data_name} 为空，跳过验证")
            return {'passed': False, 'reason': 'empty_data'}
        
        print(f"\n=== 验证 {data_name} ===")
        
        validation_results = {
            'data_name': data_name,
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'passed': True,
            'issues': []
        }
        
        # 1. 数据完整性检查
        completeness_result = self.check_data_completeness(df)
        validation_results.update(completeness_result)
        
        # 2. 价格一致性检查
        if self.consistency_config.get('enabled', False):
            consistency_result = self.check_price_consistency(df)
            validation_results.update(consistency_result)
        
        # 3. 负值检查
        if self.negative_config.get('check_enabled', False):
            negative_result = self.check_negative_values(df)
            validation_results.update(negative_result)
        
        # 4. 数据类型检查
        dtype_result = self.check_data_types(df)
        validation_results.update(dtype_result)
        
        # 汇总验证结果
        overall_passed = len(validation_results['issues']) == 0
        validation_results['passed'] = overall_passed
        
        if overall_passed:
            print("✅ 所有验证检查通过")
        else:
            print(f"⚠️  发现 {len(validation_results['issues'])} 个数据质量问题")
            for issue in validation_results['issues']:
                print(f"   - {issue}")
        
        return validation_results
    
    def check_data_completeness(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        检查数据完整性
        
        Args:
            df: DataFrame
            
        Returns:
            完整性检查结果
        """
        print(f"\n--- 数据完整性检查 ---")
        
        result = {
            'missing_values': {},
            'empty_rows': 0,
            'duplicate_rows': 0
        }
        
        # 检查缺失值
        missing_counts = df.isnull().sum()
        total_missing = missing_counts.sum()
        
        if total_missing > 0:
            print(f"发现 {total_missing} 个缺失值:")
            for col, count in missing_counts[missing_counts > 0].items():
                percentage = (count / len(df)) * 100
                result['missing_values'][col] = {
                    'count': count,
                    'percentage': percentage
                }
                print(f"  {col}: {count} ({percentage:.1f}%)")
        else:
            print("✅ 无缺失值")
        
        # 检查空行
        empty_rows = df.isnull().all(axis=1).sum()
        result['empty_rows'] = empty_rows
        if empty_rows > 0:
            print(f"⚠️  发现 {empty_rows} 行完全空的数据")
            result['issues'].append(f"存在 {empty_rows} 行空数据")
        
        # 检查重复行
        duplicate_rows = df.duplicated().sum()
        result['duplicate_rows'] = duplicate_rows
        if duplicate_rows > 0:
            print(f"⚠️  发现 {duplicate_rows} 行重复数据")
            result['issues'].append(f"存在 {duplicate_rows} 行重复数据")
        else:
            print("✅ 无重复行")
        
        return result
    
    def check_price_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        检查价格计算一致性 (Price EUR × Units = Value EUR)
        
        Args:
            df: DataFrame
            
        Returns:
            一致性检查结果
        """
        print(f"\n--- 价格一致性检查 ---")
        
        price_col = self.consistency_config.get('price_column', 'Price EUR')
        units_col = self.consistency_config.get('units_column', 'Units')
        value_col = self.consistency_config.get('value_column', 'Value EUR')
        tolerance = self.consistency_config.get('tolerance', 0.01)
        
        result = {
            'consistency_check': {
                'enabled': True,
                'columns_available': False,
                'total_rows': 0,
                'consistent_rows': 0,
                'inconsistent_rows': 0,
                'consistency_rate': 0.0,
                'large_differences': 0
            }
        }
        
        # 检查必需列是否存在
        required_columns = [price_col, units_col, value_col]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"❌ 缺少一致性检查所需的列: {missing_columns}")
            result['issues'] = [f"缺少列: {missing_columns}"]
            return result
        
        # 移除有缺失值的行进行验证
        df_clean = df.dropna(subset=required_columns)
        
        if len(df_clean) == 0:
            print("❌ 所有行都有缺失值，无法进行一致性检查")
            result['issues'] = ["所有行都有缺失值"]
            return result
        
        result['consistency_check']['columns_available'] = True
        result['consistency_check']['total_rows'] = len(df_clean)
        
        # 计算期望值
        df_clean = df_clean.copy()
        df_clean['Calculated_Value'] = df_clean[price_col] * df_clean[units_col]
        df_clean['Difference'] = abs(df_clean['Calculated_Value'] - df_clean[value_col])
        df_clean['Difference_Percent'] = (df_clean['Difference'] / df_clean[value_col] * 100).fillna(0)
        
        # 检查一致性
        consistent_mask = df_clean['Difference'] <= tolerance
        inconsistent_mask = df_clean['Difference'] > tolerance
        
        consistent_count = consistent_mask.sum()
        inconsistent_count = inconsistent_mask.sum()
        consistency_rate = (consistent_count / len(df_clean)) * 100
        
        result['consistency_check']['consistent_rows'] = consistent_count
        result['consistency_check']['inconsistent_rows'] = inconsistent_count
        result['consistency_check']['consistency_rate'] = consistency_rate
        
        print(f"总检查行数: {len(df_clean)}")
        print(f"一致的行数: {consistent_count}")
        print(f"不一致的行数: {inconsistent_count}")
        print(f"一致性比例: {consistency_rate:.2f}%")
        
        # 检查大差异
        large_diff_threshold = 1000
        large_differences = (df_clean['Difference'] > large_diff_threshold).sum()
        result['consistency_check']['large_differences'] = large_differences
        
        if large_differences > 0:
            print(f"⚠️  发现 {large_differences} 行有大差异 (> {large_diff_threshold})")
            result.setdefault('issues', []).append(f"存在 {large_differences} 行大差异数据")
        
        # 如果一致性低于阈值，标记为问题
        if consistency_rate < 80:
            print(f"⚠️  一致性比例过低: {consistency_rate:.2f}%")
            result.setdefault('issues', []).append(f"价格一致性过低: {consistency_rate:.2f}%")
        
        return result
    
    def check_negative_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        检查负值数据
        
        Args:
            df: DataFrame
            
        Returns:
            负值检查结果
        """
        print(f"\n--- 负值检查 ---")
        
        result = {
            'negative_values': {
                'enabled': True,
                'columns_with_negatives': {},
                'total_negative_rows': 0
            }
        }
        
        # 检查数值列的负值
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        negative_found = False
        negative_rows_set = set()
        
        for col in numeric_columns:
            negative_mask = df[col] < 0
            negative_count = negative_mask.sum()
            
            if negative_count > 0:
                negative_found = True
                negative_rows_set.update(df[negative_mask].index)
                
                percentage = (negative_count / len(df)) * 100
                result['negative_values']['columns_with_negatives'][col] = {
                    'count': negative_count,
                    'percentage': percentage,
                    'min_value': df[col].min()
                }
                
                print(f"{col}: {negative_count} 个负值 ({percentage:.1f}%) [最小值: {df[col].min()}]")
        
        result['negative_values']['total_negative_rows'] = len(negative_rows_set)
        
        if not negative_found:
            print("✅ 无负值数据")
        else:
            threshold = self.negative_config.get('report_threshold', 10)
            if len(negative_rows_set) > threshold:
                result.setdefault('issues', []).append(f"存在 {len(negative_rows_set)} 行负值数据")
        
        return result
    
    def check_data_types(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        检查数据类型
        
        Args:
            df: DataFrame
            
        Returns:
            数据类型检查结果
        """
        print(f"\n--- 数据类型检查 ---")
        
        result = {
            'data_types': {},
            'type_issues': []
        }
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            null_count = df[col].isnull().sum()
            unique_count = df[col].nunique()
            
            result['data_types'][col] = {
                'dtype': dtype,
                'null_count': null_count,
                'unique_count': unique_count
            }
            
            # 检查可能的类型问题
            if dtype == 'object':
                # 检查是否应该是数值类型
                non_null_series = df[col].dropna()
                if len(non_null_series) > 0:
                    try:
                        pd.to_numeric(non_null_series)
                        result['type_issues'].append(f"列 '{col}' 可能应该是数值类型")
                    except ValueError:
                        pass
        
        if result['type_issues']:
            print("⚠️  数据类型问题:")
            for issue in result['type_issues']:
                print(f"   - {issue}")
            result.setdefault('issues', []).extend(result['type_issues'])
        else:
            print("✅ 数据类型检查通过")
        
        return result
    
    def generate_validation_report(self, validation_results: Dict[str, Any]) -> str:
        """
        生成验证报告
        
        Args:
            validation_results: 验证结果字典
            
        Returns:
            格式化的验证报告字符串
        """
        report_lines = []
        report_lines.append("="*60)
        report_lines.append(f" 数据验证报告 - {validation_results.get('data_name', '未知数据')}")
        report_lines.append("="*60)
        
        # 基本信息
        report_lines.append(f"数据行数: {validation_results.get('total_rows', 0):,}")
        report_lines.append(f"数据列数: {validation_results.get('total_columns', 0)}")
        report_lines.append(f"整体状态: {'✅ 通过' if validation_results.get('passed', False) else '⚠️  存在问题'}")
        
        # 一致性检查
        if 'consistency_check' in validation_results:
            cc = validation_results['consistency_check']
            if cc.get('enabled', False) and cc.get('columns_available', False):
                report_lines.append(f"\n价格一致性:")
                report_lines.append(f"  检查行数: {cc.get('total_rows', 0):,}")
                report_lines.append(f"  一致行数: {cc.get('consistent_rows', 0):,}")
                report_lines.append(f"  一致性比例: {cc.get('consistency_rate', 0):.2f}%")
        
        # 负值检查
        if 'negative_values' in validation_results:
            nv = validation_results['negative_values']
            if nv.get('enabled', False):
                report_lines.append(f"\n负值检查:")
                report_lines.append(f"  负值行数: {nv.get('total_negative_rows', 0)}")
                if nv.get('columns_with_negatives'):
                    for col, info in nv['columns_with_negatives'].items():
                        report_lines.append(f"  {col}: {info['count']} 个负值")
        
        # 问题总结
        issues = validation_results.get('issues', [])
        if issues:
            report_lines.append(f"\n发现的问题 ({len(issues)}):")
            for i, issue in enumerate(issues, 1):
                report_lines.append(f"  {i}. {issue}")
        
        report_lines.append("="*60)
        
        return "\n".join(report_lines)
    
    def save_validation_report(self, validation_results: Dict[str, Any], 
                             output_path: str) -> None:
        """
        保存验证报告到文件
        
        Args:
            validation_results: 验证结果字典
            output_path: 输出文件路径
        """
        report = self.generate_validation_report(validation_results)
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"验证报告已保存到: {output_path}")
        except Exception as e:
            print(f"保存验证报告失败: {str(e)}")
    
    def __str__(self) -> str:
        """返回验证器的字符串表示"""
        return f"DataValidator(consistency_check={self.consistency_config.get('enabled', False)})"