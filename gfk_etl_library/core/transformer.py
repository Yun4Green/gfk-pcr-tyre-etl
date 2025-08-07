"""
数据转换器模块

负责数据格式转换，包括列重命名、宽转长格式、日期映射等。
"""

import pandas as pd
from typing import Dict, List, Any, Optional
from ..utils import print_dataframe_summary, validate_required_columns


class DataTransformer:
    """数据转换器类"""
    
    def __init__(self, transform_config: Dict[str, Any]):
        """
        初始化数据转换器
        
        Args:
            transform_config: 转换配置字典
        """
        self.config = transform_config
        self.column_mapping = transform_config.get('column_mapping', {})
        self.date_mapping = transform_config.get('date_mapping', {})
    
    def transform_dataframe(self, df: pd.DataFrame, data_name: str = "数据") -> pd.DataFrame:
        """
        执行完整的数据转换流程
        
        Args:
            df: 要转换的DataFrame
            data_name: 数据名称（用于日志）
            
        Returns:
            转换后的DataFrame
        """
        if df is None or df.empty:
            print(f"警告: {data_name} 为空，跳过转换")
            return df
        
        print(f"\n=== 转换 {data_name} ===")
        
        # 1. 重命名列
        df_transformed = self.rename_columns(df.copy())
        
        # 2. 转换为长格式
        df_transformed = self.wide_to_long(df_transformed)
        
        print_dataframe_summary(df_transformed, f"转换后: {data_name}")
        
        return df_transformed
    
    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        根据配置重命名列
        
        Args:
            df: DataFrame
            
        Returns:
            重命名后的DataFrame
        """
        if not self.column_mapping:
            print("无列重命名配置")
            return df
        
        # 只重命名存在的列
        existing_mapping = {old: new for old, new in self.column_mapping.items() 
                          if old in df.columns}
        
        if existing_mapping:
            print(f"重命名列: {existing_mapping}")
            df = df.rename(columns=existing_mapping)
        else:
            print("无需重命名列（指定的列不存在）")
        
        return df
    
    def wide_to_long(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        将宽格式转换为长格式
        
        Args:
            df: 宽格式DataFrame
            
        Returns:
            长格式DataFrame
        """
        # 获取月份列（时间序列列）
        month_columns = [col for col in df.columns if col in self.date_mapping.keys()]
        
        if not month_columns:
            print("警告: 未找到时间序列列，跳过宽转长")
            return df
        
        print(f"找到 {len(month_columns)} 个时间序列列: {month_columns}")
        
        # 获取标识列（非时间序列列）
        id_columns = [col for col in df.columns if col not in month_columns]
        
        print(f"标识列 ({len(id_columns)}): {id_columns}")
        
        # 转换为长格式
        long_data = []
        
        for _, row in df.iterrows():
            for month_col in month_columns:
                value = row[month_col]
                
                # 只保留非空非零值
                if pd.notna(value) and value != 0:
                    new_row = {}
                    
                    # 复制所有标识列
                    for id_col in id_columns:
                        new_row[id_col] = row[id_col]
                    
                    # 添加日期和值
                    new_row['Date'] = self.date_mapping.get(month_col, month_col)
                    new_row['Value'] = value
                    
                    long_data.append(new_row)
        
        result_df = pd.DataFrame(long_data)
        
        print(f"宽转长完成: {len(df)} 行 → {len(result_df)} 行")
        
        return result_df
    
    def pivot_by_facts(self, df: pd.DataFrame, pivot_config: Dict[str, Any]) -> pd.DataFrame:
        """
        根据Facts列进行透视
        
        Args:
            df: 长格式DataFrame
            pivot_config: 透视配置
            
        Returns:
            透视后的DataFrame
        """
        print(f"\n=== Facts透视操作 ===")
        print(f"透视前数据行数: {len(df)}")
        
        index_columns = pivot_config.get('index_columns', [])
        pivot_column = pivot_config.get('pivot_column', 'Facts')
        value_column = pivot_config.get('value_column', 'Value')
        
        # 验证必需列是否存在
        required_columns = index_columns + [pivot_column, value_column]
        existing_columns = [col for col in required_columns if col in df.columns]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"警告: 缺少透视所需的列: {missing_columns}")
            # 调整index_columns，只使用存在的列
            index_columns = [col for col in index_columns if col in df.columns]
            print(f"使用现有的标识列: {index_columns}")
        
        if pivot_column not in df.columns:
            print(f"错误: 透视列 '{pivot_column}' 不存在")
            return df
        
        # 显示Facts列的唯一值
        unique_facts = df[pivot_column].unique()
        print(f"Facts列的唯一值 ({len(unique_facts)}): {list(unique_facts)}")
        
        try:
            # 创建透视表
            pivot_df = df.pivot_table(
                index=index_columns,
                columns=pivot_column,
                values=value_column,
                aggfunc='sum'
            ).reset_index()
            
            # 清除列名
            pivot_df.columns.name = None
            
            print(f"透视后数据行数: {len(pivot_df)}")
            print(f"透视后列数: {len(pivot_df.columns)}")
            
            # 显示新列
            fact_columns = [col for col in pivot_df.columns if col not in index_columns]
            print(f"Facts转换后的列: {fact_columns}")
            
            return pivot_df
            
        except Exception as e:
            print(f"错误: 透视操作失败 - {str(e)}")
            return df
    
    def add_calculated_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        添加计算列
        
        Args:
            df: DataFrame
            
        Returns:
            添加计算列后的DataFrame
        """
        # 这里可以添加各种计算列的逻辑
        # 例如：价格一致性检查的计算列
        
        if all(col in df.columns for col in ['Price EUR', 'Units']):
            df['Calculated_Value'] = df['Price EUR'] * df['Units']
            print("添加计算列: Calculated_Value = Price EUR × Units")
        
        return df
    
    def standardize_date_format(self, df: pd.DataFrame, 
                              date_column: str = 'Date') -> pd.DataFrame:
        """
        标准化日期格式
        
        Args:
            df: DataFrame
            date_column: 日期列名
            
        Returns:
            日期格式标准化后的DataFrame
        """
        if date_column not in df.columns:
            print(f"警告: 日期列 '{date_column}' 不存在")
            return df
        
        try:
            df[date_column] = pd.to_datetime(df[date_column])
            print(f"✅ 日期列 '{date_column}' 已转换为datetime格式")
        except Exception as e:
            print(f"警告: 无法转换日期列格式 - {str(e)}")
        
        return df
    
    def filter_data(self, df: pd.DataFrame, 
                   filters: Dict[str, Any]) -> pd.DataFrame:
        """
        根据条件过滤数据
        
        Args:
            df: DataFrame
            filters: 过滤条件字典
            
        Returns:
            过滤后的DataFrame
        """
        filtered_df = df.copy()
        original_rows = len(filtered_df)
        
        for column, condition in filters.items():
            if column not in filtered_df.columns:
                print(f"警告: 过滤列 '{column}' 不存在")
                continue
            
            if isinstance(condition, dict):
                # 复杂条件
                if 'min' in condition:
                    filtered_df = filtered_df[filtered_df[column] >= condition['min']]
                if 'max' in condition:
                    filtered_df = filtered_df[filtered_df[column] <= condition['max']]
                if 'values' in condition:
                    filtered_df = filtered_df[filtered_df[column].isin(condition['values'])]
            else:
                # 简单条件
                filtered_df = filtered_df[filtered_df[column] == condition]
        
        filtered_rows = len(filtered_df)
        removed_rows = original_rows - filtered_rows
        
        if removed_rows > 0:
            print(f"过滤完成: {original_rows} → {filtered_rows} 行 (移除 {removed_rows} 行)")
        
        return filtered_df
    
    def validate_transformation(self, original_df: pd.DataFrame, 
                              transformed_df: pd.DataFrame) -> bool:
        """
        验证转换结果
        
        Args:
            original_df: 原始DataFrame
            transformed_df: 转换后DataFrame
            
        Returns:
            是否通过验证
        """
        print(f"\n=== 转换验证 ===")
        
        # 基本检查
        if transformed_df is None or transformed_df.empty:
            print("❌ 转换后数据为空")
            return False
        
        # 数据量检查（长格式通常会增加行数）
        print(f"原始数据: {len(original_df)} 行 × {len(original_df.columns)} 列")
        print(f"转换后: {len(transformed_df)} 行 × {len(transformed_df.columns)} 列")
        
        # 检查关键列是否存在
        required_columns = ['Date', 'Value']
        missing_columns = [col for col in required_columns if col not in transformed_df.columns]
        
        if missing_columns:
            print(f"❌ 缺少关键列: {missing_columns}")
            return False
        
        print("✅ 转换验证通过")
        return True
    
    def __str__(self) -> str:
        """返回转换器的字符串表示"""
        return f"DataTransformer(columns_to_rename={len(self.column_mapping)})"