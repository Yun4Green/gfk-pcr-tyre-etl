"""
数据加载器模块

负责从各种数据源加载数据，支持单文件和多文件加载。
"""

import pandas as pd
import os
from typing import Dict, List, Optional, Union
from ..utils import validate_file_exists, print_dataframe_summary, get_file_size_mb


class DataLoader:
    """数据加载器类"""
    
    def __init__(self, input_directory: str = "."):
        """
        初始化数据加载器
        
        Args:
            input_directory: 输入文件目录
        """
        self.input_directory = input_directory
    
    def load_single_file(self, file_path: str, **kwargs) -> Optional[pd.DataFrame]:
        """
        加载单个CSV文件
        
        Args:
            file_path: 文件路径
            **kwargs: 传递给pd.read_csv的额外参数
            
        Returns:
            DataFrame或None（如果加载失败）
        """
        full_path = os.path.join(self.input_directory, file_path)
        
        if not validate_file_exists(full_path, "数据文件"):
            return None
        
        try:
            print(f"正在加载文件: {file_path} ({get_file_size_mb(full_path):.1f} MB)")
            
            # 默认参数
            default_kwargs = {
                'encoding': 'utf-8',
                'low_memory': False
            }
            default_kwargs.update(kwargs)
            
            df = pd.read_csv(full_path, **default_kwargs)
            print_dataframe_summary(df, f"已加载: {os.path.basename(file_path)}")
            
            return df
            
        except Exception as e:
            print(f"错误: 无法加载文件 {file_path} - {str(e)}")
            return None
    
    def load_country_files(self, countries_config: Dict[str, Dict[str, str]]) -> Dict[str, pd.DataFrame]:
        """
        加载多个国家的数据文件
        
        Args:
            countries_config: 国家配置字典，格式: {'国家名': {'file': '文件路径'}}
            
        Returns:
            国家数据字典，格式: {'国家名': DataFrame}
        """
        country_data = {}
        
        print(f"\n=== 加载国家数据文件 ===")
        print(f"计划加载 {len(countries_config)} 个国家的数据")
        
        for country_name, config in countries_config.items():
            file_path = config.get('file')
            if not file_path:
                print(f"警告: {country_name} 缺少文件路径配置")
                continue
            
            df = self.load_single_file(file_path)
            if df is not None:
                # 添加国家列
                df['country'] = country_name
                country_data[country_name] = df
                print(f"✅ {country_name}: {len(df)} 行数据")
            else:
                print(f"❌ {country_name}: 加载失败")
        
        print(f"\n成功加载 {len(country_data)}/{len(countries_config)} 个国家的数据")
        return country_data
    
    def load_spain_files(self, spain_config: Dict[str, Dict[str, str]]) -> Dict[str, pd.DataFrame]:
        """
        加载西班牙的多个车型数据文件
        
        Args:
            spain_config: 西班牙配置字典，格式: {'车型': {'file': '文件路径'}}
            
        Returns:
            车型数据字典，格式: {'车型': DataFrame}
        """
        spain_data = {}
        
        print(f"\n=== 加载西班牙数据文件 ===")
        print(f"计划加载 {len(spain_config)} 个车型的数据")
        
        for vehicle_type, config in spain_config.items():
            file_path = config.get('file')
            if not file_path:
                print(f"警告: {vehicle_type} 缺少文件路径配置")
                continue
            
            df = self.load_single_file(file_path)
            if df is not None:
                # 添加国家和车型信息
                df['country'] = 'Spain'
                # 注意：西班牙数据可能已经有Type of Vehicle列，这里不覆盖
                spain_data[vehicle_type] = df
                print(f"✅ {vehicle_type}: {len(df)} 行数据")
            else:
                print(f"❌ {vehicle_type}: 加载失败")
        
        print(f"\n成功加载 {len(spain_data)}/{len(spain_config)} 个车型的数据")
        return spain_data
    
    def load_multiple_files(self, file_paths: List[str], 
                           add_source_column: bool = True) -> Dict[str, pd.DataFrame]:
        """
        加载多个文件
        
        Args:
            file_paths: 文件路径列表
            add_source_column: 是否添加源文件名列
            
        Returns:
            文件数据字典，格式: {'文件名': DataFrame}
        """
        file_data = {}
        
        print(f"\n=== 加载多个文件 ===")
        print(f"计划加载 {len(file_paths)} 个文件")
        
        for file_path in file_paths:
            df = self.load_single_file(file_path)
            if df is not None:
                if add_source_column:
                    df['_source_file'] = os.path.basename(file_path)
                
                file_name = os.path.splitext(os.path.basename(file_path))[0]
                file_data[file_name] = df
                print(f"✅ {file_name}: {len(df)} 行数据")
            else:
                print(f"❌ {os.path.basename(file_path)}: 加载失败")
        
        print(f"\n成功加载 {len(file_data)}/{len(file_paths)} 个文件")
        return file_data
    
    def validate_data_structure(self, df: pd.DataFrame, 
                              expected_columns: Optional[List[str]] = None) -> bool:
        """
        验证数据结构
        
        Args:
            df: 要验证的DataFrame
            expected_columns: 期望的列名列表
            
        Returns:
            是否通过验证
        """
        if df is None or df.empty:
            print("错误: 数据为空")
            return False
        
        if expected_columns:
            missing_columns = [col for col in expected_columns if col not in df.columns]
            if missing_columns:
                print(f"错误: 缺少必需的列: {missing_columns}")
                print(f"当前可用列: {list(df.columns)}")
                return False
        
        print("✅ 数据结构验证通过")
        return True
    
    def get_common_columns(self, data_dict: Dict[str, pd.DataFrame]) -> List[str]:
        """
        获取所有DataFrame的公共列
        
        Args:
            data_dict: 数据字典
            
        Returns:
            公共列名列表
        """
        if not data_dict:
            return []
        
        common_columns = set(next(iter(data_dict.values())).columns)
        
        for name, df in data_dict.items():
            common_columns = common_columns.intersection(set(df.columns))
            print(f"{name}: {len(df.columns)} 列")
        
        common_list = list(common_columns)
        print(f"\n公共列 ({len(common_list)}): {common_list}")
        
        return common_list
    
    def __str__(self) -> str:
        """返回加载器的字符串表示"""
        return f"DataLoader(input_directory='{self.input_directory}')"