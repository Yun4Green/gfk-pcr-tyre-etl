"""
配置管理器

负责加载和管理YAML配置文件，支持配置继承和合并。
"""

import os
import yaml
from typing import Dict, Any, Optional


class ConfigManager:
    """配置管理器类"""
    
    def __init__(self, config_path: str):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径
            
        Raises:
            FileNotFoundError: 当配置文件不存在时
            yaml.YAMLError: 当YAML文件格式错误时
        """
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        加载配置文件，支持include机制
        
        Returns:
            配置字典
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 处理include机制
        if 'include' in config:
            base_config_path = self._resolve_include_path(config['include'])
            base_config = self._load_base_config(base_config_path)
            config = self._merge_configs(base_config, config)
            del config['include']  # 移除include键
        
        return config
    
    def _resolve_include_path(self, include_path: str) -> str:
        """
        解析include路径
        
        Args:
            include_path: 相对或绝对路径
            
        Returns:
            绝对路径
        """
        if os.path.isabs(include_path):
            return include_path
        
        # 相对于当前配置文件的目录
        config_dir = os.path.dirname(self.config_path)
        return os.path.join(config_dir, include_path)
    
    def _load_base_config(self, base_path: str) -> Dict[str, Any]:
        """
        加载基础配置文件
        
        Args:
            base_path: 基础配置文件路径
            
        Returns:
            基础配置字典
        """
        with open(base_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        深度合并两个配置字典
        
        Args:
            base: 基础配置
            override: 覆盖配置
            
        Returns:
            合并后的配置
        """
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        使用点分隔符获取嵌套配置值
        
        Args:
            key_path: 配置键路径，如 'processing.cleaning.remove_total_rows'
            default: 默认值
            
        Returns:
            配置值
            
        Examples:
            >>> config.get('data_sources.countries.Germany.code')
            'DE'
        """
        keys = key_path.split('.')
        value = self.config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_countries(self) -> Dict[str, Dict[str, str]]:
        """
        获取国家配置
        
        Returns:
            国家配置字典
        """
        return self.get('data_sources.countries', {})
    
    def get_spain_files(self) -> Dict[str, Dict[str, str]]:
        """
        获取西班牙文件配置
        
        Returns:
            西班牙文件配置字典
        """
        return self.get('data_sources.spain_files', {})
    
    def get_column_mapping(self) -> Dict[str, str]:
        """
        获取列映射配置
        
        Returns:
            列映射字典
        """
        return self.get('processing.column_mapping', {})
    
    def get_date_mapping(self) -> Dict[str, str]:
        """
        获取日期映射配置
        
        Returns:
            日期映射字典
        """
        return self.get('processing.date_mapping', {})
    
    def get_pivot_config(self) -> Dict[str, Any]:
        """
        获取透视配置
        
        Returns:
            透视配置字典
        """
        return self.get('processing.pivot', {})
    
    def is_validation_enabled(self, validation_type: str) -> bool:
        """
        检查特定验证是否启用
        
        Args:
            validation_type: 验证类型，如 'consistency_check', 'negative_values'
            
        Returns:
            是否启用
        """
        return self.get(f'validation.{validation_type}.enabled', False)
    
    def get_output_pattern(self) -> str:
        """
        获取输出文件名模式
        
        Returns:
            文件名模式字符串
        """
        return self.get('output.filename_pattern', 'GFK_PROCESSED_{timestamp}.csv')
    
    def __str__(self) -> str:
        """返回配置的字符串表示"""
        return f"ConfigManager(config_path='{self.config_path}')"
    
    def __repr__(self) -> str:
        """返回配置的详细表示"""
        return f"ConfigManager(config_path='{self.config_path}', keys={list(self.config.keys())})"