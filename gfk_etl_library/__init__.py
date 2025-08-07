"""
GFK PCR Tyre ETL Library - GFK PCR轮胎市场数据处理库

这是一个专业的数据预处理库，专门用于处理GFK 2025年欧洲PCR轮胎市场CSV数据。

主要功能:
- PCR轮胎数据加载和清洗
- 格式转换（宽转长）
- 数据验证和质量检查
- 灵活的配置管理
- 标准化的数据导出

使用示例:
    from gfk_etl_library import GFKDataPipeline
    
    pipeline = GFKDataPipeline('config/europe_config.yml')
    result = pipeline.run()
"""

__version__ = "2.0.0"
__author__ = "Julian Luan"

from .pipeline import GFKDataPipeline
from .config import ConfigManager

__all__ = ['GFKDataPipeline', 'ConfigManager']