# GFK PCR Tyre ETL API 参考文档

> **作者**: Julian Luan (julian.luan@sailun-tyres.eu)  
> **更新时间**: 2025年1月  
> **适用于**: GFK 2025年PCR轮胎市场数据

本文档详细说明了GFK PCR Tyre ETL库的所有类和方法。

## 目录

1. [主要接口类](#主要接口类)
2. [核心处理模块](#核心处理模块)
3. [工具模块](#工具模块)
4. [配置管理](#配置管理)
5. [异常处理](#异常处理)

## 主要接口类

### GFKDataPipeline

主要的数据处理管道类，协调所有ETL步骤。

#### 类定义

```python
class GFKDataPipeline:
    def __init__(self, config_path: str)
    def run(self, export_data: bool = True, export_validation: bool = True) -> Dict[str, Any]
```

#### 方法详解

##### `__init__(config_path: str)`

初始化数据处理管道。

**参数:**
- `config_path` (str): 配置文件路径

**异常:**
- `FileNotFoundError`: 当配置文件不存在时
- `Exception`: 当初始化失败时

**示例:**
```python
pipeline = GFKDataPipeline('config/europe_config.yml')
```

##### `run(export_data: bool = True, export_validation: bool = True) -> Dict[str, Any]`

运行完整的数据处理管道。

**参数:**
- `export_data` (bool): 是否导出处理后的数据，默认True
- `export_validation` (bool): 是否导出验证报告，默认True

**返回:**
- `Dict[str, Any]`: 处理结果字典，包含以下键：
  - `success` (bool): 处理是否成功
  - `final_data` (pd.DataFrame): 最终处理后的数据
  - `validation_results` (dict): 验证结果
  - `export_results` (dict): 导出文件路径
  - `processing_stages` (dict): 各阶段处理统计
  - `pipeline_duration` (float): 总耗时（秒）

**示例:**
```python
results = pipeline.run()
if results['success']:
    print(f"处理完成: {len(results['final_data'])} 行")
```

##### `get_summary_report() -> str`

获取处理总结报告。

**返回:**
- `str`: 格式化的总结报告字符串

##### `export_summary_report(filename: Optional[str] = None) -> str`

导出处理总结报告到文件。

**参数:**
- `filename` (str, optional): 文件名，如果未提供则自动生成

**返回:**
- `str`: 导出文件路径

## 核心处理模块

### DataLoader

数据加载器，负责从各种数据源加载数据。

#### 类定义

```python
class DataLoader:
    def __init__(self, input_directory: str = ".")
    def load_single_file(self, file_path: str, **kwargs) -> Optional[pd.DataFrame]
    def load_country_files(self, countries_config: Dict[str, Dict[str, str]]) -> Dict[str, pd.DataFrame]
    def load_spain_files(self, spain_config: Dict[str, Dict[str, str]]) -> Dict[str, pd.DataFrame]
```

#### 方法详解

##### `load_single_file(file_path: str, **kwargs) -> Optional[pd.DataFrame]`

加载单个CSV文件。

**参数:**
- `file_path` (str): 文件路径
- `**kwargs`: 传递给pd.read_csv的额外参数

**返回:**
- `Optional[pd.DataFrame]`: 加载的DataFrame，如果失败则返回None

**示例:**
```python
loader = DataLoader("./data")
df = loader.load_single_file("data.csv", encoding='utf-8')
```

##### `load_country_files(countries_config: Dict[str, Dict[str, str]]) -> Dict[str, pd.DataFrame]`

加载多个国家的数据文件。

**参数:**
- `countries_config` (dict): 国家配置字典
  - 格式: `{'国家名': {'file': '文件路径'}}`

**返回:**
- `Dict[str, pd.DataFrame]`: 国家数据字典

**示例:**
```python
countries = {
    'Germany': {'file': 'germany_data.csv'},
    'France': {'file': 'france_data.csv'}
}
country_data = loader.load_country_files(countries)
```

### DataCleaner

数据清洗器，负责数据清洗和质量预处理。

#### 类定义

```python
class DataCleaner:
    def __init__(self, cleaning_config: Dict[str, Any])
    def clean_dataframe(self, df: pd.DataFrame, data_name: str = "数据") -> pd.DataFrame
    def remove_total_rows_func(self, df: pd.DataFrame) -> pd.DataFrame
    def handle_missing_values(self, df: pd.DataFrame, strategy: str = 'report') -> pd.DataFrame
    def detect_outliers(self, df: pd.DataFrame, columns: Optional[List[str]] = None) -> Dict[str, Any]
```

#### 方法详解

##### `clean_dataframe(df: pd.DataFrame, data_name: str = "数据") -> pd.DataFrame`

执行完整的数据清洗流程。

**参数:**
- `df` (pd.DataFrame): 要清洗的DataFrame
- `data_name` (str): 数据名称，用于日志输出

**返回:**
- `pd.DataFrame`: 清洗后的DataFrame

**示例:**
```python
cleaning_config = {
    'remove_total_rows': True,
    'columns_to_drop': ['unwanted_col']
}
cleaner = DataCleaner(cleaning_config)
cleaned_df = cleaner.clean_dataframe(raw_df, "原始数据")
```

##### `detect_outliers(df: pd.DataFrame, columns: Optional[List[str]] = None) -> Dict[str, Any]`

检测异常值。

**参数:**
- `df` (pd.DataFrame): 要检测的DataFrame
- `columns` (list, optional): 要检测的列名列表，None表示所有数值列

**返回:**
- `Dict[str, Any]`: 异常值检测结果

### DataTransformer

数据转换器，负责数据格式转换和重塑。

#### 类定义

```python
class DataTransformer:
    def __init__(self, transform_config: Dict[str, Any])
    def transform_dataframe(self, df: pd.DataFrame, data_name: str = "数据") -> pd.DataFrame
    def wide_to_long(self, df: pd.DataFrame) -> pd.DataFrame
    def pivot_by_facts(self, df: pd.DataFrame, pivot_config: Dict[str, Any]) -> pd.DataFrame
    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame
```

#### 方法详解

##### `transform_dataframe(df: pd.DataFrame, data_name: str = "数据") -> pd.DataFrame`

执行完整的数据转换流程。

**参数:**
- `df` (pd.DataFrame): 要转换的DataFrame
- `data_name` (str): 数据名称

**返回:**
- `pd.DataFrame`: 转换后的DataFrame

##### `pivot_by_facts(df: pd.DataFrame, pivot_config: Dict[str, Any]) -> pd.DataFrame`

根据Facts列进行透视。

**参数:**
- `df` (pd.DataFrame): 长格式DataFrame
- `pivot_config` (dict): 透视配置
  - `index_columns` (list): 索引列
  - `pivot_column` (str): 透视列
  - `value_column` (str): 值列

**返回:**
- `pd.DataFrame`: 透视后的DataFrame

**示例:**
```python
pivot_config = {
    'index_columns': ['Country', 'Date'],
    'pivot_column': 'Facts',
    'value_column': 'Value'
}
pivoted_df = transformer.pivot_by_facts(df, pivot_config)
```

### DataValidator

数据验证器，负责数据质量验证和检查。

#### 类定义

```python
class DataValidator:
    def __init__(self, validation_config: Dict[str, Any])
    def validate_dataframe(self, df: pd.DataFrame, data_name: str = "数据") -> Dict[str, Any]
    def check_price_consistency(self, df: pd.DataFrame) -> Dict[str, Any]
    def check_negative_values(self, df: pd.DataFrame) -> Dict[str, Any]
    def generate_validation_report(self, validation_results: Dict[str, Any]) -> str
```

#### 方法详解

##### `validate_dataframe(df: pd.DataFrame, data_name: str = "数据") -> Dict[str, Any]`

执行完整的数据验证流程。

**参数:**
- `df` (pd.DataFrame): 要验证的DataFrame
- `data_name` (str): 数据名称

**返回:**
- `Dict[str, Any]`: 验证结果字典，包含：
  - `passed` (bool): 是否通过验证
  - `issues` (list): 发现的问题列表
  - `consistency_check` (dict): 一致性检查结果
  - `negative_values` (dict): 负值检查结果

**示例:**
```python
validation_config = {
    'consistency_check': {
        'enabled': True,
        'tolerance': 0.01
    }
}
validator = DataValidator(validation_config)
results = validator.validate_dataframe(df, "最终数据")
```

##### `check_price_consistency(df: pd.DataFrame) -> Dict[str, Any]`

检查价格计算一致性 (Price EUR × Units = Value EUR)。

**参数:**
- `df` (pd.DataFrame): 包含价格数据的DataFrame

**返回:**
- `Dict[str, Any]`: 一致性检查结果

### DataExporter

数据导出器，负责数据导出和报告生成。

#### 类定义

```python
class DataExporter:
    def __init__(self, output_config: Dict[str, Any])
    def export_dataframe(self, df: pd.DataFrame, filename: Optional[str] = None, region: str = "DATA", **kwargs) -> str
    def export_with_validation_report(self, df: pd.DataFrame, validation_results: Dict[str, Any], region: str = "DATA") -> Dict[str, str]
    def export_multiple_dataframes(self, data_dict: Dict[str, pd.DataFrame], prefix: str = "GFK") -> Dict[str, str]
```

#### 方法详解

##### `export_dataframe(df: pd.DataFrame, filename: Optional[str] = None, region: str = "DATA", **kwargs) -> str`

导出DataFrame到CSV文件。

**参数:**
- `df` (pd.DataFrame): 要导出的DataFrame
- `filename` (str, optional): 自定义文件名
- `region` (str): 区域名称，用于文件名
- `**kwargs`: 传递给DataFrame.to_csv的额外参数

**返回:**
- `str`: 导出文件的完整路径

**示例:**
```python
output_config = {
    'output_directory': './output',
    'filename_pattern': 'GFK_{region}_PROCESSED_{timestamp}.csv'
}
exporter = DataExporter(output_config)
output_path = exporter.export_dataframe(df, region="EUROPE")
```

## 工具模块

### 工具函数

位于 `gfk_etl_library.utils` 模块中的工具函数。

#### 日期和时间

```python
def generate_timestamp() -> str
```
生成格式为 YYYYMMDD_HHMMSS 的时间戳字符串。

#### 文件操作

```python
def ensure_directory_exists(directory: str) -> None
def get_file_size_mb(file_path: str) -> float
def validate_file_exists(file_path: str, file_description: str = "文件") -> bool
```

#### 数据处理

```python
def get_dataframe_info(df: pd.DataFrame, name: str = "数据") -> Dict[str, Any]
def print_dataframe_summary(df: pd.DataFrame, title: str = "数据摘要") -> None
def validate_required_columns(df: pd.DataFrame, required_columns: List[str], data_name: str = "数据") -> bool
```

#### 文件名处理

```python
def safe_create_filename(pattern: str, **kwargs) -> str
```

**示例:**
```python
from gfk_etl_library.utils import safe_create_filename, generate_timestamp

filename = safe_create_filename(
    'GFK_{region}_PROCESSED_{timestamp}.csv',
    region='EUROPE',
    timestamp=generate_timestamp()
)
```

## 配置管理

### ConfigManager

配置管理器，负责加载和管理YAML配置文件。

#### 类定义

```python
class ConfigManager:
    def __init__(self, config_path: str)
    def get(self, key_path: str, default: Any = None) -> Any
    def get_countries(self) -> Dict[str, Dict[str, str]]
    def get_spain_files(self) -> Dict[str, Dict[str, str]]
    def get_column_mapping(self) -> Dict[str, str]
    def get_date_mapping(self) -> Dict[str, str]
    def get_pivot_config(self) -> Dict[str, Any]
```

#### 方法详解

##### `get(key_path: str, default: Any = None) -> Any`

使用点分隔符获取嵌套配置值。

**参数:**
- `key_path` (str): 配置键路径，如 'processing.cleaning.remove_total_rows'
- `default` (Any): 默认值

**返回:**
- `Any`: 配置值

**示例:**
```python
config = ConfigManager('config/europe_config.yml')
region = config.get('data_sources.region', 'UNKNOWN')
tolerance = config.get('validation.consistency_check.tolerance', 0.01)
```

##### `get_countries() -> Dict[str, Dict[str, str]]`

获取国家配置。

**返回:**
- `Dict[str, Dict[str, str]]`: 国家配置字典

## 异常处理

### 自定义异常

库中定义的自定义异常类：

#### ConfigurationError

配置相关错误。

```python
class ConfigurationError(Exception):
    pass
```

#### DataValidationError

数据验证相关错误。

```python
class DataValidationError(Exception):
    pass
```

#### DataProcessingError

数据处理相关错误。

```python
class DataProcessingError(Exception):
    pass
```

### 错误处理模式

#### 优雅错误处理

```python
try:
    pipeline = GFKDataPipeline('config/invalid_config.yml')
    results = pipeline.run()
except FileNotFoundError as e:
    print(f"配置文件不存在: {e}")
except Exception as e:
    print(f"处理失败: {e}")
```

#### 返回值检查

```python
# 大多数方法会返回可检查的值而不是抛出异常
df = loader.load_single_file('missing_file.csv')
if df is None:
    print("文件加载失败")
else:
    print(f"加载成功: {len(df)} 行")
```

## 类型提示

库中广泛使用类型提示来提高代码可读性：

```python
from typing import Dict, List, Optional, Any, Union
import pandas as pd

def process_data(
    input_data: pd.DataFrame,
    config: Dict[str, Any],
    options: Optional[List[str]] = None
) -> Union[pd.DataFrame, None]:
    # 处理逻辑
    pass
```

## 示例：完整API使用

```python
from gfk_etl_library import GFKDataPipeline
from gfk_etl_library.config import ConfigManager
from gfk_etl_library.core import DataLoader, DataCleaner
from gfk_etl_library.utils import print_dataframe_summary

# 1. 配置管理
config = ConfigManager('config/europe_config.yml')
print(f"处理区域: {config.get('data_sources.region')}")

# 2. 高级管道使用
pipeline = GFKDataPipeline('config/europe_config.yml')
results = pipeline.run(export_data=True, export_validation=True)

# 3. 结果处理
if results['success']:
    final_data = results['final_data']
    print_dataframe_summary(final_data, "最终处理结果")
    
    # 访问各阶段统计
    stages = results['processing_stages']
    print(f"数据加载: {stages['data_loading']['files_loaded']} 个文件")
    print(f"数据清洗: {stages['data_cleaning']['rows_removed']} 行被移除")
    
    # 访问验证结果
    validation = results['validation_results']
    if validation['passed']:
        print("✅ 数据验证通过")
    else:
        print(f"⚠️  发现 {len(validation['issues'])} 个问题")

# 4. 单独使用模块
loader = DataLoader("./data")
df = loader.load_single_file("sample.csv")

cleaning_config = config.get('processing.cleaning')
cleaner = DataCleaner(cleaning_config)
cleaned_df = cleaner.clean_dataframe(df, "示例数据")
```

---

完整的API文档和更多示例请访问项目GitHub仓库。