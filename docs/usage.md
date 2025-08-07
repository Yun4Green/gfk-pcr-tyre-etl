# GFK PCR Tyre ETL 使用指南

> **作者**: Julian Luan (julian.luan@sailun-tyres.eu)  
> **更新时间**: 2025年1月  
> **适用于**: GFK 2025年PCR轮胎市场数据

本文档详细介绍如何使用GFK PCR Tyre ETL库处理2025年汽车轮胎市场数据。

## 目录

1. [安装和设置](#安装和设置)
2. [快速开始](#快速开始)
3. [配置文件详解](#配置文件详解)
4. [核心模块使用](#核心模块使用)
5. [高级用法](#高级用法)
6. [故障排除](#故障排除)

## 安装和设置

### 系统要求

- Python 3.8+
- 8GB+ RAM（处理大文件时）
- 1GB+ 可用磁盘空间

### 安装依赖

```bash
cd etl-pipeline
pip install -r requirements.txt
```

### 验证安装

```bash
python -c "from gfk_etl_library import GFKDataPipeline; print('安装成功!')"
```

## 快速开始

### 1. 处理欧洲数据

```bash
# 使用命令行
python main.py --config config/europe_config.yml

# 或使用Python代码
python examples/process_europe.py
```

### 2. 处理西班牙数据

```bash
# 使用命令行
python main.py --config config/spain_config.yml

# 或使用Python代码
python examples/process_spain.py
```

### 3. 编程方式使用

```python
from gfk_etl_library import GFKDataPipeline

# 创建管道
pipeline = GFKDataPipeline('config/europe_config.yml')

# 执行处理
results = pipeline.run()

# 检查结果
if results['success']:
    final_data = results['final_data']
    print(f"处理完成: {len(final_data)} 行数据")
    
    # 访问导出文件路径
    export_paths = results['export_results']
    print(f"数据文件: {export_paths['data']}")
    print(f"验证报告: {export_paths['validation_report']}")
```

## 配置文件详解

### 配置文件结构

GFK ETL库使用YAML配置文件来管理所有处理参数。

#### 1. 数据源配置

```yaml
data_sources:
  region: "EUROPE"                    # 区域标识
  input_directory: "."                # 输入文件目录
  output_directory: "./data/processed" # 输出目录
  
  countries:                          # 国家文件配置
    Germany:
      code: "DE"
      file: "GFK_FLATFILE_CARTIRE_EUROPE_DE_SAILUN_Jun25_cleaned.csv"
    Spain:
      code: "ES"  
      file: "GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_cleaned.csv"
```

#### 2. 数据处理配置

```yaml
processing:
  # 数据清洗配置
  cleaning:
    remove_total_rows: true           # 是否删除TOTAL行
    total_patterns:                   # TOTAL行识别模式
      - "\\.TOTAL"
      - "^TOTAL$"
      - "\\.TOTAL\\."
    columns_to_drop:                  # 要删除的列
      - "MAT JUN 24"
      - "MAT JUN 25"
      - "YTD JUN 24"
      - "YTD JUN 25"
  
  # 列映射配置
  column_mapping:
    "DIMENSION (Car Tires)": "Dimension"
    "LoadIndex": "Load Index"
    "SpeedIndex": "Speed Index"
    "Type of Vehicle": "car_type"
  
  # 日期映射配置
  date_mapping:
    "JUN 24": "2024-06-01"
    "JUL 24": "2024-07-01"
    # ... 其他月份
  
  # 透视配置
  pivot:
    index_columns:                    # 透视时的索引列
      - "Seasonality"
      - "Brandlines"
      - "Rim Diameter"
      - "Dimension"
      - "Load Index"
      - "Speed Index"
      - "car_type"
      - "country"
      - "Date"
    value_column: "Value"             # 值列
    pivot_column: "Facts"             # 透视列
```

#### 3. 数据验证配置

```yaml
validation:
  consistency_check:
    enabled: true                     # 启用一致性检查
    tolerance: 0.01                   # 允许的差异容忍度
    price_column: "Price EUR"         # 价格列名
    units_column: "Units"             # 单位列名
    value_column: "Value EUR"         # 价值列名
  
  negative_values:
    check_enabled: true               # 启用负值检查
    report_threshold: 10              # 报告阈值
```

#### 4. 输出配置

```yaml
output:
  filename_pattern: "GFK_{region}_PROCESSED_{timestamp}.csv"
  include_timestamp: true             # 包含时间戳
  save_validation_report: true       # 保存验证报告
```

### 配置继承

可以使用`include`机制来继承基础配置：

```yaml
# spain_config.yml
include: "default_config.yml"         # 继承默认配置

# 覆盖特定配置
data_sources:
  region: "SPAIN"
  spain_files:
    "LIGHT TRUCK":
      file: "GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_LT.csv"
```

## 核心模块使用

### 1. 数据加载器 (DataLoader)

```python
from gfk_etl_library.core import DataLoader

# 创建加载器
loader = DataLoader(input_directory="./data")

# 加载单个文件
df = loader.load_single_file("data.csv")

# 加载多个国家文件
countries_config = {
    'Germany': {'file': 'germany_data.csv'},
    'France': {'file': 'france_data.csv'}
}
country_data = loader.load_country_files(countries_config)
```

### 2. 数据清洗器 (DataCleaner)

```python
from gfk_etl_library.core import DataCleaner

# 配置清洗器
cleaning_config = {
    'remove_total_rows': True,
    'total_patterns': [r'\.TOTAL', r'^TOTAL$'],
    'columns_to_drop': ['unwanted_col1', 'unwanted_col2']
}

cleaner = DataCleaner(cleaning_config)

# 清洗数据
cleaned_df = cleaner.clean_dataframe(raw_df, "数据名称")

# 检测异常值
outliers = cleaner.detect_outliers(df, columns=['Price', 'Units'])
```

### 3. 数据转换器 (DataTransformer)

```python
from gfk_etl_library.core import DataTransformer

# 配置转换器
transform_config = {
    'column_mapping': {
        'OldColumn': 'NewColumn'
    },
    'date_mapping': {
        'JUN 24': '2024-06-01'
    }
}

transformer = DataTransformer(transform_config)

# 转换数据
transformed_df = transformer.transform_dataframe(df, "数据名称")

# 执行透视
pivot_config = {
    'index_columns': ['Country', 'Date'],
    'pivot_column': 'Facts',
    'value_column': 'Value'
}
pivoted_df = transformer.pivot_by_facts(df, pivot_config)
```

### 4. 数据验证器 (DataValidator)

```python
from gfk_etl_library.core import DataValidator

# 配置验证器
validation_config = {
    'consistency_check': {
        'enabled': True,
        'tolerance': 0.01,
        'price_column': 'Price EUR',
        'units_column': 'Units',
        'value_column': 'Value EUR'
    },
    'negative_values': {
        'check_enabled': True,
        'report_threshold': 10
    }
}

validator = DataValidator(validation_config)

# 验证数据
validation_results = validator.validate_dataframe(df, "最终数据")

# 生成报告
report = validator.generate_validation_report(validation_results)
print(report)
```

### 5. 数据导出器 (DataExporter)

```python
from gfk_etl_library.core import DataExporter

# 配置导出器
output_config = {
    'output_directory': './output',
    'filename_pattern': 'GFK_{region}_PROCESSED_{timestamp}.csv',
    'include_timestamp': True
}

exporter = DataExporter(output_config)

# 导出数据
output_path = exporter.export_dataframe(df, region="EUROPE")

# 导出带验证报告
export_paths = exporter.export_with_validation_report(
    df, validation_results, region="EUROPE"
)
```

## 高级用法

### 1. 自定义数据处理管道

```python
from gfk_etl_library.config import ConfigManager
from gfk_etl_library.core import *

# 创建自定义管道
class CustomPipeline:
    def __init__(self, config_path):
        self.config = ConfigManager(config_path)
        self._init_modules()
    
    def _init_modules(self):
        # 自定义模块初始化
        self.loader = DataLoader(".")
        self.cleaner = DataCleaner(self.config.get('processing.cleaning'))
        # ... 其他模块
    
    def process_data(self, custom_params):
        # 自定义处理逻辑
        raw_data = self.loader.load_single_file(custom_params['file'])
        cleaned_data = self.cleaner.clean_dataframe(raw_data)
        # ... 自定义处理步骤
        return processed_data

# 使用自定义管道
pipeline = CustomPipeline('config/custom_config.yml')
result = pipeline.process_data({'file': 'special_data.csv'})
```

### 2. 批量处理多个文件

```python
import os
from gfk_etl_library import GFKDataPipeline

# 批量处理
data_files = [
    'config/europe_config.yml',
    'config/spain_config.yml'
]

results = {}
for config_file in data_files:
    region = os.path.basename(config_file).replace('_config.yml', '')
    
    pipeline = GFKDataPipeline(config_file)
    result = pipeline.run()
    
    results[region] = result
    print(f"✅ {region} 处理完成")

# 汇总结果
for region, result in results.items():
    if result['success']:
        final_data = result['final_data']
        print(f"{region}: {len(final_data)} 行数据")
```

### 3. 数据质量监控

```python
from gfk_etl_library.core import DataValidator

def monitor_data_quality(df, thresholds):
    """监控数据质量指标"""
    
    validator = DataValidator({
        'consistency_check': {'enabled': True, 'tolerance': 0.01},
        'negative_values': {'check_enabled': True}
    })
    
    results = validator.validate_dataframe(df, "监控数据")
    
    # 检查质量指标
    quality_score = 100
    
    # 一致性检查
    consistency_rate = results.get('consistency_check', {}).get('consistency_rate', 0)
    if consistency_rate < thresholds['min_consistency']:
        quality_score -= 30
        print(f"⚠️  一致性过低: {consistency_rate:.1f}%")
    
    # 缺失值检查
    missing_percentage = sum(
        info['percentage'] for info in results.get('missing_values', {}).values()
    )
    if missing_percentage > thresholds['max_missing']:
        quality_score -= 20
        print(f"⚠️  缺失值过多: {missing_percentage:.1f}%")
    
    # 负值检查
    negative_rows = results.get('negative_values', {}).get('total_negative_rows', 0)
    if negative_rows > thresholds['max_negative']:
        quality_score -= 10
        print(f"⚠️  负值过多: {negative_rows} 行")
    
    return quality_score, results

# 使用质量监控
thresholds = {
    'min_consistency': 80,  # 最低一致性比例
    'max_missing': 15,      # 最大缺失值比例
    'max_negative': 50      # 最大负值行数
}

score, details = monitor_data_quality(processed_data, thresholds)
print(f"数据质量评分: {score}/100")
```

### 4. 配置文件动态生成

```python
import yaml
from datetime import datetime

def create_dynamic_config(region, files, output_dir):
    """动态创建配置文件"""
    
    config = {
        'project': {
            'name': f'GFK {region} 数据处理',
            'version': '2.0',
            'created': datetime.now().isoformat()
        },
        'data_sources': {
            'region': region.upper(),
            'input_directory': '.',
            'output_directory': output_dir
        },
        'processing': {
            'cleaning': {
                'remove_total_rows': True,
                'total_patterns': [r'\.TOTAL', r'^TOTAL$']
            }
        }
    }
    
    # 添加文件配置
    if region.lower() == 'europe':
        config['data_sources']['countries'] = files
    else:
        config['data_sources']['spain_files'] = files
    
    # 保存配置文件
    config_path = f'config/dynamic_{region.lower()}_config.yml'
    with open(config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
    
    return config_path

# 使用动态配置
files_config = {
    'Germany': {'file': 'germany_data.csv'},
    'France': {'file': 'france_data.csv'}
}

config_path = create_dynamic_config('Europe', files_config, './output')
pipeline = GFKDataPipeline(config_path)
```

## 故障排除

### 常见错误

#### 1. 配置文件不存在

```
❌ 配置文件不存在: config/missing_config.yml
```

**解决方法**:
- 检查文件路径是否正确
- 使用 `--list-configs` 查看可用配置
- 确保配置文件在正确的目录中

#### 2. 数据文件缺失

```
❌ 警告: 文件 'data.csv' 不存在
```

**解决方法**:
- 检查数据文件是否在指定路径
- 更新配置文件中的文件路径
- 确保文件名拼写正确

#### 3. 内存不足

```
MemoryError: Unable to allocate array
```

**解决方法**:
- 确保系统有足够的RAM（建议8GB+）
- 分批处理大文件
- 使用 `low_memory=True` 参数

#### 4. 列名不匹配

```
❌ 错误: 缺少必需的列: ['Missing Column']
```

**解决方法**:
- 检查数据文件的列名
- 更新配置文件中的 `column_mapping`
- 确保输入数据格式正确

### 调试技巧

#### 1. 启用详细输出

```bash
python main.py --config config.yml --verbose
```

#### 2. 分步骤调试

```python
# 单独测试各个模块
from gfk_etl_library.core import DataLoader

loader = DataLoader(".")
df = loader.load_single_file("test.csv")

if df is not None:
    print("✅ 加载成功")
    print(f"数据维度: {df.shape}")
    print(f"列名: {list(df.columns)}")
else:
    print("❌ 加载失败")
```

#### 3. 检查数据质量

```python
# 快速数据检查
def quick_data_check(df):
    print(f"数据行数: {len(df)}")
    print(f"数据列数: {len(df.columns)}")
    print(f"缺失值: {df.isnull().sum().sum()}")
    print(f"重复行: {df.duplicated().sum()}")
    
    # 检查数值列
    numeric_cols = df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        print(f"{col}: {df[col].min()} ~ {df[col].max()}")

quick_data_check(processed_data)
```

#### 4. 验证配置

```python
from gfk_etl_library.config import ConfigManager

# 验证配置加载
try:
    config = ConfigManager('config/test_config.yml')
    print("✅ 配置加载成功")
    print(f"区域: {config.get('data_sources.region')}")
    print(f"国家数量: {len(config.get_countries())}")
except Exception as e:
    print(f"❌ 配置加载失败: {e}")
```

### 性能优化

#### 1. 内存优化

```python
# 分块读取大文件
import pandas as pd

def load_large_file_in_chunks(file_path, chunk_size=10000):
    chunks = []
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # 处理每个块
        processed_chunk = process_chunk(chunk)
        chunks.append(processed_chunk)
    
    return pd.concat(chunks, ignore_index=True)
```

#### 2. 并行处理

```python
from concurrent.futures import ThreadPoolExecutor

def parallel_file_processing(file_configs):
    """并行处理多个文件"""
    
    def process_single_file(file_config):
        loader = DataLoader(".")
        return loader.load_single_file(file_config['file'])
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_single_file, file_configs))
    
    return {config['name']: result for config, result in zip(file_configs, results)}
```

---

更多详细信息请参考 [API文档](api_reference.md) 和 [GitHub Issues](issues)。