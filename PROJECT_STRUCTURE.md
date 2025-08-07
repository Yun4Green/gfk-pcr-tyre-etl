# 📁 项目结构概览

**GFK PCR Tyre ETL** - 重构后的GFK PCR轮胎数据处理库目录结构，专门用于处理2025年CSV数据。

```
etl-pipeline/                          # 🏠 项目根目录
├── 📂 gfk_etl_library/                # 🔥 核心库目录
│   ├── __init__.py                   # 包初始化文件
│   ├── pipeline.py                   # 🎯 主管道类
│   ├── config.py                     # ⚙️ 配置管理器
│   ├── utils.py                      # 🛠️ 工具函数
│   └── 📂 core/                      # 核心处理模块
│       ├── __init__.py              
│       ├── loader.py                 # 📥 数据加载器
│       ├── cleaner.py                # 🧹 数据清洗器
│       ├── transformer.py           # 🔄 数据转换器
│       ├── validator.py              # ✅ 数据验证器
│       └── exporter.py               # 💾 数据导出器
│
├── 📂 config/                        # 📋 配置文件目录
│   ├── default_config.yml           # 基础配置模板
│   ├── europe_config.yml            # 欧洲数据配置
│   └── spain_config.yml             # 西班牙数据配置
│
├── 📂 data/                          # 📊 数据目录
│   ├── 📂 raw/                       # 原始数据文件
│   │   ├── GFK_FLATFILE_CARTIRE_EUROPE_DE_SAILUN_Jun25_cleaned.csv
│   │   ├── GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_cleaned.csv
│   │   ├── GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_LT.csv
│   │   ├── GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_PAS.csv
│   │   ├── GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_4*4.csv
│   │   ├── GFK_FLATFILE_CARTIRE_EUROPE_FR_SAILUN_Jun25_cleaned.csv
│   │   ├── GFK_FLATFILE_CARTIRE_EUROPE_GB_SAILUN_Jun25_cleaned.csv
│   │   ├── GFK_FLATFILE_CARTIRE_EUROPE_IT_SAILUN_Jun25_cleaned.csv
│   │   ├── GFK_FLATFILE_CARTIRE_EUROPE_PL_SAILUN_Jun25_cleaned.csv
│   │   └── GFK_FLATFILE_CARTIRE_EUROPE_TR_SAILUN_Jun25_cleaned.csv
│   ├── 📂 processed/                 # 处理后数据文件
│   │   ├── GFK_CARTIRE_EUROPE_PROCESSED_20250804_171050.csv
│   │   ├── GFK_CARTIRE_FOR_TABLEAU_FINAL_FILTERED_Nov24_20250804_143240.csv
│   │   └── GFK_SPAIN_CARTIRE_PROCESSED_20250806_142534.csv
│   └── 📂 reports/                   # 数据质量报告
│       └── negative_values_data.csv
│
├── 📂 examples/                      # 📖 使用示例
│   ├── process_europe.py            # 欧洲数据处理示例
│   ├── process_spain.py             # 西班牙数据处理示例
│   └── custom_pipeline.py           # 自定义管道示例
│
├── 📂 tests/                         # 🧪 测试文件
│   ├── __init__.py
│   ├── test_config.py               # 配置管理测试
│   ├── test_cleaner.py              # 数据清洗测试
│   ├── test_transformer.py          # 数据转换测试
│   ├── test_validator.py            # 数据验证测试
│   └── test_pipeline.py             # 管道集成测试
│
├── 📂 docs/                          # 📚 文档目录
│   ├── usage.md                     # 使用指南
│   └── api_reference.md             # API参考文档
│
├── 📂 legacy_scripts/                # 📦 旧版脚本（已弃用）
│   ├── README.md                    # 旧版脚本说明
│   ├── process_european_data.py     # 原欧洲数据处理脚本
│   ├── process_spain_data.py        # 原西班牙数据处理脚本
│   ├── analyze_inconsistency.py     # 原数据分析脚本
│   ├── trace_negative_to_source.py  # 原负值追踪脚本
│   ├── verify_calculation.py        # 原验证脚本
│   ├── verify_spain_data.py         # 原西班牙验证脚本
│   ├── quick_start.py               # 原快速启动脚本
│   └── project_config.json          # 原JSON配置文件
│
├── main.py                          # 🚀 主执行文件
├── setup.py                         # 📦 包安装配置
├── requirements.txt                 # 📋 依赖列表
├── README.md                        # 📖 项目说明文档
└── PROJECT_STRUCTURE.md             # 📁 本文件（项目结构说明）
```

## 🎯 目录功能说明

### 📂 核心目录

- **`gfk_etl_library/`** - 重构后的核心库，包含所有处理逻辑
- **`config/`** - 配置文件，替代硬编码参数
- **`data/`** - 统一的数据管理目录
  - `raw/` - 原始输入数据
  - `processed/` - 处理后的输出数据  
  - `reports/` - 数据质量和验证报告

### 📖 辅助目录

- **`examples/`** - 实际使用示例，替代原始脚本
- **`tests/`** - 完整的测试套件
- **`docs/`** - 详细的文档和API参考
- **`legacy_scripts/`** - 旧版脚本归档，仅供参考

## 🔄 使用方式对比

### 旧版方式（已弃用）
```bash
python legacy_scripts/process_european_data.py
python legacy_scripts/process_spain_data.py
```

### 新版方式（推荐）
```bash
# 主程序方式
python main.py --config config/europe_config.yml
python main.py --config config/spain_config.yml

# 示例脚本方式
python examples/process_europe.py
python examples/process_spain.py

# 编程方式
from gfk_etl_library import GFKDataPipeline
pipeline = GFKDataPipeline('config/europe_config.yml')
results = pipeline.run()
```

## 📊 数据流向

```
data/raw/ → gfk_etl_library → data/processed/
                            ↘ data/reports/
```

## 🔧 配置文件层次

```
default_config.yml (基础配置)
    ↓ (继承)
europe_config.yml / spain_config.yml (特定配置)
```

## ✅ 文件整理完成

所有文件已按功能和用途合理分类，项目结构清晰明了：

1. ✅ **原始数据** → `data/raw/`
2. ✅ **处理后数据** → `data/processed/`
3. ✅ **质量报告** → `data/reports/`
4. ✅ **旧版脚本** → `legacy_scripts/`
5. ✅ **新版库代码** → `gfk_etl_library/`
6. ✅ **配置文件** → `config/`
7. ✅ **测试文件** → `tests/`
8. ✅ **文档** → `docs/`
9. ✅ **示例代码** → `examples/`

现在项目具有清晰的结构，便于维护和使用！