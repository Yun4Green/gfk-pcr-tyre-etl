# Legacy Scripts - 旧版脚本

此目录包含重构前的原始脚本文件，仅供参考和对比使用。

## 📂 文件说明

### 主要处理脚本
- `process_european_data.py` - 原始的欧洲数据处理脚本
- `process_spain_data.py` - 原始的西班牙数据处理脚本

### 验证和分析脚本
- `verify_calculation.py` - 价格计算一致性验证
- `analyze_inconsistency.py` - 数据不一致性分析
- `trace_negative_to_source.py` - 负值数据追踪
- `verify_spain_data.py` - 西班牙数据验证

### 工具脚本
- `quick_start.py` - 快速入门和项目状态脚本

### 配置文件
- `project_config.json` - 旧版JSON配置文件

## 🔄 迁移对应关系

| 旧版脚本 | 新版对应 |
|---------|----------|
| `process_european_data.py` | `python main.py --config config/europe_config.yml` |
| `process_spain_data.py` | `python main.py --config config/spain_config.yml` |
| `verify_calculation.py` | 内置在Pipeline验证器中 |
| `analyze_inconsistency.py` | 内置在Pipeline验证器中 |
| `quick_start.py` | `python main.py --help` 和 `--list-configs` |

## ⚠️ 重要说明

1. **这些脚本已经被重构的模块化库替代**
2. **建议使用新版本的 `main.py` 和配置文件**
3. **这些文件保留仅供参考，不建议在生产环境中使用**
4. **如果需要特定功能，请参考新版本的实现方式**

## 🚀 使用新版本

```bash
# 替代 process_european_data.py
python main.py --config config/europe_config.yml

# 替代 process_spain_data.py  
python main.py --config config/spain_config.yml

# 查看可用配置
python main.py --list-configs

# 查看帮助信息
python main.py --help
```

## 📚 新版本优势

- ✅ 模块化设计，易于维护
- ✅ 配置文件驱动，无硬编码
- ✅ 内置数据验证和质量检查
- ✅ 完整的错误处理机制
- ✅ 详细的处理报告和日志
- ✅ 完整的测试覆盖
- ✅ 详细的文档和使用示例