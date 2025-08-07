#!/usr/bin/env python3
"""
自定义管道示例

这个示例展示了如何创建自定义的数据处理管道，
以及如何单独使用各个处理模块。
"""

import sys
import os
import pandas as pd

# 添加父目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gfk_etl_library.config import ConfigManager
from gfk_etl_library.core import DataLoader, DataCleaner, DataTransformer, DataValidator, DataExporter


def example_custom_pipeline():
    """自定义管道示例"""
    
    print("🔧 自定义数据处理管道示例")
    print("=" * 50)
    
    try:
        # 1. 加载配置
        config = ConfigManager("config/default_config.yml")
        print("✅ 配置加载成功")
        
        # 2. 单独使用各个模块
        print("\n📦 初始化处理模块...")
        
        # 初始化加载器
        loader = DataLoader(".")
        
        # 初始化清洗器
        cleaning_config = config.get('processing.cleaning', {})
        cleaner = DataCleaner(cleaning_config)
        
        # 初始化转换器
        transform_config = config.get('processing', {})
        transformer = DataTransformer(transform_config)
        
        # 初始化验证器
        validation_config = config.get('validation', {})
        validator = DataValidator(validation_config)
        
        # 初始化导出器
        output_config = config.get('output', {})
        output_config['output_directory'] = './data/processed'
        exporter = DataExporter(output_config)
        
        print("✅ 所有模块初始化完成")
        
        # 3. 模拟数据处理（创建示例数据）
        print("\n📊 创建示例数据...")
        sample_data = create_sample_data()
        
        # 4. 数据清洗
        print("\n🧹 执行数据清洗...")
        cleaned_data = cleaner.clean_dataframe(sample_data, "示例数据")
        
        # 5. 数据转换
        print("\n🔄 执行数据转换...")
        transformed_data = transformer.transform_dataframe(cleaned_data, "示例数据")
        
        # 6. 数据验证
        print("\n✅ 执行数据验证...")
        validation_results = validator.validate_dataframe(transformed_data, "示例数据")
        
        # 7. 数据导出
        print("\n💾 执行数据导出...")
        export_path = exporter.export_dataframe(transformed_data, region="SAMPLE")
        
        if export_path:
            print(f"✅ 数据已导出到: {export_path}")
        
        # 8. 显示验证报告
        print("\n📋 验证报告:")
        report = validator.generate_validation_report(validation_results)
        print(report)
        
        return True
        
    except Exception as e:
        print(f"❌ 自定义管道执行失败: {str(e)}")
        return False


def create_sample_data() -> pd.DataFrame:
    """创建示例数据用于演示"""
    
    # 创建示例数据
    data = {
        'Seasonality': ['Summer'] * 50 + ['Winter'] * 50,
        'Brandlines': ['Sailun'] * 100,
        'Rim Diameter': [15, 16, 17, 18] * 25,
        'Dimension': ['205/55 R16', '225/60 R17', '195/65 R15', '235/55 R18'] * 25,
        'Load Index': [91, 94, 87, 98] * 25,
        'Speed Index': ['V', 'H', 'H', 'V'] * 25,
        'car_type': ['PASSENGER CAR'] * 100,
        'country': ['Germany'] * 100,
        'Facts': ['SALES UNITS', 'PRICE EUR', 'SALES THS. VALUE EUR'] * 33 + ['SALES UNITS'],
        'JUN 24': [100, 50.5, 5050] * 33 + [100],
        'JUL 24': [120, 52.0, 6240] * 33 + [120],
        'AUG 24': [90, 49.5, 4455] * 33 + [90],
        # 添加一些空值和负值用于测试
        'SEP 24': [None] * 10 + [110, 51.0, 5610] * 30,
        'OCT 24': [-5] + [None] * 9 + [105, 50.0, 5250] * 30
    }
    
    df = pd.DataFrame(data)
    
    print(f"创建示例数据: {len(df)} 行 × {len(df.columns)} 列")
    
    return df


def example_module_usage():
    """演示各个模块的独立使用"""
    
    print("\n🔍 模块独立使用示例")
    print("=" * 30)
    
    # 1. 配置管理器示例
    print("\n1. 配置管理器:")
    try:
        config = ConfigManager("config/default_config.yml")
        print(f"   项目名称: {config.get('project.name', 'Unknown')}")
        print(f"   移除TOTAL行: {config.get('processing.cleaning.remove_total_rows', False)}")
        print(f"   日期映射数量: {len(config.get_date_mapping())}")
    except Exception as e:
        print(f"   ❌ 配置加载失败: {e}")
    
    # 2. 数据加载器示例
    print("\n2. 数据加载器:")
    loader = DataLoader(".")
    print(f"   加载器: {loader}")
    print("   (可用于加载CSV文件)")
    
    # 3. 数据清洗器示例
    print("\n3. 数据清洗器:")
    cleaning_config = {
        'remove_total_rows': True,
        'total_patterns': [r'\.TOTAL', r'^TOTAL$']
    }
    cleaner = DataCleaner(cleaning_config)
    print(f"   清洗器: {cleaner}")
    
    # 4. 数据转换器示例
    print("\n4. 数据转换器:")
    transform_config = {
        'column_mapping': {'OLD_COL': 'NEW_COL'},
        'date_mapping': {'JUN 24': '2024-06-01'}
    }
    transformer = DataTransformer(transform_config)
    print(f"   转换器: {transformer}")
    
    # 5. 数据验证器示例
    print("\n5. 数据验证器:")
    validation_config = {
        'consistency_check': {'enabled': True, 'tolerance': 0.01}
    }
    validator = DataValidator(validation_config)
    print(f"   验证器: {validator}")
    
    # 6. 数据导出器示例
    print("\n6. 数据导出器:")
    output_config = {
        'output_directory': './data/processed',
        'filename_pattern': 'CUSTOM_{timestamp}.csv'
    }
    exporter = DataExporter(output_config)
    print(f"   导出器: {exporter}")


def main():
    """主函数"""
    
    print("🚀 GFK ETL库自定义使用示例")
    print("=" * 60)
    
    # 示例1: 模块独立使用
    example_module_usage()
    
    # 示例2: 自定义管道
    print("\n" + "="*60)
    success = example_custom_pipeline()
    
    if success:
        print("\n🎉 自定义管道示例执行成功！")
        print("💡 您可以根据这个示例创建自己的数据处理管道")
        return 0
    else:
        print("\n❌ 自定义管道示例执行失败")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)