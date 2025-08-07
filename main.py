#!/usr/bin/env python3
"""
GFK ETL Library - 主执行文件

这是重构后的GFK数据处理库的主入口点。
通过配置文件来控制不同的数据处理场景。

使用方法:
    python main.py --config config/europe_config.yml
    python main.py --config config/spain_config.yml
    python main.py --help
"""

import argparse
import sys
import os
from pathlib import Path

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gfk_etl_library import GFKDataPipeline


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='GFK欧洲汽车轮胎数据ETL处理管道',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s --config config/europe_config.yml           # 处理欧洲7国数据
  %(prog)s --config config/spain_config.yml            # 处理西班牙数据
  %(prog)s --config config/europe_config.yml --no-export  # 只验证不导出
  %(prog)s --list-configs                             # 列出可用配置
        """
    )
    
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='配置文件路径 (例如: config/europe_config.yml)'
    )
    
    parser.add_argument(
        '--no-export',
        action='store_true',
        help='不导出数据，仅执行处理和验证'
    )
    
    parser.add_argument(
        '--no-validation-report',
        action='store_true',
        help='不生成验证报告'
    )
    
    parser.add_argument(
        '--list-configs',
        action='store_true',
        help='列出可用的配置文件'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='显示详细输出'
    )
    
    return parser.parse_args()


def list_available_configs():
    """列出可用的配置文件"""
    config_dir = Path('config')
    
    if not config_dir.exists():
        print("❌ 配置目录不存在: config/")
        return
    
    print("📋 可用的配置文件:")
    print("-" * 50)
    
    config_files = list(config_dir.glob('*.yml'))
    
    if not config_files:
        print("未找到配置文件 (.yml)")
        return
    
    # 配置文件描述
    descriptions = {
        'default_config.yml': '默认基础配置（不可直接使用）',
        'europe_config.yml': '欧洲7国数据处理配置',
        'spain_config.yml': '西班牙专用数据处理配置'
    }
    
    for config_file in sorted(config_files):
        name = config_file.name
        desc = descriptions.get(name, '自定义配置')
        print(f"  {name:<25} - {desc}")
    
    print("\n使用方法:")
    print("  python main.py --config config/europe_config.yml")


def validate_config_file(config_path: str) -> bool:
    """验证配置文件是否存在且可访问"""
    if not os.path.exists(config_path):
        print(f"❌ 配置文件不存在: {config_path}")
        print("\n💡 提示: 使用 --list-configs 查看可用配置")
        return False
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            f.read()
        return True
    except Exception as e:
        print(f"❌ 无法读取配置文件: {str(e)}")
        return False


def main():
    """主函数"""
    args = parse_arguments()
    
    # 打印欢迎信息
    print("🚀 GFK欧洲汽车轮胎数据ETL管道 v2.0")
    print("📅 重构版本 - 模块化设计")
    print("=" * 60)
    
    # 处理特殊参数
    if args.list_configs:
        list_available_configs()
        return 0
    
    # 验证必需参数
    if not args.config:
        print("❌ 错误: 必须指定配置文件")
        print("💡 使用 --help 查看帮助信息")
        print("💡 使用 --list-configs 查看可用配置")
        return 1
    
    # 验证配置文件
    if not validate_config_file(args.config):
        return 1
    
    try:
        # 创建并运行管道
        print(f"📋 使用配置: {args.config}")
        
        pipeline = GFKDataPipeline(args.config)
        
        # 执行管道
        results = pipeline.run(
            export_data=not args.no_export,
            export_validation=not args.no_validation_report
        )
        
        # 检查执行结果
        if results.get('success', False):
            print("\n🎉 处理完成！")
            
            # 显示关键结果
            final_data = results.get('final_data')
            if final_data is not None:
                print(f"📊 最终数据: {len(final_data):,} 行 × {len(final_data.columns)} 列")
            
            # 显示导出文件
            export_results = results.get('export_results', {})
            if export_results:
                print(f"📁 导出文件:")
                for file_type, path in export_results.items():
                    print(f"   {file_type}: {path}")
            
            # 保存处理总结
            if not args.no_export:
                summary_path = pipeline.export_summary_report()
                if summary_path:
                    print(f"📋 处理总结: {summary_path}")
            
            return 0
            
        else:
            print("\n❌ 处理失败")
            error = results.get('error', '未知错误')
            print(f"错误信息: {error}")
            return 1
    
    except KeyboardInterrupt:
        print("\n\n⚠️  处理被用户中断")
        return 1
    
    except Exception as e:
        print(f"\n❌ 执行过程中发生错误: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)