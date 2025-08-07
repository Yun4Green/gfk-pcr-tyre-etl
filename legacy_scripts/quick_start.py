#!/usr/bin/env python3
"""
GFK欧洲轮胎数据ETL管道 - 快速入门脚本
用于快速了解项目状态和执行常用操作
"""

import os
import pandas as pd
from datetime import datetime
import json

def print_separator(title):
    """打印分隔符"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")

def check_environment():
    """检查环境和依赖"""
    print_separator("环境检查")
    
    # 检查Python库
    try:
        import pandas as pd
        import numpy as np
        print(f"✅ pandas版本: {pd.__version__}")
        print(f"✅ numpy可用")
    except ImportError as e:
        print(f"❌ 缺少依赖: {e}")
        return False
    
    # 检查内存
    try:
        import psutil
        memory = psutil.virtual_memory()
        print(f"✅ 可用内存: {memory.available / (1024**3):.1f} GB")
    except ImportError:
        print("ℹ️  未安装psutil，无法检查内存")
    
    return True

def show_project_overview():
    """显示项目概览"""
    print_separator("项目概览")
    
    print("📊 GFK欧洲汽车轮胎数据ETL管道")
    print("   处理欧洲7国轮胎销售数据，时间跨度13个月")
    print()
    print("🌍 覆盖国家: 德国、西班牙、法国、英国、意大利、波兰、土耳其")
    print("🚗 车辆类型: 乘用车、轻型卡车、4x4")
    print("📈 数据指标: 销售单位、价格EUR、销售额千EUR")
    print("📅 时间范围: 2024年6月 - 2025年6月")

def check_data_files():
    """检查数据文件状态"""
    print_separator("数据文件检查")
    
    # 检查原始数据文件
    print("📁 原始数据文件:")
    raw_files = [
        "GFK_FLATFILE_CARTIRE_EUROPE_DE_SAILUN_Jun25_cleaned.csv",
        "GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_cleaned.csv", 
        "GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_LT.csv",
        "GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_PAS.csv",
        "GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_4*4.csv",
        "GFK_FLATFILE_CARTIRE_EUROPE_FR_SAILUN_Jun25_cleaned.csv",
        "GFK_FLATFILE_CARTIRE_EUROPE_GB_SAILUN_Jun25_cleaned.csv",
        "GFK_FLATFILE_CARTIRE_EUROPE_IT_SAILUN_Jun25_cleaned.csv",
        "GFK_FLATFILE_CARTIRE_EUROPE_PL_SAILUN_Jun25_cleaned.csv",
        "GFK_FLATFILE_CARTIRE_EUROPE_TR_SAILUN_Jun25_cleaned.csv"
    ]
    
    for file in raw_files:
        if os.path.exists(file):
            size_mb = os.path.getsize(file) / (1024*1024)
            print(f"   ✅ {file} ({size_mb:.1f} MB)")
        else:
            print(f"   ❌ {file} - 文件不存在")
    
    # 检查处理后数据文件
    print("\n📊 处理后数据文件:")
    processed_files = [f for f in os.listdir('.') if f.startswith('GFK_') and f.endswith('_PROCESSED_') and '.csv' in f]
    
    if processed_files:
        for file in sorted(processed_files, reverse=True):
            size_mb = os.path.getsize(file) / (1024*1024)
            print(f"   ✅ {file} ({size_mb:.1f} MB)")
    else:
        print("   ❌ 未找到处理后的数据文件")

def check_scripts():
    """检查脚本文件"""
    print_separator("脚本文件检查")
    
    scripts = {
        "process_european_data.py": "主ETL脚本(7国合并)",
        "process_spain_data.py": "西班牙专用ETL脚本", 
        "verify_calculation.py": "数据计算验证脚本",
        "analyze_inconsistency.py": "数据不一致性分析",
        "trace_negative_to_source.py": "负值追踪脚本",
        "verify_spain_data.py": "西班牙数据验证脚本"
    }
    
    for script, description in scripts.items():
        if os.path.exists(script):
            print(f"   ✅ {script} - {description}")
        else:
            print(f"   ❌ {script} - {description} (文件不存在)")

def show_quick_commands():
    """显示快速命令"""
    print_separator("常用命令")
    
    print("🚀 数据处理命令:")
    print("   python process_european_data.py     # 处理全欧洲数据")
    print("   python process_spain_data.py        # 单独处理西班牙数据")
    print()
    print("🔍 数据验证命令:")
    print("   python verify_calculation.py        # 验证价格计算一致性")
    print("   python analyze_inconsistency.py     # 分析数据不一致性")
    print("   python trace_negative_to_source.py  # 追踪负值数据")
    print("   python verify_spain_data.py         # 验证西班牙数据")
    print()
    print("📋 项目管理:")
    print("   python quick_start.py               # 显示项目状态(当前脚本)")

def show_latest_results():
    """显示最新处理结果"""
    print_separator("最新处理结果")
    
    # 查找最新的处理文件
    processed_files = [f for f in os.listdir('.') if f.startswith('GFK_') and '_PROCESSED_' in f and f.endswith('.csv')]
    
    if not processed_files:
        print("❌ 未找到处理结果文件")
        return
    
    # 按修改时间排序
    processed_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    
    for file in processed_files[:3]:  # 显示最新的3个文件
        try:
            df = pd.read_csv(file, nrows=0)  # 只读取列名
            size_mb = os.path.getsize(file) / (1024*1024)
            mod_time = datetime.fromtimestamp(os.path.getmtime(file))
            
            print(f"\n📁 {file}")
            print(f"   📊 大小: {size_mb:.1f} MB")
            print(f"   📅 修改时间: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   📋 列数: {len(df.columns)}")
            print(f"   🔤 列名: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
            
        except Exception as e:
            print(f"❌ 读取文件失败: {e}")

def show_data_quality_summary():
    """显示数据质量摘要"""
    print_separator("数据质量摘要")
    
    print("⚠️  已知数据质量问题:")
    print("   1. 计算不一致: Price EUR × Units ≠ Value EUR (一致性20-95%)")
    print("   2. 负值数据: 少量销售单位为负值(可能是退货调整)")  
    print("   3. 缺失值: SALES THS. VALUE EUR字段缺失10-15%")
    print("   4. TOTAL行: 原始数据包含汇总行(已自动清洗)")
    print()
    print("💡 使用建议:")
    print("   • 分析前先运行验证脚本了解数据质量")
    print("   • 注意处理缺失值，特别是Value EUR字段")
    print("   • 负值数据需要结合业务逻辑理解")
    print("   • 建议按国家分组分析，质量存在差异")

def interactive_menu():
    """交互式菜单"""
    print_separator("交互式操作")
    
    print("选择要执行的操作:")
    print("1. 运行完整数据处理流程")
    print("2. 只处理西班牙数据")
    print("3. 运行数据验证检查")
    print("4. 显示详细项目信息")
    print("5. 退出")
    
    choice = input("\n请输入选择 (1-5): ").strip()
    
    if choice == "1":
        print("\n正在运行完整数据处理流程...")
        os.system("python process_european_data.py")
        os.system("python verify_calculation.py")
    elif choice == "2":
        print("\n正在处理西班牙数据...")
        os.system("python process_spain_data.py")
        os.system("python verify_spain_data.py")
    elif choice == "3":
        print("\n正在运行数据验证...")
        os.system("python verify_calculation.py")
        os.system("python analyze_inconsistency.py")
    elif choice == "4":
        show_project_overview()
        show_data_quality_summary()
    elif choice == "5":
        print("👋 再见!")
    else:
        print("❌ 无效选择")

def main():
    """主函数"""
    print("🚀 GFK欧洲轮胎数据ETL管道 - 快速入门")
    print(f"⏰ 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 检查环境
    if not check_environment():
        return
    
    # 显示项目状态
    show_project_overview()
    check_data_files()
    check_scripts()
    show_latest_results()
    show_data_quality_summary()
    show_quick_commands()
    
    # 交互式菜单
    try:
        interactive_menu()
    except KeyboardInterrupt:
        print("\n\n👋 程序已中断，再见!")

if __name__ == "__main__":
    main() 