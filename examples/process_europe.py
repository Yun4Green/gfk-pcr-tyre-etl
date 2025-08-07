#!/usr/bin/env python3
"""
欧洲数据处理示例

这个示例展示了如何使用GFK ETL库处理欧洲7国的轮胎数据。
相当于原来的 process_european_data.py 的重构版本。
"""

import sys
import os

# 添加父目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gfk_etl_library import GFKDataPipeline


def main():
    """处理欧洲数据的主函数"""
    
    print("🇪🇺 GFK欧洲7国轮胎数据处理")
    print("=" * 50)
    
    # 配置文件路径
    config_path = "config/europe_config.yml"
    
    try:
        # 创建并运行管道
        pipeline = GFKDataPipeline(config_path)
        
        print("\n开始处理欧洲数据...")
        results = pipeline.run()
        
        if results.get('success', False):
            print("\n✅ 欧洲数据处理成功完成！")
            
            # 显示处理统计
            stages = results.get('processing_stages', {})
            
            if 'data_loading' in stages:
                loading = stages['data_loading']
                print(f"📥 加载: {loading['files_loaded']} 个国家文件")
                print(f"   数据源: {', '.join(loading['data_sources'])}")
            
            if 'data_cleaning' in stages:
                cleaning = stages['data_cleaning']
                print(f"🧹 清洗: {cleaning['total_rows_before']:,} → {cleaning['total_rows_after']:,} 行")
            
            if 'data_transformation' in stages:
                transform = stages['data_transformation']
                print(f"🔄 转换: 最终 {transform['final_rows']:,} 行 × {transform['final_columns']} 列")
            
            if 'data_validation' in stages:
                validation = stages['data_validation']
                print(f"✅ 验证: {'通过' if validation['validation_passed'] else '存在问题'}")
                if 'consistency_rate' in validation and validation['consistency_rate'] > 0:
                    print(f"   价格一致性: {validation['consistency_rate']:.1f}%")
            
            # 显示导出文件
            export_results = results.get('export_results', {})
            if export_results:
                print(f"\n📁 输出文件:")
                for file_type, path in export_results.items():
                    print(f"   {file_type}: {path}")
        
        else:
            print("\n❌ 欧洲数据处理失败")
            error = results.get('error', '未知错误')
            print(f"错误: {error}")
            return 1
    
    except Exception as e:
        print(f"\n❌ 处理过程中发生错误: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    print(f"\n程序退出，状态码: {exit_code}")
    sys.exit(exit_code)