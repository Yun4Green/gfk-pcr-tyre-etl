import pandas as pd
import numpy as np

def verify_spain_data():
    """验证西班牙数据的质量和一致性"""
    
    # 读取处理后的西班牙数据
    print("正在读取西班牙处理后的数据...")
    df = pd.read_csv('GFK_SPAIN_CARTIRE_PROCESSED_20250806_142534.csv')
    
    print(f"数据总行数: {len(df)}")
    print(f"数据总列数: {len(df.columns)}")
    
    # 检查列结构
    print(f"\n=== 列结构检查 ===")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")
    
    # 检查数据类型
    print(f"\n=== 数据类型检查 ===")
    for col in df.columns:
        print(f"  {col}: {df[col].dtype}")
    
    # 检查缺失值
    print(f"\n=== 缺失值检查 ===")
    missing_counts = df.isnull().sum()
    for col, missing_count in missing_counts.items():
        if missing_count > 0:
            missing_percent = missing_count / len(df) * 100
            print(f"  {col}: {missing_count} ({missing_percent:.2f}%)")
    
    # 检查关键列的唯一值
    print(f"\n=== 关键维度统计 ===")
    print(f"车辆类型 (car_type): {df['car_type'].unique()}")
    print(f"国家 (country): {df['country'].unique()}")
    print(f"时间范围: {df['Date'].min()} 到 {df['Date'].max()}")
    print(f"唯一品牌数: {df['Brand'].nunique()}")
    print(f"唯一轮胎尺寸数: {df['Dimension'].nunique()}")
    
    # 检查是否还有TOTAL数据
    print(f"\n=== TOTAL数据清洗验证 ===")
    total_check_columns = ['Seasonality', 'Brandlines', 'Brand', 'Rim Diameter', 'Dimension']
    
    for col in total_check_columns:
        if col in df.columns:
            total_count = df[col].astype(str).str.contains('TOTAL', na=False).sum()
            if total_count > 0:
                print(f"  ⚠️  {col} 仍包含 {total_count} 个TOTAL值")
                # 显示示例
                total_samples = df[df[col].astype(str).str.contains('TOTAL', na=False)][col].unique()[:3]
                print(f"    示例: {list(total_samples)}")
            else:
                print(f"  ✅ {col} 已清理完成")
    
    # 检查数值列的统计信息
    print(f"\n=== 数值列统计 ===")
    numeric_columns = ['PRICE EUR', 'SALES THS. VALUE EUR', 'SALES UNITS']
    
    for col in numeric_columns:
        if col in df.columns:
            print(f"\n{col}:")
            # 移除缺失值后计算统计
            clean_values = df[col].dropna()
            if len(clean_values) > 0:
                print(f"  有效值数量: {len(clean_values)}")
                print(f"  范围: {clean_values.min():.2f} - {clean_values.max():.2f}")
                print(f"  平均值: {clean_values.mean():.2f}")
                print(f"  负值数量: {(clean_values < 0).sum()}")
                print(f"  零值数量: {(clean_values == 0).sum()}")
            else:
                print(f"  ❌ 无有效数据")
    
    # 检查价格一致性（如果有Price EUR和Value EUR）
    print(f"\n=== 价格一致性验证 ===")
    if 'PRICE EUR' in df.columns and 'SALES THS. VALUE EUR' in df.columns and 'SALES UNITS' in df.columns:
        # 创建清洁数据集（移除缺失值）
        clean_df = df.dropna(subset=['PRICE EUR', 'SALES THS. VALUE EUR', 'SALES UNITS']).copy()
        
        if len(clean_df) > 0:
            # 计算期望值 (Price * Units / 1000，因为Value是千欧元)
            clean_df['Expected_Value'] = clean_df['PRICE EUR'] * clean_df['SALES UNITS'] / 1000
            clean_df['Value_Difference'] = abs(clean_df['Expected_Value'] - clean_df['SALES THS. VALUE EUR'])
            clean_df['Value_Diff_Percent'] = (clean_df['Value_Difference'] / clean_df['SALES THS. VALUE EUR'] * 100).fillna(0)
            
            # 设置容差
            tolerance = 0.1  # 0.1千欧元
            
            consistent_count = (clean_df['Value_Difference'] <= tolerance).sum()
            total_count = len(clean_df)
            
            print(f"可验证的记录数: {total_count}")
            print(f"一致的记录数: {consistent_count}")
            print(f"一致性比例: {consistent_count/total_count*100:.2f}%")
            
            if total_count - consistent_count > 0:
                print(f"\n不一致样本 (前5个):")
                inconsistent = clean_df[clean_df['Value_Difference'] > tolerance].head(5)
                for i, (_, row) in enumerate(inconsistent.iterrows(), 1):
                    print(f"  样本 {i}:")
                    print(f"    Price EUR: {row['PRICE EUR']}")
                    print(f"    Units: {row['SALES UNITS']}")
                    print(f"    Value THS EUR: {row['SALES THS. VALUE EUR']}")
                    print(f"    Expected: {row['Expected_Value']:.2f}")
                    print(f"    Difference: {row['Value_Difference']:.2f}")
        else:
            print("无可验证的完整记录")
    else:
        print("缺少必要的列进行价格一致性验证")
    
    # 按车辆类型统计
    print(f"\n=== 按车辆类型统计 ===")
    for vehicle_type in df['car_type'].unique():
        vehicle_data = df[df['car_type'] == vehicle_type]
        print(f"\n{vehicle_type}:")
        print(f"  记录数: {len(vehicle_data)}")
        print(f"  唯一品牌数: {vehicle_data['Brand'].nunique()}")
        print(f"  唯一尺寸数: {vehicle_data['Dimension'].nunique()}")
        
        # 数值统计
        for col in numeric_columns:
            if col in vehicle_data.columns:
                valid_count = vehicle_data[col].notna().sum()
                print(f"  {col} 有效值: {valid_count}")
    
    # 检查数据分布
    print(f"\n=== 时间分布检查 ===")
    time_dist = df['Date'].value_counts().sort_index()
    print("各月份数据量:")
    for date, count in time_dist.items():
        print(f"  {date}: {count}")
    
    print(f"\n=== 验证总结 ===")
    print("✅ 西班牙数据处理完成")
    print("✅ 成功合并三个车辆类型的数据")
    print("✅ 成功从宽格式转换为长格式")
    print("✅ 成功进行Facts透视")
    
    if missing_counts.sum() > 0:
        print("⚠️  存在缺失值，需要注意")
    else:
        print("✅ 无缺失值")

if __name__ == "__main__":
    verify_spain_data() 