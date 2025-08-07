import pandas as pd
import numpy as np

# 读取处理后的数据，找出负值
print("正在读取处理后的数据...")
processed_df = pd.read_csv('GFK_CARTIRE_EUROPE_PROCESSED_20250804_171050.csv', low_memory=False)

# 找出负值数据
negative_mask = (processed_df['Price EUR'] < 0) | (processed_df['Units'] < 0) | (processed_df['Value EUR'] < 0)
negative_data = processed_df[negative_mask].copy()

print(f"找到 {len(negative_data)} 行负值数据")

# 按国家分组查找原始数据
country_files = {
    'Poland': 'GFK_FLATFILE_CARTIRE_EUROPE_PL_SAILUN_Jun25_cleaned.csv',
    'United Kingdom': 'GFK_FLATFILE_CARTIRE_EUROPE_GB_SAILUN_Jun25_cleaned.csv',
    'Spain': 'GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_cleaned.csv'
}

print("\n=== 追踪负值到原始数据 ===")

for country, file_path in country_files.items():
    country_negative = negative_data[negative_data['country'] == country]
    if len(country_negative) == 0:
        continue
        
    print(f"\n--- {country} 负值数据追踪 ---")
    print(f"负值行数: {len(country_negative)}")
    
    # 读取原始数据
    try:
        original_df = pd.read_csv(file_path)
        print(f"原始数据行数: {len(original_df)}")
        
        # 为每个负值记录查找原始数据
        for i, (_, negative_row) in enumerate(country_negative.head(5).iterrows(), 1):  # 只显示前5个
            print(f"\n负值记录 {i}:")
            print(f"  处理后: Price={negative_row['Price EUR']}, Units={negative_row['Units']}, Value={negative_row['Value EUR']}")
            print(f"  维度: {negative_row['Seasonality']}, {negative_row['Brandlines']}, {negative_row['Rim Diameter']}, {negative_row['Dimension']}")
            print(f"  日期: {negative_row['Date']}")
            
            # 在原始数据中查找匹配的记录
            # 根据日期确定月份列
            date_to_month = {
                '2024-06-01': 'JUN 24', '2024-07-01': 'JUL 24', '2024-08-01': 'AUG 24',
                '2024-09-01': 'SEP 24', '2024-10-01': 'OCT 24', '2024-11-01': 'NOV 24',
                '2024-12-01': 'DEC 24', '2025-01-01': 'JAN 25', '2025-02-01': 'FEB 25',
                '2025-03-01': 'MAR 25', '2025-04-01': 'APR 25', '2025-05-01': 'MAY 25',
                '2025-06-01': 'JUN 25'
            }
            
            month_col = date_to_month.get(negative_row['Date'])
            if month_col and month_col in original_df.columns:
                # 查找匹配的维度组合
                mask = (
                    (original_df['Seasonality'] == negative_row['Seasonality']) &
                    (original_df['Brandlines'] == negative_row['Brandlines']) &
                    (original_df['Rim Diameter'] == negative_row['Rim Diameter']) &
                    (original_df['DIMENSION (Car Tires)'] == negative_row['Dimension']) &
                    (original_df['LoadIndex'] == negative_row['Load Index']) &
                    (original_df['SpeedIndex'] == negative_row['Speed Index']) &
                    (original_df['Type of Vehicle'] == negative_row['car_type'])
                )
                
                matching_records = original_df[mask]
                print(f"  找到 {len(matching_records)} 条匹配的原始记录")
                
                if len(matching_records) > 0:
                    for j, (_, orig_row) in enumerate(matching_records.iterrows(), 1):
                        print(f"  原始记录 {j}:")
                        print(f"    Facts: {orig_row['Facts']}")
                        print(f"    {month_col}: {orig_row[month_col]}")
                        
                        # 检查是否包含负值
                        if pd.notna(orig_row[month_col]) and orig_row[month_col] < 0:
                            print(f"    ⚠️  原始数据中确实存在负值: {orig_row[month_col]}")
                        elif pd.notna(orig_row[month_col]):
                            print(f"    ✅ 原始数据值: {orig_row[month_col]}")
                        else:
                            print(f"    ❓ 原始数据为空值")
                else:
                    print(f"  ❌ 未找到匹配的原始记录")
            else:
                print(f"  ❌ 无法确定月份列: {negative_row['Date']}")
                
    except Exception as e:
        print(f"读取 {country} 原始数据时出错: {e}")

# 特别检查波兰数据（负值最多）
print(f"\n=== 详细检查波兰数据 ===")
try:
    poland_original = pd.read_csv('GFK_FLATFILE_CARTIRE_EUROPE_PL_SAILUN_Jun25_cleaned.csv')
    
    # 检查原始数据中是否有负值
    month_columns = ['JUN 24', 'JUL 24', 'AUG 24', 'SEP 24', 'OCT 24', 'NOV 24', 'DEC 24',
                    'JAN 25', 'FEB 25', 'MAR 25', 'APR 25', 'MAY 25', 'JUN 25']
    
    print("波兰原始数据中的负值统计:")
    for col in month_columns:
        if col in poland_original.columns:
            negative_count = (poland_original[col] < 0).sum()
            if negative_count > 0:
                print(f"  {col}: {negative_count} 个负值")
                
                # 显示一些负值样本
                negative_samples = poland_original[poland_original[col] < 0].head(3)
                for _, sample in negative_samples.iterrows():
                    print(f"    样本: Facts={sample['Facts']}, {col}={sample[col]}")
                    
except Exception as e:
    print(f"检查波兰数据时出错: {e}")

print(f"\n=== 总结 ===")
print("负值数据可能来源:")
print("1. 原始数据中确实存在负值（退货、调整等）")
print("2. 数据聚合过程中的计算错误")
print("3. 数据转换过程中的问题")
print("4. 原始数据质量问题") 