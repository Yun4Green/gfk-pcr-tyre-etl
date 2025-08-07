import pandas as pd
import numpy as np

# 读取数据
print("正在读取数据...")
df = pd.read_csv('GFK_CARTIRE_EUROPE_PROCESSED_20250804_171050.csv', low_memory=False)

# 移除缺失值
df_clean = df.dropna(subset=['Price EUR', 'Units', 'Value EUR']).copy()

# 计算差异
df_clean['Calculated_Value'] = df_clean['Price EUR'] * df_clean['Units']
df_clean['Difference'] = abs(df_clean['Calculated_Value'] - df_clean['Value EUR'])
df_clean['Difference_Percent'] = (df_clean['Difference'] / df_clean['Value EUR'] * 100).fillna(0)

print(f"总数据行数: {len(df_clean)}")

# 分析差异分布
print("\n=== 差异分布分析 ===")
print(f"差异 = 0 的行数: {(df_clean['Difference'] == 0).sum()}")
print(f"差异 ≤ 1 的行数: {(df_clean['Difference'] <= 1).sum()}")
print(f"差异 ≤ 10 的行数: {(df_clean['Difference'] <= 10).sum()}")
print(f"差异 ≤ 100 的行数: {(df_clean['Difference'] <= 100).sum()}")
print(f"差异 > 1000 的行数: {(df_clean['Difference'] > 1000).sum()}")

# 检查是否有负值
print(f"\n=== 负值检查 ===")
negative_mask = (df_clean['Price EUR'] < 0) | (df_clean['Units'] < 0) | (df_clean['Value EUR'] < 0)
print(f"包含负值的行数: {negative_mask.sum()}")

if negative_mask.sum() > 0:
    print("\n负值样本:")
    negative_samples = df_clean[negative_mask].head(5)
    for i, (_, row) in enumerate(negative_samples.iterrows(), 1):
        print(f"样本 {i}: Price={row['Price EUR']}, Units={row['Units']}, Value={row['Value EUR']}, Country={row['country']}")

# 分析大差异的样本
print(f"\n=== 大差异样本分析 ===")
large_diff_mask = df_clean['Difference'] > 1000
print(f"差异 > 1000 的行数: {large_diff_mask.sum()}")

if large_diff_mask.sum() > 0:
    large_diff_samples = df_clean[large_diff_mask].head(5)
    for i, (_, row) in enumerate(large_diff_samples.iterrows(), 1):
        print(f"\n大差异样本 {i}:")
        print(f"  Price EUR: {row['Price EUR']}")
        print(f"  Units: {row['Units']}")
        print(f"  Value EUR: {row['Value EUR']}")
        print(f"  Calculated: {row['Calculated_Value']:.2f}")
        print(f"  Difference: {row['Difference']:.2f}")
        print(f"  Country: {row['country']}")
        print(f"  Date: {row['Date']}")
        print(f"  Seasonality: {row['Seasonality']}")
        print(f"  Brandlines: {row['Brandlines']}")

# 按Facts类型分析（如果原始数据中有这个信息）
print(f"\n=== 按国家分析差异模式 ===")
for country in df_clean['country'].unique():
    country_data = df_clean[df_clean['country'] == country]
    avg_diff = country_data['Difference'].mean()
    max_diff = country_data['Difference'].max()
    consistent_count = (country_data['Difference'] <= 1).sum()
    total_count = len(country_data)
    
    print(f"\n{country}:")
    print(f"  平均差异: {avg_diff:.2f}")
    print(f"  最大差异: {max_diff:.2f}")
    print(f"  完全一致(差异≤1): {consistent_count}/{total_count} ({consistent_count/total_count*100:.1f}%)")

# 检查是否有重复的维度组合
print(f"\n=== 重复维度组合检查 ===")
dimension_cols = ['Seasonality', 'Brandlines', 'Rim Diameter', 'Dimension', 'Load Index', 'Speed Index', 'car_type', 'country', 'Date']
duplicates = df_clean.duplicated(subset=dimension_cols, keep=False)
print(f"重复的维度组合行数: {duplicates.sum()}")

if duplicates.sum() > 0:
    print("\n重复组合示例:")
    duplicate_samples = df_clean[duplicates].groupby(dimension_cols).size().reset_index(name='count')
    duplicate_samples = duplicate_samples[duplicate_samples['count'] > 1].head(3)
    print(duplicate_samples)

# 检查数据来源问题
print(f"\n=== 数据质量检查 ===")
print(f"Price EUR 范围: {df_clean['Price EUR'].min():.2f} - {df_clean['Price EUR'].max():.2f}")
print(f"Units 范围: {df_clean['Units'].min():.2f} - {df_clean['Units'].max():.2f}")
print(f"Value EUR 范围: {df_clean['Value EUR'].min():.2f} - {df_clean['Value EUR'].max():.2f}")

# 检查是否有异常的价格或数量
print(f"\n=== 异常值检查 ===")
high_price = df_clean['Price EUR'] > 1000
high_units = df_clean['Units'] > 10000
high_value = df_clean['Value EUR'] > 1000000

print(f"Price EUR > 1000: {high_price.sum()} 行")
print(f"Units > 10000: {high_units.sum()} 行")
print(f"Value EUR > 1000000: {high_value.sum()} 行")

# 总结
print(f"\n=== 问题总结 ===")
print("数据不一致的主要原因可能是:")
print("1. 原始数据中的Price EUR、Units、Value EUR不是简单的乘法关系")
print("2. 可能存在折扣、税费、汇率转换等因素")
print("3. 数据聚合过程中可能产生了不一致")
print("4. 不同国家的数据可能有不同的计算方式")

print(f"\n建议:")
print("1. 检查原始数据的计算逻辑")
print("2. 确认Price EUR是否包含税费")
print("3. 验证数据聚合过程是否正确")
print("4. 考虑是否需要重新处理原始数据") 