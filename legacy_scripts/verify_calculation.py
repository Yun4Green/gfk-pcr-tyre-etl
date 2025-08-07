import pandas as pd
import numpy as np

# 读取处理后的数据
print("正在读取处理后的欧洲数据...")
df = pd.read_csv('GFK_CARTIRE_EUROPE_PROCESSED_20250804_171050.csv')

print(f"数据总行数: {len(df)}")

# 检查是否有缺失值
print("\n=== 缺失值检查 ===")
missing_counts = df[['Price EUR', 'Units', 'Value EUR']].isnull().sum()
print(missing_counts)

# 移除有缺失值的行进行验证
df_clean = df.dropna(subset=['Price EUR', 'Units', 'Value EUR'])
print(f"移除缺失值后的行数: {len(df_clean)}")

# 计算 Price EUR × Units
df_clean['Calculated_Value'] = df_clean['Price EUR'] * df_clean['Units']

# 计算差异
df_clean['Difference'] = abs(df_clean['Calculated_Value'] - df_clean['Value EUR'])
df_clean['Difference_Percent'] = (df_clean['Difference'] / df_clean['Value EUR'] * 100).fillna(0)

# 设置容差（允许小的浮点数误差）
tolerance = 0.01

# 检查一致性
consistent_mask = df_clean['Difference'] <= tolerance
inconsistent_mask = df_clean['Difference'] > tolerance

print(f"\n=== 计算结果验证 ===")
print(f"一致的行数: {consistent_mask.sum()}")
print(f"不一致的行数: {inconsistent_mask.sum()}")
print(f"一致性比例: {consistent_mask.sum() / len(df_clean) * 100:.2f}%")

# 显示不一致的样本
if inconsistent_mask.sum() > 0:
    print(f"\n=== 不一致的样本 (前10个) ===")
    inconsistent_samples = df_clean[inconsistent_mask].head(10)
    for i, (_, row) in enumerate(inconsistent_samples.iterrows(), 1):
        print(f"\n样本 {i}:")
        print(f"  Price EUR: {row['Price EUR']}")
        print(f"  Units: {row['Units']}")
        print(f"  Value EUR: {row['Value EUR']}")
        print(f"  Calculated: {row['Calculated_Value']:.2f}")
        print(f"  Difference: {row['Difference']:.2f}")
        print(f"  Difference %: {row['Difference_Percent']:.2f}%")
        print(f"  Country: {row['country']}")
        print(f"  Date: {row['Date']}")

# 按国家统计一致性
print(f"\n=== 按国家统计一致性 ===")
country_stats = []
for country in df_clean['country'].unique():
    country_data = df_clean[df_clean['country'] == country]
    country_consistent = (country_data['Difference'] <= tolerance).sum()
    country_total = len(country_data)
    consistency_rate = country_consistent / country_total * 100
    
    country_stats.append({
        'Country': country,
        'Total_Rows': country_total,
        'Consistent_Rows': country_consistent,
        'Consistency_Rate': consistency_rate
    })

country_df = pd.DataFrame(country_stats)
print(country_df.to_string(index=False))

# 显示一些统计信息
print(f"\n=== 差异统计 ===")
print(f"平均差异: {df_clean['Difference'].mean():.4f}")
print(f"最大差异: {df_clean['Difference'].max():.4f}")
print(f"差异标准差: {df_clean['Difference'].std():.4f}")

# 检查是否有零值或负值
print(f"\n=== 异常值检查 ===")
zero_price = (df_clean['Price EUR'] == 0).sum()
zero_units = (df_clean['Units'] == 0).sum()
zero_value = (df_clean['Value EUR'] == 0).sum()
negative_price = (df_clean['Price EUR'] < 0).sum()
negative_units = (df_clean['Units'] < 0).sum()
negative_value = (df_clean['Value EUR'] < 0).sum()

print(f"Price EUR = 0: {zero_price} 行")
print(f"Units = 0: {zero_units} 行")
print(f"Value EUR = 0: {zero_value} 行")
print(f"Price EUR < 0: {negative_price} 行")
print(f"Units < 0: {negative_units} 行")
print(f"Value EUR < 0: {negative_value} 行")

# 总结
print(f"\n=== 验证总结 ===")
if consistent_mask.sum() / len(df_clean) >= 0.95:
    print("✅ 数据一致性良好 (≥95%)")
elif consistent_mask.sum() / len(df_clean) >= 0.90:
    print("⚠️  数据一致性一般 (90-95%)")
else:
    print("❌ 数据一致性较差 (<90%)") 