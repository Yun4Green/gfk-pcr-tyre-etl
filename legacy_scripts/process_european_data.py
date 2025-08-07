import pandas as pd
import numpy as np
import os
from datetime import datetime

def process_country_data(file_path, country_name):
    """处理单个国家的数据"""
    print(f"正在处理 {country_name} 数据...")
    
    # 读取数据
    df = pd.read_csv(file_path)
    
    # 删除指定的列
    columns_to_drop = ['MAT JUN 24', 'MAT JUN 25', 'YTD JUN 24', 'YTD JUN 25']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    # 重命名列以匹配模板
    column_mapping = {
        'DIMENSION (Car Tires)': 'Dimension',
        'LoadIndex': 'Load Index',
        'SpeedIndex': 'Speed Index',
        'Type of Vehicle': 'car_type'
    }
    df = df.rename(columns=column_mapping)
    
    # 添加国家列
    df['country'] = country_name
    
    # 将月份列转换为长格式
    month_columns = ['JUN 24', 'JUL 24', 'AUG 24', 'SEP 24', 'OCT 24', 'NOV 24', 'DEC 24',
                    'JAN 25', 'FEB 25', 'MAR 25', 'APR 25', 'MAY 25', 'JUN 25']
    
    # 创建日期映射
    date_mapping = {
        'JUN 24': '2024-06-01', 'JUL 24': '2024-07-01', 'AUG 24': '2024-08-01',
        'SEP 24': '2024-09-01', 'OCT 24': '2024-10-01', 'NOV 24': '2024-11-01',
        'DEC 24': '2024-12-01', 'JAN 25': '2025-01-01', 'FEB 25': '2025-02-01',
        'MAR 25': '2025-03-01', 'APR 25': '2025-04-01', 'MAY 25': '2025-05-01',
        'JUN 25': '2025-06-01'
    }
    
    # 转换为长格式
    long_data = []
    
    for _, row in df.iterrows():
        for month_col in month_columns:
            if month_col in df.columns:
                value = row[month_col]
                if pd.notna(value) and value != 0:  # 只保留非空非零值
                    new_row = {
                        'Seasonality': row['Seasonality'],
                        'Brandlines': row['Brandlines'],
                        'Rim Diameter': row['Rim Diameter'],
                        'Dimension': row['Dimension'],
                        'Load Index': row['Load Index'],
                        'Speed Index': row['Speed Index'],
                        'car_type': row['car_type'],
                        'country': row['country'],
                        'Date': date_mapping[month_col],
                        'Facts': row['Facts'],
                        'Value': value
                    }
                    long_data.append(new_row)
    
    return pd.DataFrame(long_data)

def pivot_by_facts(df):
    """根据Facts列进行透视"""
    print("正在根据Facts列进行透视...")
    
    # 创建透视表
    pivot_df = df.pivot_table(
        index=['Seasonality', 'Brandlines', 'Rim Diameter', 'Dimension', 'Load Index', 
               'Speed Index', 'car_type', 'country', 'Date'],
        columns='Facts',
        values='Value',
        aggfunc='sum'
    ).reset_index()
    
    # 重命名列
    pivot_df.columns.name = None
    
    return pivot_df

def main():
    # 定义国家文件映射
    country_files = {
        'Germany': 'GFK_FLATFILE_CARTIRE_EUROPE_DE_SAILUN_Jun25_cleaned.csv',
        'Spain': 'GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_cleaned.csv',
        'France': 'GFK_FLATFILE_CARTIRE_EUROPE_FR_SAILUN_Jun25_cleaned.csv',
        'United Kingdom': 'GFK_FLATFILE_CARTIRE_EUROPE_GB_SAILUN_Jun25_cleaned.csv',
        'Italy': 'GFK_FLATFILE_CARTIRE_EUROPE_IT_SAILUN_Jun25_cleaned.csv',
        'Poland': 'GFK_FLATFILE_CARTIRE_EUROPE_PL_SAILUN_Jun25_cleaned.csv',
        'Turkey': 'GFK_FLATFILE_CARTIRE_EUROPE_TR_SAILUN_Jun25_cleaned.csv'
    }
    
    all_data = []
    
    # 处理每个国家的数据
    for country, file_path in country_files.items():
        if os.path.exists(file_path):
            country_data = process_country_data(file_path, country)
            all_data.append(country_data)
            print(f"{country} 数据处理完成，行数: {len(country_data)}")
        else:
            print(f"警告: 文件 {file_path} 不存在")
    
    # 合并所有数据
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\n合并后总行数: {len(combined_df)}")
        
        # 根据Facts列进行透视
        final_df = pivot_by_facts(combined_df)
        
        # 生成输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'GFK_CARTIRE_EUROPE_PROCESSED_{timestamp}.csv'
        
        # 保存结果
        final_df.to_csv(output_file, index=False)
        print(f"\n处理完成！输出文件: {output_file}")
        print(f"最终数据行数: {len(final_df)}")
        print(f"最终数据列数: {len(final_df.columns)}")
        
        # 显示列信息
        print("\n最终列结构:")
        for i, col in enumerate(final_df.columns, 1):
            print(f"  {i}. {col}")
    else:
        print("错误: 没有找到任何数据文件")

if __name__ == "__main__":
    main() 