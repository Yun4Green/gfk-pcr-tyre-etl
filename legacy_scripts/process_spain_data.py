import pandas as pd
import numpy as np
import os
from datetime import datetime

def clean_total_rows(df):
    """删除所有包含.TOTAL或TOTAL的行"""
    print(f"清洗前行数: {len(df)}")
    
    # 需要检查的列
    columns_to_check = ['Seasonality', 'Rim Diameter', 'DIMENSION (Car Tires)', 
                       'SpeedIndex', 'LoadIndex', 'Brandlines', 'Brand']
    
    # 创建过滤条件 - 排除包含.TOTAL或TOTAL的行
    mask = pd.Series([True] * len(df))
    
    for col in columns_to_check:
        if col in df.columns:
            # 排除包含.TOTAL、TOTAL、.TOTAL.的行
            col_mask = ~(
                df[col].astype(str).str.contains(r'\.TOTAL', na=False) |
                df[col].astype(str).str.contains(r'^TOTAL$', na=False) |
                df[col].astype(str).str.contains(r'\.TOTAL\.', na=False)
            )
            mask = mask & col_mask
    
    cleaned_df = df[mask].copy()
    print(f"清洗后行数: {len(cleaned_df)}")
    print(f"删除了 {len(df) - len(cleaned_df)} 行包含TOTAL的数据")
    
    return cleaned_df

def process_spain_file(file_path, vehicle_type):
    """处理单个西班牙数据文件"""
    print(f"\n正在处理 {vehicle_type} 数据: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"警告: 文件 {file_path} 不存在")
        return None
    
    # 读取数据
    df = pd.read_csv(file_path)
    print(f"原始数据行数: {len(df)}")
    
    # 清除TOTAL行
    df = clean_total_rows(df)
    
    # 删除指定的列
    columns_to_drop = ['MAT JUN 24', 'MAT JUN 25', 'YTD JUN 24', 'YTD JUN 25']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    # 重命名列以匹配标准格式
    column_mapping = {
        'Fact': 'Facts',
        'TYPE OF VEHICLE': 'Type of Vehicle',
        'DIMENSION (Car Tires)': 'Dimension',
        'LoadIndex': 'Load Index',
        'SpeedIndex': 'Speed Index'
    }
    df = df.rename(columns=column_mapping)
    
    # 添加国家列
    df['country'] = 'Spain'
    
    # 定义月份列
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
                        'Brand': row.get('Brand', ''),  # 添加Brand列
                        'Rim Diameter': row['Rim Diameter'],
                        'Dimension': row['Dimension'],
                        'Load Index': row['Load Index'],
                        'Speed Index': row['Speed Index'],
                        'car_type': row['Type of Vehicle'],
                        'country': row['country'],
                        'Date': date_mapping[month_col],
                        'Facts': row['Facts'],
                        'Value': value
                    }
                    long_data.append(new_row)
    
    result_df = pd.DataFrame(long_data)
    print(f"转换为长格式后行数: {len(result_df)}")
    
    return result_df

def pivot_by_facts(df):
    """根据Facts列进行透视"""
    print("\n正在根据Facts列进行透视...")
    print(f"透视前数据行数: {len(df)}")
    
    # 检查Facts列的唯一值
    print("Facts列的唯一值:")
    for fact in df['Facts'].unique():
        print(f"  - {fact}")
    
    # 创建透视表
    pivot_df = df.pivot_table(
        index=['Seasonality', 'Brandlines', 'Brand', 'Rim Diameter', 'Dimension', 
               'Load Index', 'Speed Index', 'car_type', 'country', 'Date'],
        columns='Facts',
        values='Value',
        aggfunc='sum'
    ).reset_index()
    
    # 重命名列
    pivot_df.columns.name = None
    
    print(f"透视后数据行数: {len(pivot_df)}")
    print("透视后的列:")
    for i, col in enumerate(pivot_df.columns, 1):
        print(f"  {i}. {col}")
    
    return pivot_df

def main():
    # 定义西班牙数据文件
    spain_files = {
        'LIGHT TRUCK': 'GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_LT.csv',
        'PASSENGER CAR': 'GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_PAS.csv',
        '4X4': 'GFK_FLATFILE_CARTIRE_EUROPE_ES_SAILUN_Jun25_4*4.csv'
    }
    
    all_spain_data = []
    
    # 处理每个西班牙数据文件
    for vehicle_type, file_path in spain_files.items():
        spain_data = process_spain_file(file_path, vehicle_type)
        if spain_data is not None:
            all_spain_data.append(spain_data)
            print(f"{vehicle_type} 数据处理完成，行数: {len(spain_data)}")
    
    # 合并所有西班牙数据
    if all_spain_data:
        combined_spain_df = pd.concat(all_spain_data, ignore_index=True)
        print(f"\n西班牙数据合并完成，总行数: {len(combined_spain_df)}")
        
        # 根据Facts列进行透视
        final_spain_df = pivot_by_facts(combined_spain_df)
        
        # 生成输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'GFK_SPAIN_CARTIRE_PROCESSED_{timestamp}.csv'
        
        # 保存结果
        final_spain_df.to_csv(output_file, index=False)
        print(f"\n西班牙数据处理完成！")
        print(f"输出文件: {output_file}")
        print(f"最终数据行数: {len(final_spain_df)}")
        print(f"最终数据列数: {len(final_spain_df.columns)}")
        
        # 显示数据质量统计
        print(f"\n=== 数据质量统计 ===")
        print(f"唯一的车辆类型: {final_spain_df['car_type'].unique()}")
        print(f"数据时间范围: {final_spain_df['Date'].min()} 到 {final_spain_df['Date'].max()}")
        print(f"唯一品牌数量: {final_spain_df['Brand'].nunique()}")
        
        # 检查是否有缺失值
        missing_summary = final_spain_df.isnull().sum()
        if missing_summary.sum() > 0:
            print(f"\n缺失值统计:")
            for col, missing_count in missing_summary.items():
                if missing_count > 0:
                    print(f"  {col}: {missing_count}")
        else:
            print(f"\n✅ 无缺失值")
            
    else:
        print("错误: 没有找到任何西班牙数据文件")

if __name__ == "__main__":
    main() 