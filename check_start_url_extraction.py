#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查生成的增强格式数据中start_url字段是否正确提取
"""
import json

def check_start_url(filename):
    """检查start_url字段"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"文件: {filename}")
        print(f"总记录数: {len(data)}")
        print("\n前5条记录的start_url:")
        
        for i, record in enumerate(data[:5]):
            start_url = record.get('start_url', '')
            if start_url:
                print(f"{i+1}. {start_url}")
            else:
                print(f"{i+1}. (空)")
        
        # 统计有start_url的记录数
        has_start_url = sum(1 for record in data if record.get('start_url', ''))
        print(f"\n有start_url的记录数: {has_start_url}/{len(data)} ({has_start_url/len(data)*100:.1f}%)")
        
    except Exception as e:
        print(f"读取文件失败: {e}")

if __name__ == '__main__':
    print("检查8月数据:")
    check_start_url('enhanced_valid_conversations_august_1_5.json')
    print("\n" + "="*60)
    print("检查9月数据:")
    check_start_url('enhanced_valid_conversations_september_1_5.json')
