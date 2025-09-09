#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查原始聊天数据的结构，特别是查找 start_url 字段
"""
import json
import sys

def inspect_data_structure(filename, max_records=2):
    """检查数据结构"""
    print(f"\n{'='*60}")
    print(f"检查文件: {filename}")
    print(f"{'='*60}")
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"总记录数: {len(data)}")
        
        for i, record in enumerate(data[:max_records]):
            print(f"\n--- 记录 {i+1} 结构 ---")
            
            # 检查顶层字段
            print("顶层字段:", list(record.keys()))
            
            # 检查users字段
            if 'users' in record:
                print("Users 数量:", len(record['users']))
                for j, user in enumerate(record['users']):
                    print(f"  User {j+1} 类型: {user.get('type', 'unknown')}")
                    print(f"  User {j+1} 字段: {list(user.keys())}")
                    
                    # 检查customer的visit信息
                    if user.get('type') == 'customer' and 'visit' in user:
                        visit = user['visit']
                        print(f"  Visit 字段: {list(visit.keys())}")
                        
                        # 重点检查是否有start_url
                        if 'start_url' in visit:
                            print(f"  ✓ 找到 start_url: {visit['start_url'][:100]}...")
                        
                        # 检查last_pages
                        if 'last_pages' in visit and visit['last_pages']:
                            print(f"  Last pages 数量: {len(visit['last_pages'])}")
                            if visit['last_pages']:
                                first_page = visit['last_pages'][0]
                                print(f"  第一个页面字段: {list(first_page.keys())}")
                                if 'url' in first_page:
                                    print(f"  第一个页面URL: {first_page['url'][:100]}...")
            
            # 检查thread字段
            if 'thread' in record:
                thread = record['thread']
                print(f"Thread 字段: {list(thread.keys())}")
                if 'events' in thread:
                    print(f"Events 数量: {len(thread['events'])}")
            
            print("-" * 40)
        
        # 专门搜索start_url字段
        print(f"\n专门搜索 'start_url' 字段...")
        search_for_start_url(data[:5])  # 搜索前5条记录
        
    except Exception as e:
        print(f"读取文件失败: {e}")

def search_for_start_url(data, path=""):
    """递归搜索start_url字段"""
    if isinstance(data, dict):
        for key, value in data.items():
            current_path = f"{path}.{key}" if path else key
            if key == 'start_url':
                print(f"✓ 找到 start_url 在: {current_path}")
                print(f"  值: {value}")
            else:
                search_for_start_url(value, current_path)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            current_path = f"{path}[{i}]" if path else f"[{i}]"
            search_for_start_url(item, current_path)

def main():
    """主函数"""
    files_to_check = [
        'chats_8月1-5.json',
        'chats_9月1-5.json'
    ]
    
    for filename in files_to_check:
        try:
            inspect_data_structure(filename)
        except FileNotFoundError:
            print(f"文件不存在: {filename}")
        except Exception as e:
            print(f"检查文件 {filename} 时出错: {e}")

if __name__ == '__main__':
    main()
