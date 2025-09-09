#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json

def analyze_traffic_source(referrer, start_url):
    """
    分析流量来源，区分付费和自然流量 - 修正后的逻辑
    """
    # 合并referrer和start_url进行检查
    combined_url = f"{referrer} {start_url}".lower()
    
    # 检查是否包含付费参数 - 修正逻辑，只要包含任一关键参数即可
    google_paid_params = ['gad_source', 'gclid', 'gad_campaignid', 'gbraid', 'wbraid']
    google_utm_paid = 'utm_source=google' in combined_url and 'utm_medium=cpc' in combined_url
    facebook_paid_params = ['utm_source=fb', 'fbclid']
    other_paid_params = ['utm_campaign']
    
    # 检查是否为付费流量
    is_google_paid = any(param in combined_url for param in google_paid_params) or google_utm_paid
    is_facebook_paid = any(param in combined_url for param in facebook_paid_params)
    is_other_paid = any(param in combined_url for param in other_paid_params)
    
    is_paid = is_google_paid or is_facebook_paid or is_other_paid
    
    # 确定基础渠道
    if not referrer:
        channel = 'direct'
    elif 'google' in referrer.lower():
        channel = 'google'
    elif 'facebook' in referrer.lower() or 'fb.com' in referrer.lower():
        channel = 'facebook'
    elif 'bing' in referrer.lower():
        channel = 'bing'
    elif 'youtube' in referrer.lower():
        channel = 'youtube'
    else:
        channel = 'other'
    
    # 组合渠道和类型
    if is_paid:
        return f"{channel}_paid"
    else:
        return f"{channel}_organic"

def test_logic():
    """测试修正后的逻辑"""
    
    test_cases = [
        # Facebook付费案例
        ("https://www.facebook.com/", "https://example.com/?utm_source=FB&utm_medium=social", "facebook_paid"),
        ("", "https://example.com/?utm_source=fb", "direct_paid"),
        ("https://www.facebook.com/", "https://example.com/?fbclid=123", "facebook_paid"),
        
        # Google付费案例
        ("https://www.google.com/", "https://example.com/?gclid=123", "google_paid"),
        ("", "https://example.com/?gad_source=1", "direct_paid"),
        ("https://www.google.com/", "https://example.com/?utm_source=google&utm_medium=cpc", "google_paid"),
        
        # 自然流量案例
        ("https://www.google.com/", "https://example.com/", "google_organic"),
        ("https://www.facebook.com/", "https://example.com/", "facebook_organic"),
        ("", "https://example.com/", "direct_organic"),
    ]
    
    print("测试修正后的流量识别逻辑:")
    print("=" * 60)
    
    for referrer, start_url, expected in test_cases:
        result = analyze_traffic_source(referrer, start_url)
        status = "✓" if result == expected else "✗"
        print(f"{status} Referrer: {referrer}")
        print(f"  Start URL: {start_url}")
        print(f"  Expected: {expected}, Got: {result}")
        print()

def analyze_sample_data():
    """分析样本数据"""
    try:
        with open('enhanced_valid_conversations_august_1_5.json', 'r', encoding='utf-8') as f:
            august_data = json.load(f)
        
        print(f"\n分析8月1-5日数据，共{len(august_data)}条记录")
        print("=" * 60)
        
        # 统计流量来源
        source_counts = {}
        sample_urls = {}
        
        for record in august_data[:100]:  # 只分析前100条
            referrer = record.get('referrer', '')
            start_url = record.get('start_url', '')
            source = analyze_traffic_source(referrer, start_url)
            
            source_counts[source] = source_counts.get(source, 0) + 1
            
            # 保存样本URL
            if source not in sample_urls:
                sample_urls[source] = {
                    'referrer': referrer,
                    'start_url': start_url
                }
        
        # 显示结果
        for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"{source}: {count}条")
            sample = sample_urls[source]
            print(f"  样本 - Referrer: {sample['referrer']}")
            print(f"  样本 - Start URL: {sample['start_url'][:100]}...")
            print()
            
    except FileNotFoundError:
        print("未找到数据文件")

if __name__ == "__main__":
    test_logic()
    analyze_sample_data()
