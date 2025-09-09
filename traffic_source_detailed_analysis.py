#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
详细流量来源分析
展示具体的付费参数识别案例
"""

import json
from urllib.parse import urlparse, parse_qs
import re

def extract_paid_indicators(url):
    """
    提取付费流量的具体指标
    """
    if not url:
        return []
    
    indicators = []
    url_lower = url.lower()
    
    # 检查各种付费参数 - 修正逻辑，单独判断每个参数
    paid_patterns = {
        'Google Ads': ['gclid=', 'gad_source=', 'gad_campaignid=', 'gbraid=', 'wbraid='],
        'Google UTM': ['utm_source=google', 'utm_medium=cpc'],
        'Facebook Ads': ['fbclid=', 'utm_source=fb'],  # 移除utm_medium=fb的强制要求
        'Campaign': ['utm_campaign='],
        'Other UTM': ['utm_term=', 'utm_content=']
    }
    
    for category, patterns in paid_patterns.items():
        for pattern in patterns:
            if pattern in url_lower:
                indicators.append(f"{category}: {pattern}")
    
    return indicators

def analyze_detailed_sources():
    """
    详细分析流量来源
    """
    print("=" * 100)
    print("详细流量来源分析报告")
    print("=" * 100)
    
    for month_file, month_name in [
        ('enhanced_valid_conversations_august_1_5.json', '8月1-5日'),
        ('enhanced_valid_conversations_september_1_5.json', '9月1-5日')
    ]:
        print(f"\n🗓️ {month_name} 详细分析")
        print("-" * 60)
        
        try:
            with open(month_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"读取文件失败: {e}")
            continue
        
        # 分类统计
        youtube_paid = []
        youtube_organic = []
        facebook_paid = []
        facebook_organic = []
        google_paid = []
        google_organic = []
        other_paid = []
        
        for record in data:
            referrer = record.get('referrer', '')
            start_url = record.get('start_url', '')
            email = record.get('email', '')
            country = record.get('country', '')
            
            # 合并URL进行分析
            combined_url = f"{referrer} {start_url}"
            paid_indicators = extract_paid_indicators(combined_url)
            
            record_info = {
                'email': email,
                'country': country,
                'referrer': referrer,
                'start_url': start_url,
                'paid_indicators': paid_indicators
            }
            
            # YouTube分类
            if 'youtube' in referrer.lower() or 'youtu.be' in referrer.lower():
                if paid_indicators:
                    youtube_paid.append(record_info)
                else:
                    youtube_organic.append(record_info)
            
            # Facebook分类
            elif any(x in referrer.lower() for x in ['facebook', 'fb.com', 'm.facebook.com']):
                if paid_indicators:
                    facebook_paid.append(record_info)
                else:
                    facebook_organic.append(record_info)
            
            # Google分类
            elif 'google' in referrer.lower():
                if paid_indicators:
                    google_paid.append(record_info)
                else:
                    google_organic.append(record_info)
            
            # 其他付费流量
            elif paid_indicators:
                other_paid.append(record_info)
        
        # 输出YouTube分析
        print(f"\n🎥 YouTube流量分析:")
        print(f"  📊 付费流量: {len(youtube_paid)}条")
        for i, record in enumerate(youtube_paid[:3], 1):
            print(f"    {i}. {record['email']} ({record['country']})")
            print(f"       付费标识: {', '.join(record['paid_indicators'])}")
            if record['start_url']:
                print(f"       起始URL: {record['start_url'][:100]}...")
        
        print(f"  🌱 自然流量: {len(youtube_organic)}条")
        for i, record in enumerate(youtube_organic[:3], 1):
            print(f"    {i}. {record['email']} ({record['country']})")
            print(f"       来源: {record['referrer']}")
        
        # 输出Facebook分析
        print(f"\n📘 Facebook流量分析:")
        print(f"  📊 付费流量: {len(facebook_paid)}条")
        for i, record in enumerate(facebook_paid[:3], 1):
            print(f"    {i}. {record['email']} ({record['country']})")
            print(f"       付费标识: {', '.join(record['paid_indicators'])}")
        
        print(f"  🌱 自然流量: {len(facebook_organic)}条")
        for i, record in enumerate(facebook_organic[:3], 1):
            print(f"    {i}. {record['email']} ({record['country']})")
            print(f"       来源: {record['referrer']}")
        
        # 输出Google分析
        print(f"\n🔍 Google流量分析:")
        print(f"  📊 付费流量: {len(google_paid)}条")
        for i, record in enumerate(google_paid[:3], 1):
            print(f"    {i}. {record['email']} ({record['country']})")
            print(f"       付费标识: {', '.join(record['paid_indicators'])}")
        
        print(f"  🌱 自然流量: {len(google_organic)}条 (仅显示前3条)")
        for i, record in enumerate(google_organic[:3], 1):
            print(f"    {i}. {record['email']} ({record['country']})")
        
        # 其他付费流量
        if other_paid:
            print(f"\n💰 其他付费流量: {len(other_paid)}条")
            for i, record in enumerate(other_paid[:3], 1):
                print(f"    {i}. {record['email']} ({record['country']})")
                print(f"       来源: {record['referrer']}")
                print(f"       付费标识: {', '.join(record['paid_indicators'])}")

if __name__ == '__main__':
    analyze_detailed_sources()
