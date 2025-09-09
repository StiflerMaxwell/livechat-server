#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
流量来源分析脚本
分析8月和9月的有效对话数据，区分付费和自然流量
"""

import json
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
import re

def analyze_traffic_source(referrer, start_url):
    """
    分析流量来源，区分付费和自然流量
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
    elif 'youtube' in referrer.lower() or 'youtu.be' in referrer.lower():
        channel = 'youtube'
    elif 'facebook' in referrer.lower() or 'fb.com' in referrer.lower() or 'm.facebook.com' in referrer.lower():
        channel = 'facebook'
    elif 'instagram' in referrer.lower():
        channel = 'instagram'
    elif 'bing' in referrer.lower():
        channel = 'bing'
    elif 'yandex' in referrer.lower():
        channel = 'yandex'
    elif 'vertu.com' in referrer.lower():
        channel = 'website_internal'
    else:
        # 解析域名
        try:
            parsed = urlparse(referrer)
            domain = parsed.netloc.lower().replace('www.', '')
            channel = f'website_{domain}'
        except:
            channel = 'other'
    
    # 返回渠道类型
    if is_paid:
        return f"{channel}_paid"
    else:
        return f"{channel}_organic"

def analyze_month_data(file_path, month_name):
    """
    分析单月数据
    """
    print(f"\n分析 {month_name} 数据...")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"读取文件失败: {e}")
        return {}
    
    # 统计数据
    stats = defaultdict(int)
    detailed_stats = defaultdict(list)
    
    for record in data:
        referrer = record.get('referrer', '')
        start_url = record.get('start_url', '')
        original_channel = record.get('channel', '')
        
        # 分析真实来源
        traffic_source = analyze_traffic_source(referrer, start_url)
        stats[traffic_source] += 1
        
        # 记录详细信息
        detailed_stats[traffic_source].append({
            'id': record.get('id', ''),
            'email': record.get('email', ''),
            'country': record.get('country', ''),
            'original_channel': original_channel,
            'referrer': referrer,
            'start_url': start_url
        })
    
    return stats, detailed_stats

def compare_months():
    """
    对比8月和9月数据
    """
    print("=" * 80)
    print("流量来源分析报告")
    print("=" * 80)
    
    # 分析8月数据
    aug_stats, aug_details = analyze_month_data(
        'enhanced_valid_conversations_august_1_5.json', 
        '8月1-5日'
    )
    
    # 分析9月数据
    sep_stats, sep_details = analyze_month_data(
        'enhanced_valid_conversations_september_1_5.json', 
        '9月1-5日'
    )
    
    # 汇总所有来源
    all_sources = set(list(aug_stats.keys()) + list(sep_stats.keys()))
    
    print(f"\n{'来源分类':<25} {'8月数量':<10} {'9月数量':<10} {'总计':<8} {'变化':<15}")
    print("-" * 80)
    
    # 按类别分组
    organic_sources = {}
    paid_sources = {}
    
    for source in sorted(all_sources):
        aug_count = aug_stats.get(source, 0)
        sep_count = sep_stats.get(source, 0)
        total = aug_count + sep_count
        
        if aug_count > 0 and sep_count > 0:
            change = f"{((sep_count - aug_count) / aug_count * 100):+.1f}%"
        elif aug_count > 0:
            change = "新增"
        elif sep_count > 0:
            change = "消失"
        else:
            change = "-"
        
        print(f"{source:<25} {aug_count:<10} {sep_count:<10} {total:<8} {change:<15}")
        
        # 分类汇总
        if '_paid' in source:
            base_source = source.replace('_paid', '')
            paid_sources[base_source] = paid_sources.get(base_source, 0) + total
        elif '_organic' in source:
            base_source = source.replace('_organic', '')
            organic_sources[base_source] = organic_sources.get(base_source, 0) + total
    
    # 付费 vs 自然流量汇总
    print("\n" + "=" * 80)
    print("付费 vs 自然流量汇总")
    print("=" * 80)
    
    print(f"\n付费流量渠道：")
    total_paid_aug = sum(aug_stats.get(k, 0) for k in aug_stats.keys() if '_paid' in k)
    total_paid_sep = sum(sep_stats.get(k, 0) for k in sep_stats.keys() if '_paid' in k)
    
    for source, count in sorted(paid_sources.items(), key=lambda x: x[1], reverse=True):
        aug_paid = sum(aug_stats.get(k, 0) for k in aug_stats.keys() if k.startswith(source + '_paid'))
        sep_paid = sum(sep_stats.get(k, 0) for k in sep_stats.keys() if k.startswith(source + '_paid'))
        print(f"  {source:<15}: 8月 {aug_paid:>3}条 | 9月 {sep_paid:>3}条 | 总计 {count:>3}条")
    
    print(f"\n自然流量渠道：")
    total_organic_aug = sum(aug_stats.get(k, 0) for k in aug_stats.keys() if '_organic' in k)
    total_organic_sep = sum(sep_stats.get(k, 0) for k in sep_stats.keys() if '_organic' in k)
    
    for source, count in sorted(organic_sources.items(), key=lambda x: x[1], reverse=True):
        aug_organic = sum(aug_stats.get(k, 0) for k in aug_stats.keys() if k.startswith(source + '_organic'))
        sep_organic = sum(sep_stats.get(k, 0) for k in sep_stats.keys() if k.startswith(source + '_organic'))
        print(f"  {source:<15}: 8月 {aug_organic:>3}条 | 9月 {sep_organic:>3}条 | 总计 {count:>3}条")
    
    # 总体统计
    print(f"\n📊 总体统计：")
    print(f"  付费流量总计: 8月 {total_paid_aug}条 | 9月 {total_paid_sep}条 | 总计 {total_paid_aug + total_paid_sep}条")
    print(f"  自然流量总计: 8月 {total_organic_aug}条 | 9月 {total_organic_sep}条 | 总计 {total_organic_aug + total_organic_sep}条")
    
    aug_total = total_paid_aug + total_organic_aug
    sep_total = total_paid_sep + total_organic_sep
    
    if aug_total > 0:
        aug_paid_rate = total_paid_aug / aug_total * 100
        aug_organic_rate = total_organic_aug / aug_total * 100
    else:
        aug_paid_rate = aug_organic_rate = 0
        
    if sep_total > 0:
        sep_paid_rate = total_paid_sep / sep_total * 100
        sep_organic_rate = total_organic_sep / sep_total * 100
    else:
        sep_paid_rate = sep_organic_rate = 0
    
    print(f"\n📈 转化率分析：")
    print(f"  8月付费流量占比: {aug_paid_rate:.1f}%")
    print(f"  8月自然流量占比: {aug_organic_rate:.1f}%")
    print(f"  9月付费流量占比: {sep_paid_rate:.1f}%")
    print(f"  9月自然流量占比: {sep_organic_rate:.1f}%")
    
    # 详细分析YouTube和Facebook付费流量
    print(f"\n🎥 YouTube流量详细分析：")
    youtube_paid_aug = [k for k in aug_stats.keys() if 'youtube_paid' in k]
    youtube_paid_sep = [k for k in sep_stats.keys() if 'youtube_paid' in k]
    youtube_organic_aug = [k for k in aug_stats.keys() if 'youtube_organic' in k]
    youtube_organic_sep = [k for k in sep_stats.keys() if 'youtube_organic' in k]
    
    print(f"  YouTube付费: 8月 {sum(aug_stats.get(k, 0) for k in youtube_paid_aug)}条 | 9月 {sum(sep_stats.get(k, 0) for k in youtube_paid_sep)}条")
    print(f"  YouTube自然: 8月 {sum(aug_stats.get(k, 0) for k in youtube_organic_aug)}条 | 9月 {sum(sep_stats.get(k, 0) for k in youtube_organic_sep)}条")
    
    print(f"\n📘 Facebook流量详细分析：")
    facebook_paid_aug = [k for k in aug_stats.keys() if 'facebook_paid' in k]
    facebook_paid_sep = [k for k in sep_stats.keys() if 'facebook_paid' in k]
    facebook_organic_aug = [k for k in aug_stats.keys() if 'facebook_organic' in k]
    facebook_organic_sep = [k for k in sep_stats.keys() if 'facebook_organic' in k]
    
    print(f"  Facebook付费: 8月 {sum(aug_stats.get(k, 0) for k in facebook_paid_aug)}条 | 9月 {sum(sep_stats.get(k, 0) for k in facebook_paid_sep)}条")
    print(f"  Facebook自然: 8月 {sum(aug_stats.get(k, 0) for k in facebook_organic_aug)}条 | 9月 {sum(sep_stats.get(k, 0) for k in facebook_organic_sep)}条")

if __name__ == '__main__':
    compare_months()
