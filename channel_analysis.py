import json
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from collections import defaultdict

# 定义香港时区 (UTC+8)
HKT = timezone(timedelta(hours=8))

def classify_channel(referrer, session_fields=None):
    """
    根据referrer和session_fields分类为免费或付费渠道
    """
    if not referrer:
        return "直接访问", "免费"
    
    referrer = referrer.lower()
    
    # 付费渠道关键词
    paid_keywords = [
        'google.com/aclk',  # Google Ads
        'googleadservices.com',
        'doubleclick.net',
        'facebook.com/tr',  # Facebook Ads
        'bing.com/aclk',   # Bing Ads
        'yahoo.com/aclk',  # Yahoo Ads
        'baidu.com/aclk',  # 百度推广
        'so.com/aclk',     # 360推广
        'sogou.com/aclk',  # 搜狗推广
        'amazon.com/gp/aw/cr',  # Amazon广告
        'instagram.com/ads',
        'twitter.com/i/adsct',
        'tiktok.com/ads',
        'linkedin.com/li/track',
        'pinterest.com/ct',
        'snapchat.com/ct',
        'utm_medium=cpc',   # 付费点击
        'utm_medium=paid',  # 付费媒体
        'utm_source=google_ads',
        'utm_source=facebook_ads',
        'utm_source=bing_ads',
        'gclid=',          # Google Ads点击ID
        'fbclid=',         # Facebook点击ID
        'msclkid=',        # Microsoft Ads点击ID
    ]
    
    # 检查是否为付费渠道
    for keyword in paid_keywords:
        if keyword in referrer:
            domain = urlparse(referrer).netloc
            return domain or "付费广告", "付费"
    
    # 免费渠道分类
    try:
        domain = urlparse(referrer).netloc
        if not domain:
            return "其他", "免费"
            
        # 社交媒体 (有机流量)
        social_domains = [
            'facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com',
            'youtube.com', 'tiktok.com', 'pinterest.com', 'snapchat.com',
            'weibo.com', 'wechat.com', 'qq.com'
        ]
        
        # 搜索引擎 (有机搜索)
        search_domains = [
            'google.com', 'bing.com', 'yahoo.com', 'baidu.com',
            'so.com', 'sogou.com', 'yandex.com', 'duckduckgo.com'
        ]
        
        # 检查域名分类
        for social in social_domains:
            if social in domain:
                return domain, "免费"
                
        for search in search_domains:
            if search in domain:
                return domain, "免费"
                
        # 其他网站推荐
        return domain, "免费"
        
    except:
        return "其他", "免费"

def analyze_channel_data(input_file):
    """
    分析聊天数据的渠道分布
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"成功读取文件: {input_file}")
        print(f"总对话数: {len(data)}")
    except Exception as e:
        print(f"读取文件错误: {e}")
        return {}

    # 存储统计数据
    channel_stats = {
        '免费': defaultdict(int),
        '付费': defaultdict(int)
    }
    
    valid_consultations = {
        '免费': 0,
        '付费': 0
    }
    
    # 存储样本数据用于验证
    samples = {
        '免费': [],
        '付费': []
    }

    for chat in data:
        # 提取基本信息
        users = chat.get('users', [])
        if not users:
            continue
            
        customer = next((user for user in users if user.get('type') == 'customer'), {})
        customer_name = customer.get('name', '')
        customer_email = customer.get('email', '')
        customer_phone = customer.get('phone', '')
        
        # 检查是否为有效咨询（只要有邮箱或手机号就算）
        def has_valid_contact_info():
            def is_test_email(email):
                if not email:
                    return False
                email = email.strip().lower()
                test_domains = ['@v-ycfz.com', '@vertu.cn']
                for domain in test_domains:
                    if email.endswith(domain):
                        return True
                if email.endswith('@qq.com') and 'test' in email:
                    return True
                return False
            
            def is_test_name(name):
                if not name:
                    return False
                name = name.strip().lower()
                test_keywords = ['test', 'testing', '测试', 'demo']
                return any(keyword in name for keyword in test_keywords)
            
            # 如果是测试邮箱或测试姓名，直接返回False
            if is_test_email(customer_email) or is_test_name(customer_name):
                return False
            
            # 只要有有效的邮箱或手机号就算有效咨询
            has_valid_email = customer_email and '@' in customer_email.strip() and not is_test_email(customer_email)
            phone_digits = ''.join(filter(str.isdigit, customer_phone)) if customer_phone else ''
            has_valid_phone = len(phone_digits) >= 7
            
            return has_valid_email or has_valid_phone
        
        if not has_valid_contact_info():
            continue
            
        # 提取来源信息
        visit_info = customer.get('visit', {})
        referrer = visit_info.get('referrer', '')
        session_fields = customer.get('session_fields', [])
        
        # 分类渠道
        channel_name, channel_type = classify_channel(referrer, session_fields)
        
        # 统计
        channel_stats[channel_type][channel_name] += 1
        valid_consultations[channel_type] += 1
        
        # 收集样本（每种类型最多10个）
        if len(samples[channel_type]) < 10:
            samples[channel_type].append({
                'referrer': referrer,
                'channel_name': channel_name,
                'customer_name': customer_name,
                'customer_email': customer_email
            })
    
    return {
        'channel_stats': channel_stats,
        'valid_consultations': valid_consultations,
        'samples': samples
    }

def generate_channel_report(month_name, input_file):
    """
    生成指定月份的渠道分析报告
    """
    print(f"\n{'='*60}")
    print(f"{month_name} 渠道分析报告")
    print(f"{'='*60}")
    
    results = analyze_channel_data(input_file)
    
    if not results:
        print("分析失败")
        return None
    
    channel_stats = results['channel_stats']
    valid_consultations = results['valid_consultations']
    samples = results['samples']
    
    print(f"\n有效咨询总数: {sum(valid_consultations.values())}")
    print(f"  - 免费渠道: {valid_consultations['免费']} ({valid_consultations['免费']/sum(valid_consultations.values())*100:.1f}%)")
    print(f"  - 付费渠道: {valid_consultations['付费']} ({valid_consultations['付费']/sum(valid_consultations.values())*100:.1f}%)")
    
    # 详细的免费渠道分布
    print(f"\n免费渠道详细分布:")
    free_channels = sorted(channel_stats['免费'].items(), key=lambda x: x[1], reverse=True)
    for channel, count in free_channels:
        percentage = count / valid_consultations['免费'] * 100 if valid_consultations['免费'] > 0 else 0
        print(f"  - {channel}: {count} ({percentage:.1f}%)")
    
    # 详细的付费渠道分布
    print(f"\n付费渠道详细分布:")
    paid_channels = sorted(channel_stats['付费'].items(), key=lambda x: x[1], reverse=True)
    for channel, count in paid_channels:
        percentage = count / valid_consultations['付费'] * 100 if valid_consultations['付费'] > 0 else 0
        print(f"  - {channel}: {count} ({percentage:.1f}%)")
    
    # 显示样本数据用于验证
    print(f"\n样本数据验证:")
    print(f"免费渠道样本 (前5个):")
    for i, sample in enumerate(samples['免费'][:5]):
        print(f"  {i+1}. {sample['channel_name']} - {sample['referrer'][:50]}...")
    
    print(f"付费渠道样本 (前5个):")
    for i, sample in enumerate(samples['付费'][:5]):
        print(f"  {i+1}. {sample['channel_name']} - {sample['referrer'][:50]}...")
    
    return valid_consultations

def main():
    """
    主函数：分析8月和9月的渠道数据
    """
    files_to_analyze = [
        {
            'file': 'chats_8月1-5.json',
            'month': '8月1-5日'
        },
        {
            'file': 'chats_9月1-5.json',
            'month': '9月1-5日'
        }
    ]
    
    total_results = {
        '8月': None,
        '9月': None
    }
    
    for file_info in files_to_analyze:
        month_key = '8月' if '8月' in file_info['month'] else '9月'
        total_results[month_key] = generate_channel_report(file_info['month'], file_info['file'])
    
    # 生成汇总报告
    print(f"\n{'='*60}")
    print("汇总报告")
    print(f"{'='*60}")
    
    for month, results in total_results.items():
        if results:
            print(f"\n{month}:")
            print(f"  免费渠道有效咨询: {results['免费']}")
            print(f"  付费渠道有效咨询: {results['付费']}")
            print(f"  总计: {results['免费'] + results['付费']}")
    
    # 对比分析
    if total_results['8月'] and total_results['9月']:
        print(f"\n月度对比:")
        aug_total = total_results['8月']['免费'] + total_results['8月']['付费']
        sep_total = total_results['9月']['免费'] + total_results['9月']['付费']
        
        print(f"  总有效咨询变化: {aug_total} → {sep_total} ({(sep_total-aug_total)/aug_total*100:+.1f}%)" if aug_total > 0 else "")
        print(f"  免费渠道变化: {total_results['8月']['免费']} → {total_results['9月']['免费']} ({(total_results['9月']['免费']-total_results['8月']['免费'])/total_results['8月']['免费']*100:+.1f}%)" if total_results['8月']['免费'] > 0 else "")
        print(f"  付费渠道变化: {total_results['8月']['付费']} → {total_results['9月']['付费']} ({(total_results['9月']['付费']-total_results['8月']['付费'])/total_results['8月']['付费']*100:+.1f}%)" if total_results['8月']['付费'] > 0 else "")

if __name__ == '__main__':
    main()
