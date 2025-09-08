import json
import re
from datetime import datetime, timezone, timedelta
import sys
import os

# 定义香港时区 (UTC+8)
HKT = timezone(timedelta(hours=8))

def clean_chat_data(input_file, output_file):
    """
    清洗聊天数据并统计有效咨询数量
    """
    # 读取输入文件
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"成功读取文件: {input_file}")
        # 打印读取到的对话总数
        initial_chat_count = len(data)
        print(f"输入文件中包含 {initial_chat_count} 条原始对话记录。")

    except FileNotFoundError:
        print(f"错误：找不到文件 {input_file}")
        return 0, 0
    except json.JSONDecodeError:
        print(f"错误：无法解析文件 {input_file}，请检查文件格式是否为有效的JSON。")
        return 0, 0
    except Exception as e:
        print(f"读取文件时发生未知错误: {e}")
        return 0, 0

    cleaned_chats = []

    for chat in data:
        # 提取基本信息
        chat_id = chat.get('id', '')
        users = chat.get('users', [])
        thread = chat.get('thread', {})
        events = thread.get('events', [])

        # 提取客户信息
        customer = next((user for user in users if user.get('type') == 'customer'), {})
        customer_id = customer.get('id') # 获取客户ID用于匹配作者
        customer_name = customer.get('name', '')
        customer_email = customer.get('email', '')
        customer_phone = customer.get('phone', '')
        
        # 提取用户来源信息
        visit_info = customer.get('visit', {})
        session_fields = customer.get('session_fields', [])
        
        # 构建来源数据
        source_data = {
            'referrer': visit_info.get('referrer', ''),
            'ip': visit_info.get('ip', ''),
            'user_agent': visit_info.get('user_agent', ''),
            'geolocation': visit_info.get('geolocation', {}),
            'visit_started_at': visit_info.get('started_at', ''),
            'visit_ended_at': visit_info.get('ended_at', ''),
            'session_fields': session_fields,
            'last_pages': visit_info.get('last_pages', [])
        }

        # 提取对话内容
        messages = []
        for event in events:
            event_type = event.get('type', '')
            created_at = event.get('created_at', '')
            author_id = event.get('author_id', '')
            text = event.get('text', '')

            # 格式化时间
            try:
                # 兼容 'Z' 和非 'Z' 的ISO格式
                if created_at:
                    # 尝试解析时间字符串
                    dt_original = datetime.fromisoformat(created_at.replace('Z', '+00:00'))

                    # 如果是 naive datetime (无时区信息)，假设它是 UTC 并标记
                    if dt_original.tzinfo is None:
                        dt_utc = dt_original.replace(tzinfo=timezone.utc) # 标记为 UTC
                    else:
                        dt_utc = dt_original

                    # 将时间转换为香港时间 (HKT)
                    dt_hkt = dt_utc.astimezone(HKT)

                    # 格式化为不带时区信息的字符串 (保持原有输出格式)
                    formatted_time = dt_hkt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    formatted_time = '' # 处理 None 或空字符串

            except (ValueError, TypeError) as e: # 捕获解析错误
                print(f"WARNING: Could not parse or convert time \'{created_at}\' for chat ID {chat_id}: {e}. Using original string.", file=sys.stderr)
                formatted_time = created_at # 解析失败时使用原始字符串
            except Exception as e: # 捕获其他可能的错误
                print(f"WARNING: Unexpected error processing time \'{created_at}\' for chat ID {chat_id}: {e}. Using original string.", file=sys.stderr)
                formatted_time = created_at # 发生其他错误时使用原始字符串

            # 判断消息发送者
            # 确保 customer_id 存在且 author_id 不是 None 或空字符串
            sender = 'Customer' if customer_id and author_id == customer_id else 'Agent'

            # 跳过系统消息、表单消息和无效消息
            if event_type in ['system_message', 'form']:
                continue
            # 进一步清理文本，移除仅包含空白字符的文本
            if not text or not text.strip():
                continue

            messages.append({
                'time': formatted_time,
                'sender': sender,
                'content': text.strip() # 移除文本两端的空白字符
            })

        # 新的有效咨询判断标准：只要客户留下了联系方式就算有效咨询，但要排除测试数据
        def has_valid_contact_info():
            # 检查是否为测试邮箱
            def is_test_email(email):
                if not email:
                    return False
                email = email.strip().lower()
                # 检查测试邮箱后缀
                test_domains = ['@v-ycfz.com', '@vertu.cn']
                for domain in test_domains:
                    if email.endswith(domain):
                        return True
                # 检查包含test的qq邮箱
                if email.endswith('@qq.com') and 'test' in email:
                    return True
                return False
            
            # 检查是否为测试姓名
            def is_test_name(name):
                if not name:
                    return False
                name = name.strip().lower()
                test_keywords = ['test', 'testing', '测试', 'demo']
                return any(keyword in name for keyword in test_keywords)
            
            # 如果是测试邮箱或测试姓名，直接返回False
            if is_test_email(customer_email) or is_test_name(customer_name):
                return False
            
            # 检查是否有有效的姓名（排除默认值和测试值）
            has_valid_name = customer_name and customer_name.strip() not in ['', 'Anonymous', 'Guest', '匿名用户', '游客']
            # 检查是否有有效的邮箱（排除测试邮箱）
            has_valid_email = customer_email and '@' in customer_email.strip() and not is_test_email(customer_email)
            # 检查是否有有效的电话（至少7位数字）
            phone_digits = ''.join(filter(str.isdigit, customer_phone)) if customer_phone else ''
            has_valid_phone = len(phone_digits) >= 7
            
            return has_valid_name or has_valid_email or has_valid_phone
        
        # 如果客户留下了联系方式，就认为是有效咨询
        if has_valid_contact_info():
            cleaned_chat = {
                'chat_id': chat_id,
                'customer': {
                    'name': customer_name,
                    'email': customer_email,
                    'phone': customer_phone
                },
                'source': source_data,
                'messages': messages
            }
            cleaned_chats.append(cleaned_chat)

    # 打印清洗后保留的对话数量
    cleaned_chat_count = len(cleaned_chats)
    print(f"清洗后保留了 {cleaned_chat_count} 条有效咨询记录（基于联系方式）。")

    # 保存清洗后的数据
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cleaned_chats, f, ensure_ascii=False, indent=2)
        print(f"清洗后的数据已保存到文件: {output_file}")
        print(f"有效咨询数量统计: {cleaned_chat_count}")
        return initial_chat_count, cleaned_chat_count
    except IOError as e:
        print(f"写入文件 {output_file} 时发生IO错误: {e}")
        return initial_chat_count, 0
    except Exception as e:
        print(f"写入文件时发生未知错误: {e}")
        return initial_chat_count, 0

def analyze_source_data(cleaned_data):
    """
    分析来源数据统计
    """
    referrer_stats = {}
    country_stats = {}
    platform_stats = {}
    
    for chat in cleaned_data:
        source = chat.get('source', {})
        
        # 统计referrer
        referrer = source.get('referrer', '')
        if referrer:
            # 提取域名
            try:
                from urllib.parse import urlparse
                domain = urlparse(referrer).netloc
                if domain:
                    referrer_stats[domain] = referrer_stats.get(domain, 0) + 1
                else:
                    referrer_stats['直接访问'] = referrer_stats.get('直接访问', 0) + 1
            except:
                referrer_stats['其他'] = referrer_stats.get('其他', 0) + 1
        else:
            referrer_stats['直接访问'] = referrer_stats.get('直接访问', 0) + 1
        
        # 统计国家
        geolocation = source.get('geolocation', {})
        country = geolocation.get('country', '未知')
        country_stats[country] = country_stats.get(country, 0) + 1
        
        # 统计平台
        session_fields = source.get('session_fields', [])
        platform = '未知'
        for field in session_fields:
            if 'cca_platform' in field:
                platform = field['cca_platform']
                break
        platform_stats[platform] = platform_stats.get(platform, 0) + 1
    
    return referrer_stats, country_stats, platform_stats

def process_files():
    """
    处理两个月份的聊天文件
    """
    files_to_process = [
        {
            'input': 'uploads/chats_8月1-5.json',
            'output': 'uploads/cleaned_chats_8月1-5.json',
            'month': '8月1-5日'
        },
        {
            'input': 'uploads/chats_9月1-5.json', 
            'output': 'uploads/cleaned_chats_9月1-5.json',
            'month': '9月1-5日'
        }
    ]
    
    total_original = 0
    total_cleaned = 0
    
    print("=" * 60)
    print("开始处理月份聊天数据文件")
    print("=" * 60)
    
    for file_info in files_to_process:
        print(f"\n正在处理 {file_info['month']} 的数据...")
        print("-" * 40)
        
        if not os.path.exists(file_info['input']):
            print(f"文件不存在: {file_info['input']}")
            continue
            
        original_count, cleaned_count = clean_chat_data(file_info['input'], file_info['output'])
        total_original += original_count
        total_cleaned += cleaned_count
        
        print(f"{file_info['month']} 处理完成:")
        print(f"  - 原始对话数: {original_count}")
        print(f"  - 有效咨询数: {cleaned_count}")
        if original_count > 0:
            print(f"  - 有效率: {cleaned_count/original_count*100:.1f}%")
        
        # 分析来源数据
        if os.path.exists(file_info['output']):
            try:
                with open(file_info['output'], 'r', encoding='utf-8') as f:
                    cleaned_data = json.load(f)
                
                referrer_stats, country_stats, platform_stats = analyze_source_data(cleaned_data)
                
                print(f"  - 来源统计:")
                print(f"    * 前3个推荐来源: {dict(list(sorted(referrer_stats.items(), key=lambda x: x[1], reverse=True))[:3])}")
                print(f"    * 前3个国家: {dict(list(sorted(country_stats.items(), key=lambda x: x[1], reverse=True))[:3])}")
                print(f"    * 平台分布: {platform_stats}")
            except Exception as e:
                print(f"  - 来源分析失败: {e}")
    
    print("\n" + "=" * 60)
    print("处理总结")
    print("=" * 60)
    print(f"总原始对话数: {total_original}")
    print(f"总有效咨询数: {total_cleaned}")
    if total_original > 0:
        print(f"总体有效率: {total_cleaned/total_original*100:.1f}%")
    print("=" * 60)

if __name__ == '__main__':
    process_files()
