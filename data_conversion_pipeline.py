#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据清洗和转换管道
从原始聊天数据到增强的有效对话数据的完整转换流程

有效咨询判断标准：
- 必须有有效的邮箱或电话号码
- 仅有名字的记录将被排除
- 排除测试账号和数据
"""

import json
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import sys
import os

# 定义香港时区 (UTC+8)
HKT = timezone(timedelta(hours=8))

class ChatDataProcessor:
    """聊天数据处理器"""
    
    def __init__(self):
        self.test_domains = ['@v-ycfz.com', '@vertu.cn']
        self.test_keywords = ['test', 'testing', '测试', 'demo']
        self.invalid_names = ['', 'Anonymous', 'Guest', '匿名用户', '游客']
    
    def clean_chat_data(self, input_file, output_file):
        """
        第一步：清洗原始聊天数据，提取有效咨询
        
        有效咨询标准：
        - 必须有有效的邮箱或电话号码
        - 仅有名字的记录将被排除
        - 排除测试邮箱和测试姓名
        """
        print(f"开始清洗聊天数据: {input_file}")
        
        # 读取输入文件
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"成功读取文件: {input_file}")
            initial_chat_count = len(data)
            print(f"输入文件中包含 {initial_chat_count} 条原始对话记录。")
        except Exception as e:
            print(f"读取文件失败: {e}")
            return 0, 0

        cleaned_chats = []

        for chat in data:
            # 提取基本信息
            chat_id = chat.get('id', '')
            users = chat.get('users', [])
            thread = chat.get('thread', {})
            events = thread.get('events', [])
            
            # 提取thread properties信息
            thread_properties = thread.get('properties', {})
            routing_info = thread_properties.get('routing', {})

            # 提取客户信息
            customer = next((user for user in users if user.get('type') == 'customer'), {})
            customer_id = customer.get('id')
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
                'last_pages': visit_info.get('last_pages', []),
                'start_url': routing_info.get('start_url', '')  # 从thread.properties.routing提取start_url
            }

            # 提取对话内容
            messages = []
            for event in events:
                event_type = event.get('type', '')
                created_at = event.get('created_at', '')
                author_id = event.get('author_id', '')
                text = event.get('text', '')

                # 格式化时间
                formatted_time = self._format_time(created_at, chat_id)

                # 判断消息发送者
                sender = 'Customer' if customer_id and author_id == customer_id else 'Agent'

                # 跳过系统消息、表单消息和无效消息
                if event_type in ['system_message', 'form']:
                    continue
                if not text or not text.strip():
                    continue

                messages.append({
                    'time': formatted_time,
                    'sender': sender,
                    'content': text.strip()
                })

            # 判断是否为有效咨询（必须有邮箱或电话，仅有名字的不算）
            referrer = source_data.get('referrer', '')
            if self._is_valid_consultation(customer_name, customer_email, customer_phone, referrer):
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

        # 保存清洗后的数据
        cleaned_chat_count = len(cleaned_chats)
        print(f"清洗后保留了 {cleaned_chat_count} 条有效咨询记录。")

        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(cleaned_chats, f, ensure_ascii=False, indent=2)
            print(f"清洗后的数据已保存到文件: {output_file}")
            return initial_chat_count, cleaned_chat_count
        except Exception as e:
            print(f"写入文件失败: {e}")
            return initial_chat_count, 0

    def convert_to_enhanced_format(self, cleaned_file, enhanced_file):
        """
        第二步：将清洗后的数据转换为增强格式
        """
        print(f"开始转换为增强格式: {cleaned_file}")
        
        try:
            with open(cleaned_file, 'r', encoding='utf-8') as f:
                cleaned_data = json.load(f)
        except Exception as e:
            print(f"读取清洗后的数据失败: {e}")
            return 0

        enhanced_conversations = []

        for chat in cleaned_data:
            chat_id = chat.get('chat_id', '')
            customer = chat.get('customer', {})
            source = chat.get('source', {})
            messages = chat.get('messages', [])

            # 提取客户信息
            email = customer.get('email', '').strip()
            phone = customer.get('phone', '').strip()
            
            # 提取来源信息
            referrer = source.get('referrer', '')
            geolocation = source.get('geolocation', {})
            last_pages = source.get('last_pages', [])
            
            # 提取起始URL (从thread.properties.routing.start_url)
            start_url = source.get('start_url', '')
            
            # 确定渠道
            channel = self._determine_channel(referrer)
            
            # 提取创建时间（使用第一条消息的时间）
            created_at = ''
            messages_count = len(messages)
            if messages:
                # 将第一条消息时间转换为ISO格式
                first_message_time = messages[0].get('time', '')
                created_at = self._convert_to_iso_format(first_message_time)

            # 添加Started on字段（使用HKT时间格式）
            started_on = ''
            if messages:
                started_on = messages[0].get('time', '')  # 这已经是HKT格式的时间

            # 构建增强对话记录
            enhanced_record = {
                'id': chat_id,
                'email': email,
                'phone': phone,
                'channel': channel,
                'referrer': referrer,
                'start_url': start_url,  # 添加起始URL字段
                'country': geolocation.get('country', ''),
                'region': geolocation.get('region', ''),
                'city': geolocation.get('region', ''),  # 使用region作为city
                'country_code': geolocation.get('country_code', ''),
                'created_at': created_at,
                'started_on': started_on,  # 添加Started on字段
                'messages_count': messages_count,
                'has_email': bool(email),
                'has_phone': bool(phone)
            }
            
            enhanced_conversations.append(enhanced_record)

        # 按创建时间排序（降序）
        enhanced_conversations.sort(key=lambda x: x.get('created_at', ''), reverse=True)

        try:
            with open(enhanced_file, 'w', encoding='utf-8') as f:
                json.dump(enhanced_conversations, f, ensure_ascii=False, indent=2)
            print(f"增强格式数据已保存到文件: {enhanced_file}")
            print(f"转换了 {len(enhanced_conversations)} 条记录")
            return len(enhanced_conversations)
        except Exception as e:
            print(f"保存增强格式数据失败: {e}")
            return 0

    def _format_time(self, created_at, chat_id):
        """格式化时间"""
        try:
            if created_at:
                dt_original = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                if dt_original.tzinfo is None:
                    dt_utc = dt_original.replace(tzinfo=timezone.utc)
                else:
                    dt_utc = dt_original
                dt_hkt = dt_utc.astimezone(HKT)
                return dt_hkt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                return ''
        except Exception as e:
            print(f"WARNING: Could not parse time '{created_at}' for chat ID {chat_id}: {e}", file=sys.stderr)
            return created_at

    def _convert_to_iso_format(self, time_str):
        """将时间字符串转换为ISO格式"""
        try:
            if not time_str:
                return ''
            # 假设输入格式是 'YYYY-MM-DD HH:MM:SS' (HKT)
            dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            # 将其视为HKT时间，转换为UTC
            dt_hkt = dt.replace(tzinfo=HKT)
            dt_utc = dt_hkt.astimezone(timezone.utc)
            return dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        except Exception as e:
            print(f"WARNING: Could not convert time '{time_str}' to ISO format: {e}", file=sys.stderr)
            return ''

    def _is_test_email(self, email):
        """检查是否为测试邮箱"""
        if not email:
            return False
        email = email.strip().lower()
        
        # 排除特定的测试邮箱
        if email == 'katrinayu0815@gmail.com':
            return True
            
        for domain in self.test_domains:
            if email.endswith(domain):
                return True
        if email.endswith('@qq.com') and 'test' in email:
            return True
        return False

    def _is_test_name(self, name):
        """检查是否为测试姓名"""
        if not name:
            return False
        name = name.strip().lower()
        return any(keyword in name for keyword in self.test_keywords)
    
    def _is_admin_referrer(self, referrer):
        """检查是否来自管理员页面"""
        if not referrer:
            return False
        return referrer.strip().startswith('https://vertu.com/wp-admin/')

    def _is_valid_consultation(self, customer_name, customer_email, customer_phone, referrer=''):
        """判断是否为有效咨询 - 只有有电话或邮箱的才算有效，仅有名字的不算"""
        # 如果是测试邮箱或测试姓名，直接返回False
        if self._is_test_email(customer_email) or self._is_test_name(customer_name):
            return False
        
        # 如果来源是管理员页面，直接返回False
        if self._is_admin_referrer(referrer):
            return False
        
        # 检查是否有有效的邮箱
        has_valid_email = customer_email and '@' in customer_email.strip() and not self._is_test_email(customer_email)
        # 检查是否有有效的电话（不为空即可）
        has_valid_phone = bool(customer_phone and customer_phone.strip())
        
        # 只有有邮箱或电话的才算有效咨询，仅有名字的不算
        return has_valid_email or has_valid_phone

    def _determine_channel(self, referrer):
        """根据referrer确定渠道"""
        if not referrer:
            return 'direct'
        
        referrer = referrer.lower()
        
        # 解析URL获取域名
        try:
            parsed = urlparse(referrer)
            domain = parsed.netloc.lower()
        except:
            domain = referrer
        
        # 渠道判断逻辑
        if 'google' in domain:
            if 'gclid=' in referrer or 'utm_source=google' in referrer:
                return 'google_ads'
            else:
                return 'google'
        elif 'youtube' in domain or 'youtu.be' in domain:
            return 'youtube'
        elif 'facebook' in domain or 'fb.com' in domain:
            return 'facebook'
        elif 'instagram' in domain:
            return 'instagram'
        elif 'bing' in domain:
            return 'bing'
        elif 'yandex' in domain:
            return 'yandex'
        elif 'vertu.com' in domain:
            return 'website_vertu.com'
        elif 'evernote' in domain:
            return 'website_evernote.com'
        elif 'google.co' in domain:
            return 'website_google.co.in'
        elif 'android-app://com.google.android.googlequicksearchbox' in referrer:
            return 'website_com.google.android.googlequicksearchbox'
        else:
            return 'website_' + domain.replace('www.', '') if domain else 'direct'

    def process_pipeline(self, input_file, month_name):
        """完整的数据处理管道"""
        # 生成文件名
        cleaned_file = f"cleaned_chats_{month_name}.json"
        enhanced_file = f"enhanced_valid_conversations_{month_name}.json"
        
        print("=" * 80)
        print(f"开始处理 {month_name} 的数据处理管道")
        print("=" * 80)
        
        # 第一步：清洗数据
        print("\n步骤1: 清洗原始聊天数据")
        print("-" * 50)
        original_count, cleaned_count = self.clean_chat_data(input_file, cleaned_file)
        
        if cleaned_count == 0:
            print(f"清洗步骤失败，跳过后续处理")
            return
        
        # 第二步：转换为增强格式
        print("\n步骤2: 转换为增强格式")
        print("-" * 50)
        enhanced_count = self.convert_to_enhanced_format(cleaned_file, enhanced_file)
        
        # 总结
        print("\n" + "=" * 80)
        print(f"{month_name} 数据处理完成")
        print("=" * 80)
        print(f"原始对话数: {original_count}")
        print(f"有效咨询数: {cleaned_count}")
        print(f"增强记录数: {enhanced_count}")
        if original_count > 0:
            print(f"有效率: {cleaned_count/original_count*100:.1f}%")
        print("=" * 80)

def main():
    """主函数"""
    processor = ChatDataProcessor()
    
    # 配置要处理的文件
    files_to_process = [
        {
            'input': 'chats_8月1-5.json',
            'month': 'august_1_5'
        },
        {
            'input': 'chats_9月1-5.json',
            'month': 'september_1_5'
        }
    ]
    
    for file_info in files_to_process:
        if os.path.exists(file_info['input']):
            processor.process_pipeline(file_info['input'], file_info['month'])
        else:
            print(f"文件不存在: {file_info['input']}")

if __name__ == '__main__':
    main()
