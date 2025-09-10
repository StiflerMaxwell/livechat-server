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
        - 电话号码不限制位数
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
            events = chat.get('events', [])
            created_at = chat.get('created_at', '')

            # 提取客户信息
            customer = next((user for user in users if user.get('type') == 'customer'), None)
            if not customer:
                continue
                
            customer_id = customer.get('id', '')
            customer_name = customer.get('name', '')
            customer_email = customer.get('email', '')
            customer_phone = customer.get('phone', '')
            
            # 从session_fields中提取邮箱和电话
            session_fields = customer.get('session_fields', [])
            for field in session_fields:
                if isinstance(field, dict):
                    for key, value in field.items():
                        if 'email' in key.lower() and value and not customer_email:
                            customer_email = value
                        # 更严格的电话号码字段匹配，避免产品名称被误认为电话号码
                        elif (key.lower() in ['phone', 'phone_number', 'mobile', 'mobile_number', 'telephone'] 
                              and value and not customer_phone):
                            customer_phone = value
            
            # 从events中提取表单数据（包括prechat表单）
            for event in events:
                if event.get('type') == 'form':
                    # 处理标准form_data格式
                    form_data = event.get('properties', {}).get('form_data', {})
                    if 'email' in form_data and form_data['email'] and not customer_email:
                        customer_email = form_data['email']
                    if 'phone' in form_data and form_data['phone'] and not customer_phone:
                        customer_phone = form_data['phone']
                    
                    # 处理prechat表单格式
                    if event.get('properties', {}).get('form_type') == 'prechat':
                        fields = event.get('properties', {}).get('fields', [])
                        for field in fields:
                            if isinstance(field, dict):
                                field_name = field.get('name', '')
                                field_answer = field.get('answer', '')
                                
                                # 提取Phone Number字段
                                if field_name == 'Phone Number' and field_answer and not customer_phone:
                                    customer_phone = field_answer
                                
                                # 提取Email字段
                                elif 'email' in field_name.lower() and field_answer and not customer_email:
                                    customer_email = field_answer
            
            # 提取用户来源信息
            visit_info = customer.get('visit', {})
            
            # 提取起始URL
            start_url = ''
            last_pages = visit_info.get('last_pages', [])
            if last_pages:
                start_url = last_pages[0].get('url', '')
            
            # 构建来源数据
            source_data = {
                'referrer': visit_info.get('referrer', ''),
                'start_url': start_url,
                'ip': visit_info.get('ip', ''),
                'user_agent': visit_info.get('user_agent', ''),
                'geolocation': visit_info.get('geolocation', {}),
                'visit_started_at': visit_info.get('started_at', ''),
                'visit_ended_at': visit_info.get('ended_at', ''),
                'session_fields': session_fields,
                'last_pages': last_pages
            }

            # 提取对话内容
            messages = []
            for event in events:
                event_type = event.get('type', '')
                event_created_at = event.get('created_at', '')
                author_id = event.get('author_id', '')
                text = event.get('text', '')

                # 格式化时间
                formatted_time = self._format_time(event_created_at, chat_id)

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

            # 判断是否为有效咨询（必须有邮箱或电话）
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
                    'messages': messages,
                    'created_at': created_at
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

    def convert_to_simplified_format(self, cleaned_file, output_file):
        """
        第二步：将清洗后的数据转换为简化格式，只保留需要的字段
        """
        print(f"开始转换为简化格式: {cleaned_file}")
        
        try:
            with open(cleaned_file, 'r', encoding='utf-8') as f:
                cleaned_data = json.load(f)
        except Exception as e:
            print(f"读取清洗后的数据失败: {e}")
            return 0

        simplified_records = []

        for chat in cleaned_data:
            chat_id = chat.get('chat_id', '')
            customer = chat.get('customer', {})
            source = chat.get('source', {})
            messages = chat.get('messages', [])

            # 提取客户信息
            email = customer.get('email', '').strip()
            phone = customer.get('phone', '').strip()
            name = customer.get('name', '').strip()
            
            # 提取来源信息
            referrer = source.get('referrer', '')
            start_url = source.get('start_url', '')
            geolocation = source.get('geolocation', {})
            
            # 提取创建时间（使用第一条消息的时间）
            created_at = ''
            if messages:
                created_at = messages[0].get('time', '')

            # 构建简化记录，只保留需要的字段
            simplified_record = {
                'id': chat_id,
                'name': name,
                'email': email,
                'phone': phone,
                'start_url': start_url,
                'referrer': referrer,
                'country': geolocation.get('country', ''),
                'created_at': created_at,
                'messages_count': len(messages)
            }
            
            simplified_records.append(simplified_record)

        # 按创建时间排序（降序）
        simplified_records.sort(key=lambda x: x.get('created_at', ''), reverse=True)

        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(simplified_records, f, ensure_ascii=False, indent=2)
            print(f"简化格式数据已保存到文件: {output_file}")
            print(f"转换了 {len(simplified_records)} 条记录")
            return len(simplified_records)
        except Exception as e:
            print(f"保存简化格式数据失败: {e}")
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
        """判断是否为有效咨询 - 有邮箱或电话的都算有效，电话号码不限制位数"""
        # 如果是测试邮箱或测试姓名，直接返回False
        if self._is_test_email(customer_email) or self._is_test_name(customer_name):
            return False
        
        # 如果来源是管理员页面，直接返回False
        if self._is_admin_referrer(referrer):
            return False
        
        # 检查是否有有效的邮箱
        has_valid_email = customer_email and '@' in customer_email.strip() and not self._is_test_email(customer_email)
        
        # 检查是否有有效的电话（不为空、不是URL、不限制位数）
        has_valid_phone = False
        if customer_phone and customer_phone.strip():
            phone = customer_phone.strip()
            # 排除明显不是电话的内容
            if not phone.startswith('http') and not phone.startswith('www') and phone != 'N/A':
                # 简单验证：包含数字即可，不限制位数
                if any(char.isdigit() for char in phone):
                    has_valid_phone = True
        
        # 有邮箱或电话的都算有效咨询
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
        output_file = f"filtered_conversations_{month_name}.json"
        
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
        
        # 第二步：转换为简化格式
        print("\n步骤2: 转换为简化JSON格式")
        print("-" * 50)
        output_count = self.convert_to_simplified_format(cleaned_file, output_file)
        
        # 总结
        print("\n" + "=" * 80)
        print(f"{month_name} 数据处理完成")
        print("=" * 80)
        print(f"原始对话数: {original_count}")
        print(f"有效咨询数: {cleaned_count}")
        print(f"输出记录数: {output_count}")
        if original_count > 0:
            print(f"有效率: {cleaned_count/original_count*100:.1f}%")
        print(f"输出文件: {output_file}")
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
