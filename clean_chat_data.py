import json
import re
from datetime import datetime

def clean_chat_data(input_file, output_file):
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
        return
    except json.JSONDecodeError:
        print(f"错误：无法解析文件 {input_file}，请检查文件格式是否为有效的JSON。")
        return
    except Exception as e:
        print(f"读取文件时发生未知错误: {e}")
        return

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

        # 提取对话内容
        messages = []
        for event in events:
            event_type = event.get('type', '')
            created_at = event.get('created_at', '')
            author_id = event.get('author_id', '')
            text = event.get('text', '')

            # 格式化时间
            try:
                # 兼容 'Z' 和非 'Z' 的ISO格式，并处理可能的None或空字符串
                if created_at:
                    dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    formatted_time = '' # 或者其他默认值
            except (ValueError, TypeError): # 捕获ValueError和TypeError
                 # print(f"警告: 对话ID {chat_id} 中的时间格式 '{created_at}' 无效，使用原始字符串。") # 可以选择打印警告
                 formatted_time = created_at
            except Exception as e:
                 # print(f"警告: 对话ID {chat_id} 中处理时间 '{created_at}' 时发生错误: {e}，使用原始字符串。") # 可以选择打印警告
                 formatted_time = created_at


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

        # === 修改点在这里 ===
        # 如果对话有实际内容 且 包含至少一条客户发送的消息，则添加到清洗后的数据中
        # 原来的条件是： if messages:
        # 修改后的条件是： messages 非空 且 存在任意一条消息的 sender 是 'Customer'
        if messages and any(msg['sender'] == 'Customer' for msg in messages):
            cleaned_chat = {
                'chat_id': chat_id,
                'customer': {
                    'name': customer_name,
                    'email': customer_email,
                    'phone': customer_phone
                },
                'messages': messages
            }
            cleaned_chats.append(cleaned_chat)
        # ==================

    # 打印清洗后保留的对话数量
    cleaned_chat_count = len(cleaned_chats)
    print(f"清洗后保留了 {cleaned_chat_count} 条包含有效客户消息的对话记录。")

    # 保存清洗后的数据
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cleaned_chats, f, ensure_ascii=False, indent=2)
        print(f"清洗后的数据已保存到文件: {output_file}")
    except IOError as e:
        print(f"写入文件 {output_file} 时发生IO错误: {e}")
    except Exception as e:
        print(f"写入文件时发生未知错误: {e}")


if __name__ == '__main__':
    input_file = 'chats20250430.json'  # 输入文件路径
    output_file = 'cleaned_chats_customer_only.json'  # 输出文件路径，建议改个名字区分
    clean_chat_data(input_file, output_file)