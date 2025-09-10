import json

def find_customer_info(filename, max_records=10):
    """查找客户信息存储的位置"""
    print(f"=== 查找客户信息: {filename} ===")
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"总记录数: {len(data)}")
        
        found_customer_data = []
        
        for i, chat in enumerate(data[:max_records]):
            chat_id = chat.get('id', f'record_{i}')
            
            # 检查用户信息
            users = chat.get('users', [])
            customer_info = {}
            
            for user in users:
                if user.get('type') == 'customer':
                    # 检查直接字段
                    name = user.get('name', '')
                    email = user.get('email', '')
                    phone = user.get('phone', '')
                    
                    if name or email or phone:
                        customer_info = {
                            'chat_id': chat_id,
                            'name': name,
                            'email': email, 
                            'phone': phone,
                            'source': 'user_direct'
                        }
                        found_customer_data.append(customer_info)
                        print(f"记录{i+1} ({chat_id}): 直接字段 - name={name}, email={email}, phone={phone}")
            
            # 检查thread events中的表单或其他来源
            thread = chat.get('thread', {})
            events = thread.get('events', [])
            
            for event in events:
                event_type = event.get('type', '')
                properties = event.get('properties', {})
                
                # 检查form事件
                if event_type == 'form':
                    form_type = properties.get('form_type', '')
                    form_data = properties.get('form_data', {})
                    fields = properties.get('fields', [])
                    
                    if form_data or fields:
                        print(f"记录{i+1} ({chat_id}): 发现表单 - type={form_type}")
                        if form_data:
                            print(f"  form_data: {form_data}")
                        if fields:
                            print(f"  fields: {fields}")
                
                # 检查文本消息中是否包含客户信息
                text = event.get('text', '')
                if text and any(keyword in text.lower() for keyword in ['phone', 'email', 'contact', '@']):
                    print(f"记录{i+1} ({chat_id}): 消息可能包含联系信息: {text[:100]}...")
        
        print(f"\n=== 总结 ===")
        print(f"找到客户信息的记录数: {len(found_customer_data)}")
        for info in found_customer_data:
            print(f"  {info['chat_id']}: {info['name']} | {info['email']} | {info['phone']}")
        
        return found_customer_data
        
    except Exception as e:
        print(f"处理文件时出错: {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == '__main__':
    # 检查两个文件
    files = ['chats_8月1-5.json', 'chats_9月1-5.json']
    
    all_customer_data = []
    
    for filename in files:
        try:
            customer_data = find_customer_info(filename, max_records=20)
            all_customer_data.extend(customer_data)
            print("\n" + "="*50 + "\n")
        except FileNotFoundError:
            print(f"文件 {filename} 不存在")
    
    print(f"总共找到 {len(all_customer_data)} 条包含客户信息的记录")
