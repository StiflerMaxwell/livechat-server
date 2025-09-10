import json

def find_phone_fields(filename, max_records=5):
    """查找原始数据中可能包含电话号码的字段"""
    print(f"=== 分析文件: {filename} ===")
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"总记录数: {len(data)}")
        
        phone_patterns = []
        form_structures = []
        
        for i, chat in enumerate(data[:max_records]):
            print(f"\n--- 记录 {i+1} ---")
            
            # 检查用户信息中的phone字段
            users = chat.get('users', [])
            for user in users:
                if user.get('type') == 'customer':
                    phone = user.get('phone', '')
                    if phone:
                        print(f"用户phone字段: {phone}")
                        phone_patterns.append(phone)
            
            # 检查thread events中的表单数据
            thread = chat.get('thread', {})
            events = thread.get('events', [])
            
            for event in events:
                event_type = event.get('type', '')
                
                # 查找form类型的事件
                if event_type == 'form':
                    properties = event.get('properties', {})
                    form_type = properties.get('form_type', '')
                    
                    print(f"发现表单事件: form_type={form_type}")
                    
                    # 检查form_data
                    form_data = properties.get('form_data', {})
                    if form_data:
                        print(f"  form_data: {form_data}")
                        if 'phone' in form_data:
                            phone_patterns.append(form_data['phone'])
                    
                    # 检查fields数组
                    fields = properties.get('fields', [])
                    if fields:
                        print(f"  fields数量: {len(fields)}")
                        for field in fields:
                            if isinstance(field, dict):
                                name = field.get('name', '')
                                answer = field.get('answer', '')
                                print(f"    字段: {name} = {answer}")
                                
                                # 检查可能的电话字段名
                                if any(keyword in name.lower() for keyword in ['phone', 'tel', 'mobile', 'contact']):
                                    if answer:
                                        print(f"    *** 可能的电话字段: {name} = {answer}")
                                        phone_patterns.append(answer)
                                        
                        form_structures.append({
                            'form_type': form_type,
                            'fields': [f.get('name', '') for f in fields if isinstance(f, dict)]
                        })
        
        print(f"\n=== 总结 ===")
        print(f"发现的电话号码: {phone_patterns}")
        print(f"表单结构: {form_structures}")
        
        return phone_patterns, form_structures
        
    except Exception as e:
        print(f"处理文件时出错: {e}")
        return [], []

if __name__ == '__main__':
    # 检查两个文件
    files = ['chats_8月1-5.json', 'chats_9月1-5.json']
    
    for filename in files:
        try:
            find_phone_fields(filename, max_records=3)
            print("\n" + "="*50 + "\n")
        except FileNotFoundError:
            print(f"文件 {filename} 不存在")
