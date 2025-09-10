import json

def analyze_full_structure(filename, max_records=2):
    """完整分析数据结构"""
    print(f"=== 完整分析文件: {filename} ===")
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"总记录数: {len(data)}")
        
        for i, chat in enumerate(data[:max_records]):
            print(f"\n--- 完整记录 {i+1} 结构 ---")
            print(f"Chat ID: {chat.get('id', 'N/A')}")
            
            # 分析用户结构
            users = chat.get('users', [])
            print(f"用户数量: {len(users)}")
            
            for j, user in enumerate(users):
                print(f"  用户{j+1}:")
                print(f"    类型: {user.get('type', 'N/A')}")
                print(f"    姓名: {user.get('name', 'N/A')}")
                print(f"    邮箱: {user.get('email', 'N/A')}")
                print(f"    电话: {user.get('phone', 'N/A')}")
                
                # 检查session_fields
                session_fields = user.get('session_fields', [])
                if session_fields:
                    print(f"    session_fields: {session_fields}")
                
                # 检查visit信息
                visit = user.get('visit', {})
                if visit:
                    print(f"    visit keys: {list(visit.keys())}")
            
            # 分析thread结构
            thread = chat.get('thread', {})
            events = thread.get('events', [])
            print(f"事件数量: {len(events)}")
            
            # 查看前几个事件的结构
            for k, event in enumerate(events[:3]):
                print(f"  事件{k+1}:")
                print(f"    类型: {event.get('type', 'N/A')}")
                print(f"    作者ID: {event.get('author_id', 'N/A')}")
                
                # 检查properties
                properties = event.get('properties', {})
                if properties:
                    print(f"    properties keys: {list(properties.keys())}")
                    
                    # 特别检查form相关的properties
                    if 'form_type' in properties:
                        print(f"    form_type: {properties['form_type']}")
                    if 'form_data' in properties:
                        print(f"    form_data: {properties['form_data']}")
                    if 'fields' in properties:
                        fields = properties['fields']
                        print(f"    fields数量: {len(fields) if isinstance(fields, list) else 'N/A'}")
                        if isinstance(fields, list) and fields:
                            for field in fields[:2]:  # 只显示前2个字段
                                if isinstance(field, dict):
                                    print(f"      字段: {field.get('name', 'N/A')} = {field.get('answer', 'N/A')}")
                
                # 检查文本内容
                text = event.get('text', '')
                if text:
                    print(f"    文本: {text[:50]}...")
                    
                print()
        
    except Exception as e:
        print(f"处理文件时出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    # 先分析一个文件
    analyze_full_structure('chats_9月1-5.json', max_records=1)
