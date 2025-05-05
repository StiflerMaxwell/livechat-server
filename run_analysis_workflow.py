# run_analysis_workflow.py
# Usage: python run_analysis_workflow.py <raw_input_json_path> <final_output_excel_path> [limit_number]

import json
import os
import sys # 导入 sys 模块
import uuid # 导入 uuid 用于生成临时文件名
from google import genai
from datetime import datetime
import time
from dotenv import load_dotenv
import pandas as pd

# --- 获取脚本所在的目录并添加到 Python 搜索路径 ---
# 这允许 Python 找到同目录下的其他脚本 (clean_chat_data.py, analyze_chats.py)
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir) # 将当前目录添加到搜索路径的开头
# -----------------------------------------------------

# --- 尝试从同目录下的脚本导入函数和变量 ---
try:
    # 导入清洗函数 (来自 clean_chat_data.py)
    # 确保 clean_chat_data.py 就在这个 run_analysis_workflow.py 文件旁边
    from clean_chat_data import clean_chat_data
    # 从 analyze_chats.py 导入核心分析函数，以及它初始化好的 client 和 MODEL_NAME
    # analyze_chats.py 在导入时会自动加载 .env 并尝试初始化 client/model
    # 确保 analyze_chats.py 就在这个 run_analysis_workflow.py 文件旁边
    from analyze_chats import run_analysis_process, client, MODEL_NAME

except ImportError as e:
    # 当作为子进程运行时，直接打印错误并退出，Node.js 会捕获 stderr
    # 使用特定的前缀 'PYTHON_FATAL_ERROR:' 让 Node.js 容易识别关键错误
    print(f"PYTHON_FATAL_ERROR: Cannot import modules. Ensure clean_chat_data.py and analyze_chats.py are in the same directory.")
    print(f"PYTHON_FATAL_ERROR_DETAIL: {e}", file=sys.stderr) # 打印到标准错误流
    sys.exit(1)
except Exception as e:
    print(f"PYTHON_FATAL_ERROR: Exception during module import or initialization.")
    print(f"PYTHON_FATAL_ERROR_DETAIL: {e}", file=sys.stderr)
    sys.exit(1)


# --- 主函数，现在接受文件路径和可选 limit 作为参数 ---
def main(raw_input_file_path, final_output_excel_path, limit_value=None):
    # 使用特定的前缀 'PYTHON_STATUS:' 打印状态信息，Node.js 可以捕获并转发到前端
    print("PYTHON_STATUS: Starting chat analysis workflow...")
    print(f"PYTHON_STATUS: Raw input file: {raw_input_file_path}")
    print(f"PYTHON_STATUS: Final output file: {final_output_excel_path}")
    if limit_value is not None:
        print(f"PYTHON_STATUS: Analysis limit: {limit_value}")
    else:
        print("PYTHON_STATUS: No limit, processing all chats.")

    # --- 定义中间文件 ---
    # 中间文件将保存在原始输入文件所在的目录
    input_dir = os.path.dirname(raw_input_file_path)
    intermediate_cleaned_filename = f"temp_cleaned_chats_intermediate_{os.path.basename(raw_input_file_path)}_{uuid.uuid4().hex}.json"
    intermediate_cleaned_file_path = os.path.join(input_dir, intermediate_cleaned_filename)

    print(f"PYTHON_STATUS: Intermediate cleaned file: {intermediate_cleaned_file_path}")

    # --- 步骤 1: 清洗原始对话数据 ---
    print("\nPYTHON_STATUS: Step 1: Cleaning raw chat data...")
    try:
        # clean_chat_data 函数本身会处理读取和保存，并在控制台打印进度
        # 我们需要确保 clean_chat_data 在错误时也使用 PYTHON_FATAL_ERROR 前缀并退出
        # **请检查 clean_chat_data.py 确保其内部错误处理也遵循这个模式**
        clean_chat_data(raw_input_file_path, intermediate_cleaned_file_path)
        print("PYTHON_STATUS: Cleaning step completed.")
    except Exception as e:
        print(f"PYTHON_FATAL_ERROR: Error during cleaning step.")
        print(f"PYTHON_FATAL_ERROR_DETAIL: {e}", file=sys.stderr)
        # 确保在错误时尝试删除可能生成的中间文件
        if os.path.exists(intermediate_cleaned_file_path):
            try:
                os.remove(intermediate_cleaned_file_path)
            except OSError:
                pass
        sys.exit(1)


    # 检查中间文件是否存在且非空
    if not os.path.exists(intermediate_cleaned_file_path) or os.path.getsize(intermediate_cleaned_file_path) == 0:
        print(f"PYTHON_FATAL_ERROR: Cleaning did not produce a valid intermediate file: {intermediate_cleaned_file_path}")
        # 尝试读取中间文件内容帮助调试
        try:
            with open(intermediate_cleaned_file_path, 'r', encoding='utf-8') as f:
                error_content = f.read(500) # 只读取前500字符
                print(f"PYTHON_ERROR_INTERMEDIATE_CONTENT:\n{error_content}...", file=sys.stderr)
        except Exception:
            pass
        sys.exit(1)


    print("\nPYTHON_STATUS: Step 2: Analyzing cleaned chat data...")

    # 调用 analyze_chats.py 中封装好的分析函数
    # run_analysis_process 会处理加载清洗后的文件，调用API，计算指标，打印进度和原始API回复，
    # 在控制台打印最终结果，并将结果保存到 Excel 文件。
    # 它还会处理 client=None 的情况。
    try:
        # analyze_chats.py 的 client 和 MODEL_NAME 已经在导入时初始化好了
        # run_analysis_process 内部会打印更详细的进度和 API 调用信息
        analyzed_results = run_analysis_process(
            cleaned_input_file=intermediate_cleaned_file_path,
            final_output_file_excel=final_output_excel_path,
            client_instance=client,      # 使用 analyze_chats.py 导入的 client
            model_name_str=MODEL_NAME, # 使用 analyze_chats.py 导入的 MODEL_NAME
            limit=limit_value,           # 使用从命令行参数解析的 limit
            print_results_to_console=True # 在子进程的控制台也打印进度和结果
        )
        print("PYTHON_STATUS: Analysis step completed.")
        # analyze_chats.run_analysis_process 已经打印了总结信息和保存信息

        # 如果分析结果为空，可能是清洗或分析过程有问题但未致命退出
        if not analyzed_results:
            print("PYTHON_WARNING: Analysis completed, but no results were generated.", file=sys.stderr)


    except Exception as e:
        print(f"PYTHON_FATAL_ERROR: Error during analysis step.")
        print(f"PYTHON_FATAL_ERROR_DETAIL: {e}", file=sys.stderr)
        # 在分析失败时尝试删除可能生成的中间文件
        if os.path.exists(intermediate_cleaned_file_path):
            try:
                os.remove(intermediate_cleaned_file_path)
            except OSError:
                pass
        import traceback
        traceback.print_exc(file=sys.stderr) # 打印完整的错误堆栈到 stderr
        sys.exit(1) # 分析失败，退出子进程

    finally:
        # --- 清理中间文件 ---
        # 无论分析成功或失败，都尝试删除中间文件
        if os.path.exists(intermediate_cleaned_file_path):
            print(f"PYTHON_STATUS: Attempting to clean up intermediate file: {intermediate_cleaned_file_path}")
            try:
                os.remove(intermediate_cleaned_file_path)
                print("PYTHON_STATUS: Intermediate file removed.")
            except OSError as e:
                print(f"PYTHON_WARNING: Could not remove intermediate file {intermediate_cleaned_file_path}: {e}", file=sys.stderr)


    print("PYTHON_STATUS: Chat analysis workflow finished.")
    sys.exit(0) # 成功完成，退出子进程


# --- 当脚本作为主程序运行 (即被 Node.js 的 child_process 调用) ---
if __name__ == "__main__":
    # sys.argv 包含命令行参数列表
    # sys.argv[0] 是脚本名本身
    # sys.argv[1] 应该是原始输入文件路径
    # sys.argv[2] 应该是最终输出 Excel 文件路径
    # sys.argv[3] (可选) 应该是处理数量限制 (数字字符串)

    if len(sys.argv) < 3:
        print("PYTHON_FATAL_ERROR: Missing command line arguments.")
        print("PYTHON_FATAL_ERROR: Usage: python run_analysis_workflow.py <raw_input_json_path> <final_output_excel_path> [limit]")
        sys.exit(1)

    raw_input_path = sys.argv[1]
    final_output_path = sys.argv[2]
    limit = None # Default to no limit

    if len(sys.argv) > 3:
        try:
            # Parse limit argument
            limit_str = sys.argv[3]
            limit = int(limit_str)
            if limit < 0:
                 limit = None
                 print("PYTHON_WARNING: Negative limit provided via command line. Processing all chats.", file=sys.stderr)
        except ValueError:
            print("PYTHON_WARNING: Invalid limit provided via command line. Processing all chats.", file=sys.stderr)
            limit = None

    # 调用主函数执行工作流
    main(raw_input_path, final_output_path, limit)