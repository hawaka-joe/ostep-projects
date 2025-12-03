#!/usr/bin/env python3
"""
生成符合 psort 要求的测试输入文件
每条记录：100字节（前4字节是key，后96字节是数据）
"""

import sys
import random
import struct

def generate_test_file(filename, num_records=100):
    """
    生成测试输入文件
    
    Args:
        filename: 输出文件名
        num_records: 要生成的记录数量
    """
    with open(filename, 'wb') as f:
        for i in range(num_records):
            # 生成一个随机的4字节key（可以是整数或字符串）
            # 这里使用随机整数作为key，便于测试排序
            key = random.randint(0, 0xFFFFFFFF)
            
            # 将key写入前4字节（使用大端序，也可以用小端序）
            key_bytes = struct.pack('>I', key)  # '>I' 表示大端序的4字节无符号整数
            
            # 生成96字节的随机数据
            data = bytes(random.randint(0, 255) for _ in range(96))
            
            # 写入完整的100字节记录
            f.write(key_bytes + data)
    
    print(f"已生成测试文件: {filename}")
    print(f"记录数量: {num_records}")
    print(f"文件大小: {num_records * 100} 字节")

def generate_sorted_test_file(filename, num_records=100):
    """
    生成一个已经排序的测试文件（用于验证排序是否正确）
    """
    with open(filename, 'wb') as f:
        for i in range(num_records):
            # 使用递增的key值
            key = i
            
            key_bytes = struct.pack('>I', key)
            data = bytes(random.randint(0, 255) for _ in range(96))
            
            f.write(key_bytes + data)
    
    print(f"已生成已排序的测试文件: {filename}")

def generate_reverse_sorted_test_file(filename, num_records=100):
    """
    生成一个逆序的测试文件
    """
    with open(filename, 'wb') as f:
        for i in range(num_records - 1, -1, -1):
            key = i
            
            key_bytes = struct.pack('>I', key)
            data = bytes(random.randint(0, 255) for _ in range(96))
            
            f.write(key_bytes + data)
    
    print(f"已生成逆序测试文件: {filename}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法:")
        print("  python3 generate_input.py <输出文件名> [记录数量] [类型]")
        print("  类型: random(默认), sorted, reverse")
        print("\n示例:")
        print("  python3 generate_input.py input.dat 1000")
        print("  python3 generate_input.py input_sorted.dat 1000 sorted")
        print("  python3 generate_input.py input_reverse.dat 1000 reverse")
        sys.exit(1)
    
    filename = sys.argv[1]
    num_records = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    file_type = sys.argv[3] if len(sys.argv) > 3 else 'random'
    
    if file_type == 'sorted':
        generate_sorted_test_file(filename, num_records)
    elif file_type == 'reverse':
        generate_reverse_sorted_test_file(filename, num_records)
    else:
        generate_test_file(filename, num_records)

