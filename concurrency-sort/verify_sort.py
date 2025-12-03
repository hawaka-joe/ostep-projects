#!/usr/bin/env python3
"""
验证排序结果是否正确
"""
import sys
import struct

def verify_sort(filename):
    """验证文件中的记录是否按key排序"""
    with open(filename, 'rb') as f:
        prev_key = None
        record_num = 0
        
        while True:
            record = f.read(100)
            if len(record) < 100:
                break
            
            # 读取前4字节作为key（大端序）
            key_bytes = record[:4]
            key = struct.unpack('>I', key_bytes)[0]
            
            if prev_key is not None and key < prev_key:
                print(f"错误: 记录 {record_num} 的key ({key}) 小于前一条记录的key ({prev_key})")
                return False
            
            prev_key = key
            record_num += 1
        
        print(f"验证通过: {record_num} 条记录已正确排序")
        return True

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("用法: python3 verify_sort.py <排序后的文件>")
        sys.exit(1)
    
    if verify_sort(sys.argv[1]):
        sys.exit(0)
    else:
        sys.exit(1)

