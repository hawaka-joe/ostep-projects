#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>

#define RECORD_SIZE 100
#define KEY_SIZE 4

typedef struct {
    unsigned char *start;
    int num_records;
    int thread_id;
} SortArgs;

// 比较函数：比较两条记录的前4字节key
static int compare_records(const void *a, const void *b) {
    const unsigned char *rec_a = (const unsigned char *)a;
    const unsigned char *rec_b = (const unsigned char *)b;
    
    // 比较前4字节（key）
    for (int i = 0; i < KEY_SIZE; i++) {
        if (rec_a[i] != rec_b[i]) {
            return rec_a[i] - rec_b[i];
        }
    }
    return 0;
}

// 线程排序函数：对指定范围的记录进行排序
void *sort_chunk(void *arg) {
    SortArgs *args = (SortArgs *)arg;
    qsort(args->start, args->num_records, RECORD_SIZE, compare_records);
    return NULL;
}

// 归并两个已排序的块
void merge_chunks(unsigned char *chunk1, int num1, 
                  unsigned char *chunk2, int num2, 
                  unsigned char *output) {
    int i = 0, j = 0, k = 0;
    
    while (i < num1 && j < num2) {
        if (compare_records(chunk1 + i * RECORD_SIZE, 
                           chunk2 + j * RECORD_SIZE) <= 0) {
            memcpy(output + k * RECORD_SIZE, 
                   chunk1 + i * RECORD_SIZE, RECORD_SIZE);
            i++;
        } else {
            memcpy(output + k * RECORD_SIZE, 
                   chunk2 + j * RECORD_SIZE, RECORD_SIZE);
            j++;
        }
        k++;
    }
    
    // 复制剩余记录
    while (i < num1) {
        memcpy(output + k * RECORD_SIZE, 
               chunk1 + i * RECORD_SIZE, RECORD_SIZE);
        i++;
        k++;
    }
    while (j < num2) {
        memcpy(output + k * RECORD_SIZE, 
               chunk2 + j * RECORD_SIZE, RECORD_SIZE);
        j++;
        k++;
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "用法: %s input output\n", argv[0]);
        exit(1);
    }
    
    const char *input_file = argv[1];
    const char *output_file = argv[2];
    
    // 打开输入文件
    int fd_in = open(input_file, O_RDONLY);
    if (fd_in < 0) {
        perror("无法打开输入文件");
        exit(1);
    }
    
    // 获取文件大小
    struct stat st;
    if (fstat(fd_in, &st) < 0) {
        perror("无法获取文件大小");
        close(fd_in);
        exit(1);
    }
    
    int file_size = st.st_size;
    int num_records = file_size / RECORD_SIZE;
    
    if (file_size % RECORD_SIZE != 0) {
        fprintf(stderr, "警告: 文件大小不是100字节的倍数\n");
    }
    
    // 使用mmap映射输入文件（MAP_PRIVATE确保修改不会写回文件）
    unsigned char *input_data = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd_in, 0);
    if (input_data == MAP_FAILED) {
        perror("mmap失败");
        close(fd_in);
        exit(1);
    }
    close(fd_in);
    
    // 获取CPU核心数
    int num_threads = sysconf(_SC_NPROCESSORS_ONLN);
    if (num_threads < 1) num_threads = 1;
    if (num_threads > num_records) num_threads = num_records;
    
    // 分配输出缓冲区
    unsigned char *output_data = malloc(file_size);
    if (!output_data) {
        perror("内存分配失败");
        munmap(input_data, file_size);
        exit(1);
    }
    
    // 分配临时缓冲区用于归并
    unsigned char *temp_buffer = malloc(file_size);
    if (!temp_buffer) {
        perror("内存分配失败");
        free(output_data);
        munmap(input_data, file_size);
        exit(1);
    }
    
    // 创建线程和参数数组
    pthread_t *threads = malloc(num_threads * sizeof(pthread_t));
    SortArgs *args = malloc(num_threads * sizeof(SortArgs));
    
    if (!threads || !args) {
        perror("内存分配失败");
        free(output_data);
        free(temp_buffer);
        munmap(input_data, file_size);
        exit(1);
    }
    
    // 计算每个线程处理的记录数
    int records_per_thread = num_records / num_threads;
    int remainder = num_records % num_threads;
    
    // 创建线程对各个块进行排序
    int offset = 0;
    for (int i = 0; i < num_threads; i++) {
        int chunk_size = records_per_thread + (i < remainder ? 1 : 0);
        args[i].start = input_data + offset * RECORD_SIZE;
        args[i].num_records = chunk_size;
        args[i].thread_id = i;
        
        pthread_create(&threads[i], NULL, sort_chunk, &args[i]);
        offset += chunk_size;
    }
    
    // 等待所有线程完成
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // 归并所有已排序的块
    // 先将第一个块复制到输出缓冲区
    int first_chunk_size = records_per_thread + (0 < remainder ? 1 : 0);
    memcpy(output_data, input_data, first_chunk_size * RECORD_SIZE);
    int merged_size = first_chunk_size;
    
    // 依次归并后续块
    offset = first_chunk_size;
    for (int i = 1; i < num_threads; i++) {
        int chunk_size = records_per_thread + (i < remainder ? 1 : 0);
        unsigned char *chunk_start = input_data + offset * RECORD_SIZE;
        
        // 使用临时缓冲区进行归并
        merge_chunks(output_data, merged_size, 
                    chunk_start, chunk_size, 
                    temp_buffer);
        
        // 将归并结果复制回output_data
        memcpy(output_data, temp_buffer, (merged_size + chunk_size) * RECORD_SIZE);
        
        merged_size += chunk_size;
        offset += chunk_size;
    }
    
    // 写入输出文件
    FILE *fp_out = fopen(output_file, "wb");
    if (!fp_out) {
        perror("无法打开输出文件");
        free(output_data);
        free(temp_buffer);
        free(threads);
        free(args);
        munmap(input_data, file_size);
        exit(1);
    }
    
    if (fwrite(output_data, RECORD_SIZE, num_records, fp_out) != num_records) {
        perror("写入文件失败");
        fclose(fp_out);
        free(output_data);
        free(temp_buffer);
        free(threads);
        free(args);
        munmap(input_data, file_size);
        exit(1);
    }
    
    // 强制同步到磁盘
    int fd_out = fileno(fp_out);
    if (fsync(fd_out) < 0) {
        perror("fsync失败");
    }
    
    fclose(fp_out);
    
    // 清理资源
    free(output_data);
    free(temp_buffer);
    free(threads);
    free(args);
    munmap(input_data, file_size);
    
    return 0;
}

