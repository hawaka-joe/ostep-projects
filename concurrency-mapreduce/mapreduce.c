#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "mapreduce.h"

// Key-Value 对结构
typedef struct {
    char *key;
    char *value;
} KVPair;

// 分区数据结构
typedef struct {
    KVPair *pairs;          // KV 对数组
    int count;              // 当前数量
    int capacity;           // 容量
    pthread_mutex_t lock;   // 互斥锁
} Partition;

// 全局状态
static Partition *partitions = NULL;
static int num_partitions = 0;
static Partitioner partition_func = NULL;

// Getter 函数的状态（每个线程独立）
typedef struct {
    char *current_key;
    int partition_num;
    int current_index;
} GetterState;

// 线程本地存储的 Getter 状态
static __thread GetterState getter_state = {NULL, -1, 0};

// 比较函数：用于排序 KV 对
static int compare_kv_pairs(const void *a, const void *b) {
    const KVPair *pair_a = (const KVPair *)a;
    const KVPair *pair_b = (const KVPair *)b;
    return strcmp(pair_a->key, pair_b->key);
}

// 默认哈希分区函数
unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

// 扩展分区容量
static void expand_partition(Partition *part) {
    if (part->count >= part->capacity) {
        int new_capacity = part->capacity == 0 ? 16 : part->capacity * 2;
        KVPair *new_pairs = realloc(part->pairs, new_capacity * sizeof(KVPair));
        if (!new_pairs) {
            perror("realloc failed");
            exit(1);
        }
        part->pairs = new_pairs;
        part->capacity = new_capacity;
    }
}

// MR_Emit: 线程安全地存储 key/value 对
void MR_Emit(char *key, char *value) {
    if (!partitions || !partition_func) {
        fprintf(stderr, "MR_Emit called before MR_Run\n");
        return;
    }
    
    // 确定分区
    unsigned long partition_num = partition_func(key, num_partitions);
    Partition *part = &partitions[partition_num];
    
    // 加锁保护
    pthread_mutex_lock(&part->lock);
    
    // 扩展容量（如果需要）
    expand_partition(part);
    
    // 复制 key 和 value
    KVPair *pair = &part->pairs[part->count];
    pair->key = strdup(key);
    pair->value = strdup(value);
    
    if (!pair->key || !pair->value) {
        perror("strdup failed");
        exit(1);
    }
    
    part->count++;
    
    pthread_mutex_unlock(&part->lock);
}

// Getter 函数：供 Reduce 函数迭代获取值
static char *get_next_value(char *key, int partition_number) {
    Partition *part = &partitions[partition_number];
    
    // 如果 key 改变或分区改变，重置索引
    // 使用字符串内容比较，而不是指针比较
    if (getter_state.current_key == NULL || 
        getter_state.partition_num != partition_number ||
        strcmp(getter_state.current_key, key) != 0) {
        getter_state.current_key = key;
        getter_state.partition_num = partition_number;
        getter_state.current_index = 0;
    }
    
    // 由于分区已排序，同一个 key 的所有值应该是连续的
    // 查找匹配的 key
    while (getter_state.current_index < part->count) {
        KVPair *pair = &part->pairs[getter_state.current_index];
        if (strcmp(pair->key, key) == 0) {
            char *value = pair->value;
            getter_state.current_index++;
            return value;
        }
        // 如果已经过了这个 key 的范围（因为已排序），停止查找
        if (strcmp(pair->key, key) > 0) {
            break;
        }
        getter_state.current_index++;
    }
    
    return NULL;
}

// Mapper 线程参数
typedef struct {
    char **files;
    int num_files;
    int *next_file_index;
    pthread_mutex_t *file_lock;
    Mapper map_func;
} MapperArgs;

// Mapper 线程函数  
static void *mapper_thread(void *arg) {
    MapperArgs *args = (MapperArgs *)arg;
    
    while (1) {
        int file_index = -1;
        
        // 获取下一个文件
        pthread_mutex_lock(args->file_lock);
        if (*args->next_file_index < args->num_files) {
            file_index = (*args->next_file_index)++;
        }
        pthread_mutex_unlock(args->file_lock);
        
        if (file_index == -1) {
            break;  // 没有更多文件
        }
        
        // 调用用户的 Map 函数
        args->map_func(args->files[file_index]);
    }
    
    return NULL;
}

// Reducer 线程参数
typedef struct {
    int partition_num;
    Reducer reduce_func;
} ReducerArgs;

// Reducer 线程函数
static void *reducer_thread(void *arg) {
    ReducerArgs *args = (ReducerArgs *)arg;
    Partition *part = &partitions[args->partition_num];
    
    // 遍历分区中所有唯一的 key
    char *last_key = NULL;
    for (int i = 0; i < part->count; i++) {
        char *current_key = part->pairs[i].key;
        
        // 如果是新的 key，调用 Reduce
        if (!last_key || strcmp(current_key, last_key) != 0) {
            // 重置 getter 状态
            getter_state.current_key = NULL;
            getter_state.partition_num = -1;
            getter_state.current_index = 0;
            
            // 调用用户的 Reduce 函数
            args->reduce_func(current_key, get_next_value, args->partition_num);
            last_key = current_key;
        }
    }
    
    return NULL;
}

// MR_Run: 主函数
void MR_Run(int argc, char *argv[], 
            Mapper map, int num_mappers, 
            Reducer reduce, int num_reducers, 
            Partitioner partition) {
    
    if (argc < 2) {
        fprintf(stderr, "Usage: %s file1 file2 ...\n", argv[0]);
        return;
    }
    
    // 保存分区函数
    partition_func = partition;
    num_partitions = num_reducers;
    
    // 初始化分区
    partitions = malloc(num_partitions * sizeof(Partition));
    if (!partitions) {
        perror("malloc failed");
        exit(1);
    }
    
    for (int i = 0; i < num_partitions; i++) {
        partitions[i].pairs = NULL;
        partitions[i].count = 0;
        partitions[i].capacity = 0;
        if (pthread_mutex_init(&partitions[i].lock, NULL) != 0) {
            perror("pthread_mutex_init failed");
            exit(1);
        }
    }
    
    // 准备文件列表
    int num_files = argc - 1;
    char **files = &argv[1];
    
    // ========== Map 阶段 ==========
    pthread_t *mapper_threads = malloc(num_mappers * sizeof(pthread_t));
    if (!mapper_threads) {
        perror("malloc failed");
        exit(1);
    }
    
    int next_file_index = 0;
    pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER;
    
    MapperArgs mapper_args = {
        .files = files,
        .num_files = num_files,
        .next_file_index = &next_file_index,
        .file_lock = &file_lock,
        .map_func = map
    };
    
    // 创建 mapper 线程
    for (int i = 0; i < num_mappers; i++) {
        if (pthread_create(&mapper_threads[i], NULL, mapper_thread, &mapper_args) != 0) {
            perror("pthread_create failed");
            exit(1);
        }
    }
    
    // 等待所有 mapper 完成
    for (int i = 0; i < num_mappers; i++) {
        pthread_join(mapper_threads[i], NULL);
    }
    
    free(mapper_threads);
    pthread_mutex_destroy(&file_lock);
    
    // ========== 排序阶段 ==========
    for (int i = 0; i < num_partitions; i++) {
        Partition *part = &partitions[i];
        if (part->count > 0) {
            qsort(part->pairs, part->count, sizeof(KVPair), compare_kv_pairs);
        }
    }
    
    // ========== Reduce 阶段 ==========
    pthread_t *reducer_threads = malloc(num_reducers * sizeof(pthread_t));
    if (!reducer_threads) {
        perror("malloc failed");
        exit(1);
    }
    
    ReducerArgs *reducer_args = malloc(num_reducers * sizeof(ReducerArgs));
    if (!reducer_args) {
        perror("malloc failed");
        exit(1);
    }
    
    // 创建 reducer 线程
    for (int i = 0; i < num_reducers; i++) {
        reducer_args[i].partition_num = i;
        reducer_args[i].reduce_func = reduce;
        
        if (pthread_create(&reducer_threads[i], NULL, reducer_thread, &reducer_args[i]) != 0) {
            perror("pthread_create failed");
            exit(1);
        }
    }
    
    // 等待所有 reducer 完成
    for (int i = 0; i < num_reducers; i++) {
        pthread_join(reducer_threads[i], NULL);
    }
    
    free(reducer_threads);
    free(reducer_args);
    
    // ========== 清理阶段 ==========
    for (int i = 0; i < num_partitions; i++) {
        Partition *part = &partitions[i];
        for (int j = 0; j < part->count; j++) {
            free(part->pairs[j].key);
            free(part->pairs[j].value);
        }
        free(part->pairs);
        pthread_mutex_destroy(&part->lock);
    }
    
    free(partitions);
    partitions = NULL;
    num_partitions = 0;
    partition_func = NULL;
}

