const http = require('http');
const { URL } = require('url');

// 配置参数
const TARGET_URL = 'http://localhost:3000/spin.cgi?1';
const NUM_REQUESTS = 40; // 并行请求数量，可以根据需要修改

// 发起单个请求的函数
function makeRequest(requestId) {
  return new Promise((resolve, reject) => {
    const startTime = Date.now();
    
    const url = new URL(TARGET_URL);
    const options = {
      hostname: url.hostname,
      port: url.port,
      path: url.pathname + url.search,
      method: 'GET'
    };

    const req = http.request(options, (res) => {
      let data = '';
      
      res.on('data', (chunk) => {
        data += chunk;
      });
      
      res.on('end', () => {
        const endTime = Date.now();
        const duration = endTime - startTime;
        
        resolve({
          requestId,
          statusCode: res.statusCode,
          headers: res.headers,
          body: data,
          duration: duration
        });
      });
    });

    req.on('error', (error) => {
      const endTime = Date.now();
      const duration = endTime - startTime;
      
      reject({
        requestId,
        error: error.message,
        duration: duration
      });
    });

    req.end();
  });
}

// 主函数
async function main() {
  console.log(`开始发起 ${NUM_REQUESTS} 个并行请求到: ${TARGET_URL}\n`);
  
  const overallStartTime = Date.now();
  
  // 创建所有请求的 Promise 数组
  const requests = Array.from({ length: NUM_REQUESTS }, (_, i) => 
    makeRequest(i + 1)
  );
  
  try {
    // 等待所有请求完成（使用 allSettled 以便即使部分失败也能继续）
    const settledResults = await Promise.allSettled(requests);
    
    const overallEndTime = Date.now();
    const totalDuration = overallEndTime - overallStartTime;
    
    // 处理结果
    const results = settledResults.map((settled, index) => {
      if (settled.status === 'fulfilled') {
        return settled.value;
      } else {
        return {
          requestId: index + 1,
          error: settled.reason.error || settled.reason.message || 'Unknown error',
          duration: settled.reason.duration || 0
        };
      }
    });
    
    // 打印每个请求的响应
    console.log('='.repeat(80));
    console.log('每个请求的响应详情:');
    console.log('='.repeat(80));
    
    results.forEach((result) => {
      console.log(`\n请求 #${result.requestId}:`);
      if (result.error) {
        console.log(`  错误: ${result.error}`);
        console.log(`  耗时: ${result.duration}ms`);
      } else {
        console.log(`  状态码: ${result.statusCode}`);
        console.log(`  耗时: ${result.duration}ms`);
        console.log(`  响应头:`, JSON.stringify(result.headers, null, 2));
        console.log(`  响应体:`);
        console.log(result.body);
      }
      console.log('-'.repeat(80));
    });
    
    // 打印总体统计
    const successfulResults = results.filter(r => !r.error);
    console.log('\n' + '='.repeat(80));
    console.log('总体统计:');
    console.log('='.repeat(80));
    console.log(`总请求数: ${NUM_REQUESTS}`);
    console.log(`成功请求数: ${successfulResults.length}`);
    console.log(`失败请求数: ${results.length - successfulResults.length}`);
    console.log(`总耗时: ${totalDuration}ms (${(totalDuration / 1000).toFixed(2)}秒)`);
    
    if (successfulResults.length > 0) {
      console.log(`平均每个请求耗时: ${(successfulResults.reduce((sum, r) => sum + r.duration, 0) / successfulResults.length).toFixed(2)}ms`);
      console.log(`最快请求: ${Math.min(...successfulResults.map(r => r.duration))}ms`);
      console.log(`最慢请求: ${Math.max(...successfulResults.map(r => r.duration))}ms`);
    }
    
  } catch (error) {
    console.error('发生未预期的错误:', error);
  }
}

// 运行主函数
main().catch(console.error);

