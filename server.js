'use strict';
/*
 * llm-auto-router
 * 一个 OpenAI 兼容的自动路由/故障切换代理：
 *  - 客户端把 base_url 指向本服务，model 填 "auto"（或列表中的某个模型名）
 *  - 本服务按优先级顺序把请求发给上游（默认用户的 New API 网关）
 *  - 遇到 429 / 5xx / 超时 / 网络错误，立刻换下一个模型重试
 *  - 400/422 视为请求本身的问题，原样返回，不切换
 *  - 熔断：同一模型连续失败 FAIL_THRESHOLD 次 -> 关禁闭 BENCH_SECONDS 秒；
 *    禁闭到期后下一次请求拿它试探，成功即恢复，失败重新禁闭
 *  - 401/403 视为该模型的密钥/权限问题，立即长禁闭 AUTH_BENCH_SECONDS 秒
 *  - 流式响应：在收到第一个数据块之前都可以切换；一旦开始向客户端输出就锁定该模型
 *  - 本服务不保存任何密钥：客户端的 Authorization 头原样透传给上游
 *
 * 环境变量：
 *  PORT                  监听端口（默认 3000，平台会注入）
 *  UPSTREAM_BASE         上游 base_url（默认 https://iwanmei.kdns.fr/v1）
 *  MODELS                模型优先级列表，逗号分隔
 *  FAIL_THRESHOLD        连续失败几次触发熔断（默认 3）
 *  BENCH_SECONDS         熔断禁闭时长秒（默认 30）
 *  AUTH_BENCH_SECONDS    401/403 禁闭时长秒（默认 300）
 *  FIRST_TOKEN_TIMEOUT_MS 流式首块超时毫秒（默认 20000）
 *  ATTEMPT_TIMEOUT_MS    单次尝试总超时毫秒（默认 90000）
 */
const http = require('http');
const https = require('https');
const { URL } = require('url');

const PORT = parseInt(process.env.PORT || '3000', 10);
const UPSTREAM_BASE = (process.env.UPSTREAM_BASE || 'https://iwanmei.kdns.fr/v1').replace(/\/+$/, '');
const MODELS = (process.env.MODELS || 'k-exaone_v2.5,Atria-Dawn-Preview,minimax-m3,glm-5.3,sensenova-6.8-flash,glm-5.2,deepseek-v4-flash')
  .split(',').map((s) => s.trim()).filter(Boolean);
const FAIL_THRESHOLD = parseInt(process.env.FAIL_THRESHOLD || '3', 10);
const BENCH_SECONDS = parseInt(process.env.BENCH_SECONDS || '30', 10);
const AUTH_BENCH_SECONDS = parseInt(process.env.AUTH_BENCH_SECONDS || '300', 10);
const FIRST_TOKEN_TIMEOUT_MS = parseInt(process.env.FIRST_TOKEN_TIMEOUT_MS || '20000', 10);
const ATTEMPT_TIMEOUT_MS = parseInt(process.env.ATTEMPT_TIMEOUT_MS || '90000', 10);
const MAX_BODY_BYTES = 16 * 1024 * 1024;

const upstreamUrl = new URL(UPSTREAM_BASE + '/chat/completions');
const transport = upstreamUrl.protocol === 'https:' ? https : http;

// ---------- 熔断状态 ----------
const breakers = new Map(); // model -> { fails, benchedUntil }
function breakerOf(model) {
  if (!breakers.has(model)) breakers.set(model, { fails: 0, benchedUntil: 0 });
  return breakers.get(model);
}
function isBenched(model) {
  return breakerOf(model).benchedUntil > Date.now();
}
function recordSuccess(model) {
  const b = breakerOf(model);
  b.fails = 0;
  b.benchedUntil = 0;
}
function recordFailure(model, kind) {
  const b = breakerOf(model);
  b.fails += 1;
  if (kind === 'auth') {
    b.benchedUntil = Date.now() + AUTH_BENCH_SECONDS * 1000;
  } else if (b.fails >= FAIL_THRESHOLD) {
    b.benchedUntil = Date.now() + BENCH_SECONDS * 1000;
  }
}

// ---------- 最近尝试记录（供 /status 查看） ----------
const recentAttempts = [];
function logAttempt(model, status, ms, note) {
  const entry = { t: new Date().toISOString(), model, status, ms, note: note || '' };
  recentAttempts.push(entry);
  if (recentAttempts.length > 50) recentAttempts.shift();
  console.log(`[router] ${entry.t} model=${model} status=${status} ${ms}ms ${entry.note}`);
}

// 候选顺序：请求点名了某个模型（非 "auto"）-> 它排第一，其余按优先级跟随；
// 填 "auto" 或不填 -> 按优先级全序。
function candidateOrder(requested) {
  if (requested && requested !== 'auto') {
    return [requested, ...MODELS.filter((m) => m !== requested)];
  }
  return [...MODELS];
}

// 挑这一轮实际要试的：跳过禁闭中的；如果全在禁闭里，按解禁先后兜底全试（fail-open）。
function pickCandidates(requested) {
  const ordered = candidateOrder(requested);
  const free = ordered.filter((m) => !isBenched(m));
  if (free.length > 0) return free;
  return ordered.sort((a, b) => breakerOf(a).benchedUntil - breakerOf(b).benchedUntil);
}

// ---------- 单次上游尝试 ----------
// 回调风格，返回一个可取消的 handle。事件通过 handlers 通知：
//  onCommit(statusCode, headers, firstChunk) —— 已锁定该模型，开始向客户端输出（仅流式）
//  onDone(statusCode, headers, bodyBuffer)   —— 非流式完整结束，或流式错误响应体收集完
//  onError(err, kind)                        —— 网络错误/超时，kind: 'retry' | 'first_token_timeout'
function attemptOnce(model, payload, clientHeaders, handlers) {
  const bodyStr = JSON.stringify({ ...payload, model });
  const started = Date.now();
  let settled = false;
  let committed = false;

  // 上游迟迟不回响应头也要超时：流式按首块超时算，非流式按单次总超时算。
  const preTimer = setTimeout(() => {
    if (settled || committed) return;
    settled = true;
    try { req.destroy(); } catch (_) {}
    handlers.onError(
      new Error('upstream no response'),
      payload.stream === true ? 'first_token_timeout' : 'retry',
      Date.now() - started
    );
  }, payload.stream === true ? FIRST_TOKEN_TIMEOUT_MS : ATTEMPT_TIMEOUT_MS);

  const req = transport.request(
    {
      protocol: upstreamUrl.protocol,
      hostname: upstreamUrl.hostname,
      port: upstreamUrl.port || (upstreamUrl.protocol === 'https:' ? 443 : 80),
      path: upstreamUrl.pathname,
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(bodyStr),
        ...(clientHeaders.authorization ? { authorization: clientHeaders.authorization } : {}),
        accept: clientHeaders.accept || '*/*',
      },
    },
    (up) => {
      clearTimeout(preTimer);
      const status = up.statusCode || 0;
      const isStream = payload.stream === true;

      if (!isStream) {
        // 非流式：收完整个响应体再决定
        const chunks = [];
        let total = 0;
        const timer = setTimeout(() => {
          if (settled) return;
          settled = true;
          req.destroy();
          handlers.onError(new Error('attempt timeout'), 'retry', Date.now() - started);
        }, ATTEMPT_TIMEOUT_MS);
        up.on('data', (c) => {
          total += c.length;
          if (total <= MAX_BODY_BYTES) chunks.push(c);
        });
        up.on('end', () => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          handlers.onDone(status, up.headers, Buffer.concat(chunks), Date.now() - started);
        });
        up.on('error', (err) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          handlers.onError(err, 'retry', Date.now() - started);
        });
        return;
      }

      // 流式
      if (status < 200 || status >= 300) {
        // 错误响应：收完错误体（通常很小）再交给上层分类
        const chunks = [];
        const timer = setTimeout(() => {
          if (settled) return;
          settled = true;
          req.destroy();
          handlers.onDone(status, up.headers, Buffer.concat(chunks), Date.now() - started);
        }, Math.min(ATTEMPT_TIMEOUT_MS, 15000));
        up.on('data', (c) => chunks.push(c));
        up.on('end', () => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          handlers.onDone(status, up.headers, Buffer.concat(chunks), Date.now() - started);
        });
        up.on('error', () => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          handlers.onDone(status, up.headers, Buffer.concat(chunks), Date.now() - started);
        });
        return;
      }

      // 流式 2xx：等第一个数据块；超时/出错且还没输出 -> 可切换
      let firstTimer = setTimeout(() => {
        if (settled || committed) return;
        settled = true;
        req.destroy();
        handlers.onError(new Error('first token timeout'), 'first_token_timeout', Date.now() - started);
      }, FIRST_TOKEN_TIMEOUT_MS);

      up.once('data', (firstChunk) => {
        if (settled) return;
        clearTimeout(firstTimer);
        committed = true;
        handlers.onCommit(status, up.headers, firstChunk, up, () => {
          settled = true;
        });
      });
      up.on('error', (err) => {
        if (settled || committed) return;
        settled = true;
        clearTimeout(firstTimer);
        handlers.onError(err, 'retry', Date.now() - started);
      });
    }
  );

  req.on('error', (err) => {
    clearTimeout(preTimer);
    if (settled || committed) return;
    settled = true;
    handlers.onError(err, 'retry', Date.now() - started);
  });

  req.write(bodyStr);
  req.end();

  return {
    cancel() {
      settled = true;
      try { req.destroy(); } catch (_) {}
    },
  };
}

// 错误分类：返回 'client'（原样返回不切换）| 'auth' | 'retry'
function classify(status) {
  if (status === 400 || status === 422) return 'client';
  if (status === 401 || status === 403) return 'auth';
  return 'retry'; // 429 / 5xx / 其它
}

// ---------- 主请求处理 ----------
function handleChatCompletions(req, res) {
  const chunks = [];
  let total = 0;
  let tooBig = false;
  req.on('data', (c) => {
    total += c.length;
    if (total > MAX_BODY_BYTES) { tooBig = true; return; }
    chunks.push(c);
  });
  req.on('end', () => {
    if (tooBig) {
      res.writeHead(413, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'request body too large', type: 'router_error' } }));
      return;
    }
    let payload;
    try {
      payload = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
    } catch (_) {
      res.writeHead(400, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'invalid JSON body', type: 'router_error' } }));
      return;
    }

    const candidates = pickCandidates(payload.model);
    const attempts = [];
    let idx = 0;
    let currentHandle = null;
    let clientGone = false;

    res.on('close', () => {
      clientGone = true;
      if (currentHandle) currentHandle.cancel();
    });

    function tryNext() {
      if (clientGone) return;
      if (idx >= candidates.length) {
        // 全部失败：有上游错误体就回最后一个，没有就回 502 汇总
        const last = attempts[attempts.length - 1];
        if (last && last.body) {
          res.writeHead(last.status, {
            'content-type': last.contentType || 'application/json',
            'x-router-attempts': attempts.map((a) => `${a.model}:${a.status}`).join(','),
          });
          res.end(last.body);
        } else {
          res.writeHead(502, { 'content-type': 'application/json' });
          res.end(JSON.stringify({
            error: {
              message: 'all models failed',
              type: 'router_error',
              attempts: attempts.map((a) => ({ model: a.model, status: a.status, note: a.note || '' })),
            },
          }));
        }
        return;
      }

      const model = candidates[idx++];
      currentHandle = attemptOnce(model, payload, req.headers, {
        onCommit(status, headers, firstChunk, upStream, markSettled) {
          recordSuccess(model);
          logAttempt(model, status, 0, 'stream committed');
          res.writeHead(status, {
            'content-type': headers['content-type'] || 'text/event-stream',
            'cache-control': 'no-cache',
            connection: 'keep-alive',
            'x-router-model': model,
            'x-router-attempts': attempts.map((a) => `${a.model}:${a.status}`).join(','),
          });
          res.write(firstChunk);
          upStream.on('data', (c) => { if (!clientGone) res.write(c); });
          upStream.on('end', () => { markSettled(); if (!clientGone) res.end(); });
          upStream.on('error', () => { markSettled(); if (!clientGone) res.end(); });
        },
        onDone(status, headers, body, ms) {
          if (status >= 200 && status < 300) {
            recordSuccess(model);
            logAttempt(model, status, ms, payload.stream ? 'stream-error-body?' : 'ok');
            attempts.push({ model, status });
            res.writeHead(status, {
              'content-type': headers['content-type'] || 'application/json',
              'x-router-model': model,
              'x-router-attempts': attempts.map((a) => `${a.model}:${a.status}`).join(','),
            });
            res.end(body);
            return;
          }
          const kind = classify(status);
          attempts.push({ model, status, body, contentType: headers['content-type'] });
          if (kind === 'client') {
            logAttempt(model, status, ms, 'client error, returned as-is');
            res.writeHead(status, {
              'content-type': headers['content-type'] || 'application/json',
              'x-router-model': model,
            });
            res.end(body);
            return;
          }
          recordFailure(model, kind);
          logAttempt(model, status, ms, kind === 'auth' ? 'auth error, long bench' : 'failed, switching');
          tryNext();
        },
        onError(err, kind, ms) {
          attempts.push({ model, status: 0, note: kind });
          recordFailure(model, 'retry');
          logAttempt(model, 0, ms, `${kind}: ${err.message}`);
          tryNext();
        },
      });
    }

    tryNext();
  });
}

// ---------- HTTP 服务 ----------
const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://localhost');
  if (req.method === 'POST' && u.pathname === '/v1/chat/completions') {
    handleChatCompletions(req, res);
    return;
  }
  if (req.method === 'GET' && u.pathname === '/v1/models') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      object: 'list',
      data: [
        { id: 'auto', object: 'model', owned_by: 'llm-auto-router' },
        ...MODELS.map((m) => ({ id: m, object: 'model', owned_by: 'upstream' })),
      ],
    }));
    return;
  }
  if (req.method === 'GET' && (u.pathname === '/health' || u.pathname === '/status')) {
    const now = Date.now();
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      upstream: UPSTREAM_BASE,
      models: MODELS.map((m) => {
        const b = breakerOf(m);
        return {
          model: m,
          consecutive_fails: b.fails,
          benched: b.benchedUntil > now,
          bench_remaining_seconds: b.benchedUntil > now ? Math.ceil((b.benchedUntil - now) / 1000) : 0,
        };
      }),
      recent_attempts: recentAttempts.slice(-10),
    }, null, 2));
    return;
  }
  if (req.method === 'GET' && u.pathname === '/') {
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
    res.end(`llm-auto-router is running.\nPOST /v1/chat/completions with model "auto"\nGET /status for breaker state\n`);
    return;
  }
  res.writeHead(404, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ error: { message: 'not found', type: 'router_error' } }));
});

server.listen(PORT, () => {
  console.log(`[router] listening on :${PORT}, upstream=${UPSTREAM_BASE}, models=${MODELS.join(' > ')}`);
});
