// ============================================================
// SOLANA MULTI-WALLET TRACKER — FAST BOT (60s window)
// Zero credits. No webhook provider. Runs forever for free.
// + SELL SIGNAL TRACKER — fires when all coordinated wallets exit
// ============================================================
// ⚠️ CRITICAL RULE: NEVER modify working code. Only change what
// is explicitly asked for.
// ============================================================

const https   = require('https');
const http    = require('http');
const fs      = require('fs');
const WebSocket = require('ws');

const TELEGRAM_TOKEN   = process.env.TELEGRAM_TOKEN;
const CHAT_ID          = process.env.CHAT_ID;
const GMGN_API_KEY     = process.env.GMGN_API_KEY;
const SHYFT_API_KEY    = process.env.SHYFT_API_KEY;
const HELIUS_API_KEY   = process.env.HELIUS_API_KEY;

const SOL_MINT         = 'So11111111111111111111111111111111111111112';
const WINDOW_SECS      = 300;
const MAX_TOKEN_AGE    = 900; // 15 minutes
const STRICT_AGE_CHECK = true;
const NOTABLE_HOLDER_THRESHOLD = 50_000;
const SAME_NAME_THRESHOLD = 10;
const DEV_ATH_THRESHOLD   = 1_000_000;

const WSS_PRIMARY  = HELIUS_API_KEY ? `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : SHYFT_API_KEY ? `wss://rpc.shyft.to?api_key=${SHYFT_API_KEY}` : 'wss://api.mainnet-beta.solana.com';
const WSS_FALLBACK = 'wss://api.mainnet-beta.solana.com';
const HTTP_RPC     = HELIUS_API_KEY ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : SHYFT_API_KEY ? `https://rpc.shyft.to?api_key=${SHYFT_API_KEY}` : 'https://api.mainnet-beta.solana.com';
const FIRED_FILE   = '/tmp/fired_alerts.json';

function loadFiredAlerts() {
  try { if (fs.existsSync(FIRED_FILE)) return new Set(JSON.parse(fs.readFileSync(FIRED_FILE, 'utf8'))); }
  catch(e) {}
  return new Set();
}
function saveFiredAlert(mint) {
  firedAlerts.add(mint);
  try { fs.writeFileSync(FIRED_FILE, JSON.stringify([...firedAlerts]), 'utf8'); } catch(e) {}
}

const WALLETS = [
  "CzbN6T1gKkKutvuPXcxNmV8FLqzjsDWebWmg9o8e2ZbU","H8s4GoDcABkvykQSS7mUSHTSKUcxivoULUXgZDkjuoUf",
  "AmNMqM5VbPwtG14gLBdtrqZpQrhSzavLkQPufS8CQ7LB","AMRsSeU5JpqwQWJGNLMpZzRCZSFEwYQYbMnms3dD4311",
  "2bBRwhGoL4fRZk6g8NnhBZywsF8PdLJnBRfWDCEMogD2","6EDaVsS6enYgJ81tmhEkiKFcb4HuzPUVFZeom6PHUqN3",
  "Aqje5DsN4u2PHmQxGF9PKfpsDGwQRCBhWeLKHCFhSMXk","HiSo5kykqDPs3EG14Fk9QY4B5RvkuEs8oJTiqPX3EDAn",
  "FxN3VZ4BosL5urG2yoeQ156JSdmavm9K5fdLxjkPmaMR","JDQKDrc1TQgBRvdFh56tkta5sYcDj1SoP52Eiu64rSrT",
  "HyYNVYmnFmi87NsQqWzLJhUTPBKQUfgfhdbBa554nMFF","GeUnv1jmtviRbR7Gu1JnXSGkUMUgFVBHuEVQVpTaUX1W",
  "78N177fzNJpp8pG49xDv1efYcTMSzo9tPTKEA9mAVkh2","8ZN71XTdVo8yRovnGLmNgW3Tgniw6A4J3JGLvPD686FP",
  "DPNPVvoGdwNBY849ryx2JZzakWuWbDTfSUYr8aNfKLwA","Hp34goKgAhAYW6sw9iFAZofvDTr3DAhtkSKF1R9bAk2P",
  "95ZCf3jKMHeFYvPXVZW3Ek6AEPDyjebosqnc7eNioVMo","G7NvZKjoVqBDWciSYtWWgUPB7DA1iJavdvH5jty2FAmM",
  "BCagckXeMChUKrHEd6fKFA1uiWDtcmCXMsqaheLiUPJd","4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9",
  "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o","8deJ9xeUvXSJwicYptA9mHsU2rN2pDx37KWzkDkEXhU6",
  "2T5NgDDidkvhJQg8AHDi74uCFwgp25pYFMRZXBaCUNBH","515vh1DrPuwMATt9Zoq9kP4sJL9fyojA1dHJu4DQpNRp",
  "GpTXmkdvrTajqkzX1fBmC4BUjSboF9dHgfnqPqj8WAc4","2ezv4U5HmPpkt2xLsKnw1FyyGmjFBeW7c166p99Hw2xB",
  "EaVboaPxFCYanjoNWdkxTbPvt57nhXGu5i6m9m6ZS2kK","FAicXNV5FVqtfbpn4Zccs71XcfGeyxBSGbqLDyDJZjke",
  "BAr5csYtpWoNpwhUjixX7ZPHXkUciFZzjBp9uNxZXJPh","B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC",
  "8HcYptCBAaPFWkmupiSAmysZ6Z8jB7N1c4YhVjhX7zbg","FFEjC9MHhpQViBPrD2iU6LmV2hEigyhLJaL7MZUZzyD4",
  "FTaSBuVj6w2S7XUa8fw19xrLy57DDr6kZDL6sxDXtvTP","FSAmbD6jm6SZZQadSJeC1paX3oTtAiY9hTx1UYzVoXqj",
  "G6fUXjMKPJzCY1rveAE6Qm7wy5U3vZgKDJmN1VPAdiZC","Ar2Y6o1QmrRAskjii1cRfijeKugHH13ycxW5cd7rro1x",
  "5aLY85pyxiuX3fd4RgM3Yc1e3MAL6b7UgaZz6MS3JUfG","DYAn4XpAkN5mhiXkRB7dGq4Jadnx6XYgu8L5b3WGhbrt",
  "7BNaxx6KdUYrjACNQZ9He26NBFoFxujQMAfNLnArLGH5","BCnqsPEtA1TkgednYEebRpkmwFRJDCjMQcKZMMtEdArc",
  "4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk","5ZuV8eqkvzYFVEKbLvGBdexL2tFv7E5BCd2HZpjqbdg",
  "FM1YCKED2KaqB8Uat8aB1nsffR1vezr7s6FAEieXJgke","AV7PjXHL5JXZ1YoYRoN9Dsstg1x2UciBupMCXcJP8gUz",
  "Dzp1SrZ474xwGp6ZEP6cNKo39u9zeXe1YAuTkyZyv3t4","whamNNP9tHoxLg92yHvJPdYhghEoCg1qYTsh5a2oLbx",
  "HdKJM6Lvfp9aV9tvEMC8AD4GnsbFgMUkHLoK923Sn1ET","5FqUo9aBjsp7QeeyN6Vi2ZmF2fjS4H5EU7wnAQwPy17z",
  "7hHmfYYR7L8LsCKk5akjtvVu1BbJRgHGJ2n6s7gbeKG4","CjtqWn4toBbJ1feRZBDhz3TwBjbZm5RpES8rvKWTuNtk",
  "FAX4qRQdiSj2iWDYvkJ21VieVCXGREtwMhEyAHSJ1aqp","9VXuNqqqzniYYW3fRDeaCtUUtqWsEeWWn5umh3aF9h17",
  "DAEdBmTPEKM6xkwfzC3d411QUe6coKpkND6UURa4CvHC","iPUp3qkm39ycMGbywWFMUyvaDhiiPGXeWXaDtmHNe6C",
  "CfkaAru9ArJ2tAStYHvbAyRBJL3EhDzsWYV2KYg9shxB","EeLjBXRELqrcWAXbnj8T4jQPS9Qh7UGWiKxovsJ36pZY",
  "H5Wh4EDvWQT4mShH746V5VDqxHQkaQZyPWfuhy1PRVBg","GH9yk8vgFvHnAD8JZqXxr3hBN1Lr1mJ9NPzrP5mVqiJe",
  "7hkd2kdx4bMyuUDgktZvykDh69r8YkkrX4kf1sW2C8T6","8ghYW6ftL5kUemfsoA9X37rz3ZnvyMSZRAx1kt1CxpoS",
  "GKaJNFDp2W5uCYfNKnTPN63tFXKgXgaDSfnTVfksBeq1","DaKpjVJFxq3y4iZcEu12wzpXGCNBkQE587VNACUj15rT",
  "C4ARzqpvZ4gR3ta89H5Yz7UyPTpRm22BL5U91e5dHTSf","BSFxyBwsHQsDXULygBpsTu6iUmfHUbCr6j4geZSN6YJG",
  "9Zu8AigeXgFAajBTni2VWw6Wmz7XxDqHmY5nQwdCWAyY","9dkeTBYaHJzxVgVZqympcHmPeQvHtQv1sArZiZuwmhgp",
  "AQdBYZNy3BZ1vouGUjA1w9Ay7aq7kH5UQSuh4LQWKotY","HTM87R4mgjDdiF6Yfn8duK9vbDmZxiPCTRbGvm7eCAJY",
  "8i5U2uNBEuTc4zskYP14zbebDg2RSwrrG8REhEnJb97K","7E9jfxCczubz4FXkkVKzUMHXGwzJxyppC4m7y3ew8ATg",
  "8v6ztxZwhPBNmA6aGrBzzrt6UBf2fZZfsWqZ9Lt47Kpv","6nU2L7MQVUWjtdKHVpuZA9aind73nd3rXC4YFo8KQCy4",
  "5zCkbcD74hFPeBHwYdwJLJAoLVgHX45AFeR7RzC8vFiD","8HeDT75s5g4CtCimH5B5nySqCiQhtWii8UnZhxBtFo38",
  "A8Z1ejQGk45EJibBPJviWnM3UvwKSuYun53nSCkWKM52","D9gQ6RhKEpnobPBUdWY5bPQt2p3zGk3iVz6ChpUi2ArA",
  "BZC7VEj5Y9Ege3cTRGBZW2zW7pjw3hpiSkcAoYKysvue","FgifQEkRkSSXZjf2cJ4c55BhVts2yrNKzmzBLLyicg8b",
  "EFaQQTGywnD4CjQQvTugUiyVT4LV9G6MsWqiub8X6unN","HUgpmqL6r4Z4iEZiVuNZ6J6QnAsSZpsL8giVyVtz3QhT",
  // FaBGrHWj — Dale Dev REMOVED (floods)
  "HYWo71Wk9PNDe5sBaRKazPnVyGnQDiwgXCFKvgAQ1ENp","bwamJzztZsepfkteWRChggmXuiiCQvpLqPietdNfSXa",
  // 7moqFjvm — Smart 15 REMOVED (floods)
  "DjM7Tu7whh6P3pGVBfDzwXAx2zaw51GJWrJE3PwtuN7s",
  "AvcWA3ngM55sSpjh1FZthmqA7V6BHo4f555a8w3Wv3ij",
  "J7nJ35d8EGU3fHCVCUun56C1MKakdoEQ38CFLHAhWDwP",
  // 6ujZxnph — BadBunny Dev REMOVED (floods)
  "nazikTJezTC3W2fxXE3wzs495PYzXMiq5o7co6YYACA",
  "BtMBMPkoNbnLF9Xn552guQq528KKXcsNBNNBre3oaQtr",
  "EYfdt8cNFyyTEJKp18dcoVbgUHDnM1SK3bT2uKj9XXHc",
  "EgQX9R3Qph1dPHE1Ysou1auSYqRGomCNmLDC28Yg77aq",
];
const WALLET_SET = new Set(WALLETS);

let firedAlerts    = loadFiredAlerts();
let activeAlerts   = {};
let devWalletCache = {};
let creationCache  = {};
let skipCache      = {};
let subIdToWallet  = {};
let ws             = null;
let wsReady        = false;
let reconnectDelay = 5000;
let usingFallback  = false;
let pendingSigs    = new Set();
let sellWatchlist  = {};
let tokenInfoCache    = {};
let tokenInfoInflight = {};
const logBuffer = [];

async function getCachedTokenInfo(mint) {
  if (mint in tokenInfoCache) return tokenInfoCache[mint];
  if (tokenInfoInflight[mint]) return tokenInfoInflight[mint];
  tokenInfoInflight[mint] = fetchTokenInfo(mint).then(info => {
    tokenInfoCache[mint] = info;
    delete tokenInfoInflight[mint];
    setTimeout(() => delete tokenInfoCache[mint], 600000);
    return info;
  });
  return tokenInfoInflight[mint];
}

function log(msg) {
  const t = new Date().toLocaleTimeString('en-US', { timeZone: 'America/Toronto', hour12: true, hour: '2-digit', minute: '2-digit', second: '2-digit' });
  const line = `[${t}] ${msg}`;
  console.log(line);
  logBuffer.push(line);
  if (logBuffer.length > 500) logBuffer.shift();
}

log(`[INIT] Loaded ${firedAlerts.size} previously fired contracts`);

setInterval(() => {
  const now = Math.floor(Date.now() / 1000);
  for (const mint of Object.keys(activeAlerts)) { if (now - activeAlerts[mint].firstSeenAt > WINDOW_SECS) delete activeAlerts[mint]; }
}, 60000);

setInterval(() => {
  const now = Math.floor(Date.now() / 1000);
  for (const mint of Object.keys(sellWatchlist)) {
    if (now - sellWatchlist[mint].signalTime > 6 * 3600) { log(`[SELL] Watchlist entry for ${mint.substring(0, 8)} expired`); delete sellWatchlist[mint]; }
  }
}, 5 * 60 * 1000);

function isActiveHours() {
  const eastern = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const val = eastern.getHours() * 60 + eastern.getMinutes();
  return val >= 660 && val < 1080;
}

function httpsGet(hostname, path, headers = {}) {
  return new Promise((resolve) => {
    const req = https.request({ hostname, path, method: 'GET', headers }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode !== 200) { log(`[HTTP] ${hostname} returned ${res.statusCode}`); resolve(null); return; }
        try { resolve(JSON.parse(data)); } catch { resolve(null); }
      });
    });
    req.on('error', (e) => { log(`[HTTP] Error ${hostname}: ${e.message}`); resolve(null); });
    req.setTimeout(15000, () => { req.destroy(); resolve(null); });
    req.end();
  });
}

function httpsPost(url, body) {
  return new Promise((resolve) => {
    const payload = JSON.stringify(body);
    const u = new URL(url);
    const req = https.request({ hostname: u.hostname, path: u.pathname + u.search, method: 'POST', headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) } }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch { resolve(null); } });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(20000, () => { req.destroy(); resolve(null); });
    req.write(payload);
    req.end();
  });
}

async function getTransaction(signature) {
  const result = await httpsPost(HTTP_RPC, { jsonrpc: '2.0', id: 1, method: 'getTransaction', params: [signature, { encoding: 'json', maxSupportedTransactionVersion: 0, commitment: 'confirmed' }] });
  return result?.result ?? null;
}

function extractMint(tx) {
  const meta = tx?.meta; const msg = tx?.transaction?.message;
  if (!meta || !msg) return null;
  const postBals = meta.postTokenBalances ?? []; const preBals = meta.preTokenBalances ?? [];
  const preOwned = new Set(preBals.map(b => b.mint));
  let mint = postBals.find(b => b.mint && b.mint !== SOL_MINT && !preOwned.has(b.mint))?.mint;
  if (!mint) mint = postBals.find(b => b.mint && b.mint !== SOL_MINT)?.mint;
  return mint ?? null;
}

function extractFullSell(tx, trackedWallet) {
  const meta = tx?.meta; if (!meta) return null;
  const preBals = meta.preTokenBalances ?? []; const postBals = meta.postTokenBalances ?? [];
  const accountKeys = (tx?.transaction?.message?.accountKeys ?? []).map(k => typeof k === 'string' ? k : (k?.pubkey ?? ''));
  function resolveOwner(b) { if (b.owner) return b.owner; const idx = b.accountIndex; return (idx !== undefined && accountKeys[idx]) ? accountKeys[idx] : null; }
  for (const pre of preBals) {
    if (!pre.mint || pre.mint === SOL_MINT || !sellWatchlist[pre.mint]) continue;
    const owner = resolveOwner(pre); if (!owner || owner !== trackedWallet) continue;
    const preAmt = parseFloat(pre.uiTokenAmount?.uiAmountString ?? pre.uiTokenAmount?.amount ?? '0'); if (preAmt <= 0) continue;
    const post = postBals.find(p => p.mint === pre.mint && resolveOwner(p) === trackedWallet);
    const postAmt = post ? parseFloat(post.uiTokenAmount?.uiAmountString ?? post.uiTokenAmount?.amount ?? '0') : 0;
    if (postAmt === 0) { log(`[SELL] Full exit: ${trackedWallet.substring(0,8)} sold ${pre.mint.substring(0,8)}`); return pre.mint; }
  }
  return null;
}

function sendSellSignal(tokenMint, entry) {
  const elapsed = Math.floor(Date.now() / 1000) - entry.signalTime;
  const elapsedStr = elapsed >= 60 ? `${Math.floor(elapsed/60)}m ${elapsed%60}s` : `${elapsed}s`;
  const signalTime = new Date().toLocaleTimeString('en-US', { timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
  sendTelegram(`🚨 <b>SELL Signal — All Wallets Exited</b>\n\nToken: #${entry.symbol}\nContract: <code>${tokenMint}</code>\nWallets Exited: ${entry.wallets.size}/${entry.wallets.size}\nTime Since Buy Signal: ${elapsedStr}\nSignal Time: ${signalTime}\n\n<a href="https://gmgn.ai/sol/token/${tokenMint}">GMGN</a>`);
  log(`[SELL] 🚨 Sell signal fired for #${entry.symbol}`);
}

async function handlePotentialSell(trackedWallet, tx) {
  const soldMint = extractFullSell(tx, trackedWallet); if (!soldMint) return;
  const entry = sellWatchlist[soldMint]; if (!entry || !entry.wallets.has(trackedWallet)) return;
  entry.exited.add(trackedWallet);
  log(`[SELL] ${trackedWallet.substring(0,8)} exited #${entry.symbol} | ${entry.exited.size}/${entry.wallets.size}`);
  if (entry.exited.size >= entry.wallets.size) { sendSellSignal(soldMint, entry); delete sellWatchlist[soldMint]; }
}

async function gmgnGet(path, params = {}) {
  params.timestamp = Math.floor(Date.now() / 1000).toString();
  params.client_id = Math.random().toString(36).substring(2) + Date.now().toString(36);
  const headers = { 'X-APIKEY': GMGN_API_KEY, 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36' };
  const parsed = await httpsGet('openapi.gmgn.ai', `${path}?${new URLSearchParams(params)}`, headers);
  if (parsed?.code === 0 && parsed?.data) return parsed.data;
  log(`[GMGN] Error ${path}: ${JSON.stringify(parsed)?.substring(0, 100)}`);
  return null;
}

async function fetchTokenInfo(mint) { return await gmgnGet('/v1/token/info', { chain: 'sol', address: mint }); }
async function fetchFreshWallets(mint) {
  const data = await gmgnGet('/v1/token/security', { chain: 'sol', address: mint });
  if (!data) return null;
  return data.fresh_holder_count ?? data.fresh_wallet_count ?? data.fresh_holders ?? data.freshHolder ?? null;
}

async function fetchTopHolders(mint) {
  const data = await gmgnGet('/v1/token/top_holders', { chain: 'sol', address: mint, limit: '20' });
  if (!data) return [];
  return Array.isArray(data) ? data : (data.holders ?? data.top_holders ?? data.data ?? []);
}
async function fetchWalletValue(addr) {
  const data = await gmgnGet('/v1/wallet/info', { chain: 'sol', address: addr });
  if (!data) return null;
  const val = parseFloat(data.total_value ?? data.usd_value ?? data.portfolio_value ?? 0);
  return isNaN(val) ? null : val;
}
async function fetchNotableHolders(mint) {
  try {
    const holders = await fetchTopHolders(mint);
    if (!holders.length) return [];
    log(`[HOLDERS] Checking ${holders.length} top holders for ${mint.substring(0,8)}...`);
    const notable = [];
    for (const h of holders) {
      const addr = h.address ?? h.wallet ?? h.owner; if (!addr || WALLET_SET.has(addr)) continue;
      await new Promise(r => setTimeout(r, 400));
      const value = await fetchWalletValue(addr); if (value === null || value < NOTABLE_HOLDER_THRESHOLD) continue;
      const pct = h.percent ?? h.percentage ?? null;
      notable.push({ addr, valStr: value >= 1_000_000 ? `$${(value/1_000_000).toFixed(1)}M` : `$${Math.round(value/1000)}k`, pctStr: pct !== null ? ` (${parseFloat(pct).toFixed(1)}%)` : '' });
    }
    return notable;
  } catch(e) { log(`[ERR] fetchNotableHolders: ${e.message}`); return []; }
}

async function dexFetch(url) {
  const reqHeaders = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept': 'application/json' };
  for (let attempt = 0; attempt < 4; attempt++) {
    const result = await new Promise((resolve) => {
      const req = https.get(url, { headers: reqHeaders }, (res) => {
        if ([301,302,307,308].includes(res.statusCode) && res.headers.location) {
          res.resume();
          const rr = https.get(res.headers.location, { headers: reqHeaders }, (res2) => { let d=''; res2.on('data',c=>d+=c); res2.on('end',()=>{try{resolve(JSON.parse(d));}catch{resolve(null);}}); });
          rr.on('error',()=>resolve(null)); rr.setTimeout(15000,()=>{rr.destroy();resolve(null);}); return;
        }
        if (res.statusCode === 429) { log(`[Dex] 429 attempt ${attempt+1}`); res.resume(); resolve('RATE_LIMITED'); return; }
        if (res.statusCode !== 200) { res.resume(); resolve(null); return; }
        let d=''; res.on('data',c=>d+=c); res.on('end',()=>{try{resolve(JSON.parse(d));}catch{resolve(null);}});
      });
      req.on('error',(e)=>{log(`[Dex] Error: ${e.message}`);resolve(null);}); req.setTimeout(15000,()=>{req.destroy();resolve(null);});
    });
    if (result === 'RATE_LIMITED') { await new Promise(r=>setTimeout(r,(attempt+1)*5000)); continue; }
    if (result) return result;
    if (attempt < 3) await new Promise(r=>setTimeout(r,2000));
  }
  return null;
}

async function fetchSameNameCount(mint, symbol) {
  const nowSecs = Math.floor(Date.now()/1000); const cutoff = 5*3600;
  function countMatches(pairs, sym, ex) {
    return pairs.filter(p => {
      if ((p.chainId??p.chain_id)!=='solana') return false;
      if (p.baseToken?.symbol?.toUpperCase()!==sym.toUpperCase()) return false;
      if (p.baseToken?.address===ex) return false;
      const ca=p.pairCreatedAt??p.pair_created_at; if(!ca) return false;
      const age=nowSecs-Math.floor(ca/1000); return age>=0&&age<=cutoff;
    }).length;
  }
  await new Promise(r=>setTimeout(r,4000));
  log(`[Dex] Fetching pairs for mint ${mint.substring(0,8)}...`);
  const r1 = await dexFetch(`https://api.dexscreener.com/latest/dex/tokens/${mint}`);
  if (r1) {
    const pairs = r1.pairs??r1.data??[];
    const sym = (symbol&&symbol!=='UNKNOWN') ? symbol : (pairs.find(p=>p.chainId==='solana')?.baseToken?.symbol??null);
    if (sym) { const c=countMatches(pairs,sym,mint); log(`[Dex] Mint lookup: ${sym} — ${c} same-name tokens in last 5h`); return c; }
    return 0;
  }
  if (symbol&&symbol!=='UNKNOWN') {
    log(`[Dex] Mint lookup failed — trying symbol search for ${symbol}...`);
    await new Promise(r=>setTimeout(r,3000));
    const r2 = await dexFetch(`https://api.dexscreener.com/latest/dex/search?q=${encodeURIComponent(symbol)}`);
    if (r2) { const pairs=r2.pairs??r2.data??[]; const c=countMatches(pairs,symbol,mint); log(`[Dex] Symbol search: ${symbol} — ${c} same-name tokens in last 5h`); return c; }
  }
  log(`[Dex] Both paths failed for ${mint.substring(0,8)} — returning null`); return null;
}

async function getTokenAge(mint) {
  const now=Math.floor(Date.now()/1000);
  if (skipCache[mint]) return -1;
  if (creationCache[mint]) { const age=now-creationCache[mint]; if(age>MAX_TOKEN_AGE){skipCache[mint]=true;return -1;} return age; }
  const info=await getCachedTokenInfo(mint); if(!info) return null;
  const ca=info.creation_timestamp; if(!ca) return null;
  creationCache[mint]=ca; const age=now-ca; if(age>MAX_TOKEN_AGE){skipCache[mint]=true;return -1;} return age;
}

function sendTelegram(message) {
  const body=JSON.stringify({chat_id:CHAT_ID,text:message,parse_mode:'HTML'});
  const req=https.request({hostname:'api.telegram.org',path:`/bot${TELEGRAM_TOKEN}/sendMessage`,method:'POST',headers:{'Content-Type':'application/json','Content-Length':Buffer.byteLength(body)}},(res)=>{
    let d=''; res.on('data',c=>d+=c); res.on('end',()=>{ try{const p=JSON.parse(d);if(!p.ok)log(`[TG Error] ${p.description}`);else log(`[TG] Signal delivered`);}catch{log(`[TG Error] Parse failed`);} });
  });
  req.on('error',e=>log(`[TG ERR] ${e.message}`)); req.write(body); req.end();
}

function shouldFireSignal(tokenMint, symbol, sameNameCount, devWallet, devAthMc) {
  const devIsTracked = devWallet&&devWallet!=='unknown'&&WALLET_SET.has(devWallet);
  const devAthPasses = devWallet&&devWallet!=='unknown'&&devAthMc!==null&&devAthMc>=DEV_ATH_THRESHOLD;
  const sameNamePasses = sameNameCount!==null&&sameNameCount>=SAME_NAME_THRESHOLD;
  const devPasses = devAthPasses||devIsTracked;
  if (sameNamePasses&&devPasses) { log(`[FILTER] ✅ PASS — same-name ${sameNameCount} AND dev passes`); return true; }
  const snStr=sameNameCount!==null?sameNameCount:'?'; const athStr=devAthMc!==null?`$${devAthMc.toLocaleString()}`:'N/A';
  if (!sameNamePasses&&!devPasses) log(`[FILTER] ❌ SUPPRESSED #${symbol} — same-name: ${snStr}, dev ATH: ${athStr}`);
  else if (!sameNamePasses) log(`[FILTER] ❌ SUPPRESSED #${symbol} — same-name: ${snStr} (need >=${SAME_NAME_THRESHOLD}), dev would pass`);
  else log(`[FILTER] ❌ SUPPRESSED #${symbol} — same-name passes but dev ATH: ${athStr}`);
  return false;
}

async function buildAndSendSignal(tokenMint, walletCount, elapsed, tokenInfo, coordinatedWallets) {
  try {
    const now=Math.floor(Date.now()/1000);
    let symbol='UNKNOWN',mintTimeStr='N/A',ageStr='N/A',liquidityStr='N/A',marketCapStr='N/A';
    let devWallet=null,devAthMc=null,devAth='N/A',devAthSymbol='',freshWalletsFromInfo=null;
    if (tokenInfo) {
      symbol=tokenInfo.symbol??'UNKNOWN';
      const ca=tokenInfo.creation_timestamp;
      if(ca){mintTimeStr=new Date(ca*1000).toLocaleTimeString('en-US',{timeZone:'America/Toronto',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:true});const s=now-ca;ageStr=s<60?`${s}s`:`${Math.floor(s/60)}m ${s%60}s`;}
      const liq=parseFloat(tokenInfo.liquidity); if(!isNaN(liq))liquidityStr=`$${liq.toLocaleString('en-US',{maximumFractionDigits:0})}`;
      let mc=parseFloat(tokenInfo.market_cap??tokenInfo.usd_market_cap);
      if(isNaN(mc)||mc===0){const p=parseFloat(tokenInfo.price);const s=parseFloat(tokenInfo.circulating_supply??tokenInfo.total_supply);if(!isNaN(p)&&!isNaN(s)&&p>0&&s>0)mc=p*s;}
      if(!isNaN(mc)&&mc>0)marketCapStr=`$${mc.toLocaleString('en-US',{maximumFractionDigits:0})}`;
      const ca2=tokenInfo.dev?.creator_address; if(ca2)devWallet=ca2;
      const athInfo=tokenInfo.dev?.ath_token_info;
      if(athInfo?.ath_mc){const p=parseFloat(athInfo.ath_mc);if(!isNaN(p)){devAthMc=p;devAthSymbol=athInfo.symbol?` #${athInfo.symbol}`:'';devAth=p>=1_000_000?`$${(p/1_000_000).toFixed(1)}M${devAthSymbol}`:`$${p.toLocaleString('en-US',{maximumFractionDigits:0})}${devAthSymbol}`;}}
      const fw=tokenInfo.wallet_tags_stat?.fresh_wallets; if(fw!==undefined&&fw!==null)freshWalletsFromInfo=fw;
    }
    const freshWalletsFromSecurity = freshWalletsFromInfo===null ? await fetchFreshWallets(tokenMint) : null;
    const freshWallets = freshWalletsFromInfo??freshWalletsFromSecurity;
    const sameNameCount = await fetchSameNameCount(tokenMint, symbol);
    if (!shouldFireSignal(tokenMint,symbol,sameNameCount,devWallet,devAthMc)) return;
    if (coordinatedWallets&&coordinatedWallets.size>0) {
      sellWatchlist[tokenMint]={wallets:new Set(coordinatedWallets),exited:new Set(),symbol:symbol!=='UNKNOWN'?symbol:tokenMint.substring(0,8),signalTime:Math.floor(Date.now()/1000)};
      log(`[SELL] Watching ${coordinatedWallets.size} wallets for exits on #${sellWatchlist[tokenMint].symbol}`);
    }
    const notableHolders = await fetchNotableHolders(tokenMint);
    let notableLine='';
    if(notableHolders.length>0){notableLine=`\n\n💰 <b>Notable Holders (>$50k wallet)</b>\n`+notableHolders.map(h=>`  • <code>${h.addr}</code> — ${h.valStr}${h.pctStr}`).join('\n');}
    const signalTime=new Date().toLocaleTimeString('en-US',{timeZone:'America/Toronto',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:true});
    sendTelegram(`⚡ <b>3-Wallet Fast Signal (60s)</b>\n\nToken: #${symbol}\nContract: <code>${tokenMint}</code>\nMint Time: ${mintTimeStr}\nToken Age: ${ageStr}\nLiquidity: ${liquidityStr}\nMarket Cap: ${marketCapStr}\nSame-Name Count (5h): ${sameNameCount!==null?sameNameCount:'?'}\nFresh Wallets: ${freshWallets!==null?freshWallets:'N/A'}\nWallets Coordinated: ${walletCount} within ${elapsed}s\n\nDev Wallet: ${devWallet?`<code>${devWallet}</code>`:'N/A'}\nDev ATH: ${devAth}${notableLine}\n\nSignal Time: ${signalTime}\n\n<a href="https://gmgn.ai/sol/token/${tokenMint}">GMGN</a>`);
    log(`[ALERT] Signal sent for #${symbol} (${tokenMint.substring(0,8)}) | Dev ATH: ${devAth} | Notable: ${notableHolders.length}`);
  } catch(e) { log(`[ERR] buildAndSendSignal: ${e.message}`); }
}

async function handleWalletBuy(trackedWallet, tokenMint) {
  if (firedAlerts.has(tokenMint)) { log(`[SKIP] ${tokenMint.substring(0,8)} already signalled`); return; }
  if (!devWalletCache[tokenMint]) {
    const devInfo=await getCachedTokenInfo(tokenMint);
    devWalletCache[tokenMint]=devInfo?.dev?.creator_address??'unknown';
    setTimeout(()=>delete devWalletCache[tokenMint],600000);
  }
  if (devWalletCache[tokenMint]&&devWalletCache[tokenMint]!=='unknown'&&trackedWallet===devWalletCache[tokenMint]) { log(`[SKIP] ${trackedWallet.substring(0,8)} is the dev — not counting`); return; }
  const age=await getTokenAge(tokenMint);
  if (age===-1) { log(`[SKIP] ${tokenMint.substring(0,8)} too old`); return; }
  if (age===null) { if(STRICT_AGE_CHECK){log(`[SKIP] ${tokenMint.substring(0,8)} age unknown — strict mode rejects`);return;} log(`[WARN] Age unknown for ${tokenMint.substring(0,8)} — allowing`); }
  else log(`[AGE] ${tokenMint.substring(0,8)} is ${age<60?age+'s':Math.floor(age/60)+'m '+age%60+'s'} old`);
  const now=Math.floor(Date.now()/1000);
  if (!activeAlerts[tokenMint]) activeAlerts[tokenMint]={wallets:new Set(),firstSeenAt:now};
  const entry=activeAlerts[tokenMint];
  if (now-entry.firstSeenAt>WINDOW_SECS) { log(`[RESET] ${tokenMint.substring(0,8)} window expired`); activeAlerts[tokenMint]={wallets:new Set(),firstSeenAt:now}; }
  entry.wallets.add(trackedWallet);
  const count=entry.wallets.size;
  log(`[COUNT] ${count}/3 for ${tokenMint.substring(0,8)} within ${now-entry.firstSeenAt}s`);
  if (count>=3) {
    const elapsed=now-entry.firstSeenAt; const coordinatedWallets=new Set(entry.wallets);
    saveFiredAlert(tokenMint); delete activeAlerts[tokenMint];
    const tokenInfo=await getCachedTokenInfo(tokenMint);
    await buildAndSendSignal(tokenMint,count,elapsed,tokenInfo,coordinatedWallets);
  }
}

async function processLogNotification(params) {
  const value=params?.result?.value; const subId=params?.subscription;
  if (!value) { log(`[DEBUG] No value — raw: ${JSON.stringify(params)?.substring(0,120)}`); return; }
  if (value.err!==null&&value.err!==undefined) return;
  const signature=value.signature; const trackedWallet=subIdToWallet[subId];
  if (!trackedWallet) return;
  const hasSellWatches=Object.keys(sellWatchlist).length>0;
  if (!isActiveHours()&&!hasSellWatches) return;
  log(`[LOG HIT] wallet ${trackedWallet.substring(0,8)} | sig ${signature.substring(0,12)}...`);
  if (pendingSigs.has(signature)) { log(`[DEBOUNCE] ${signature.substring(0,12)} already being processed`); return; }
  pendingSigs.add(signature); setTimeout(()=>pendingSigs.delete(signature),30000);
  let tx=null;
  for (let attempt=0;attempt<3;attempt++) {
    tx=await getTransaction(signature); if(tx)break;
    log(`[RPC] getTransaction attempt ${attempt+1} failed, retrying...`);
    await new Promise(r=>setTimeout(r,2000));
  }
  if (!tx) { log(`[SKIP] Could not fetch tx ${signature.substring(0,12)}`); return; }
  if (hasSellWatches) await handlePotentialSell(trackedWallet,tx);
  if (!isActiveHours()) return;
  const mint=extractMint(tx);
  if (!mint) { log(`[SKIP] No token mint in tx for ${trackedWallet.substring(0,8)}`); return; }
  log(`[MINT] ${trackedWallet.substring(0,8)} bought ${mint.substring(0,8)}`);
  await handleWalletBuy(trackedWallet,mint);
}

let reqIdToWallet={};
function connect(useUrl) {
  const url=useUrl??(usingFallback?WSS_FALLBACK:WSS_PRIMARY);
  log(`[WS] Connecting to ${usingFallback?'FALLBACK':'PRIMARY'} endpoint...`);
  ws=new WebSocket(url,{handshakeTimeout:30000}); subIdToWallet={}; reqIdToWallet={}; wsReady=false;
  ws.on('open',()=>{
    log(`[WS] Connected — subscribing to ${WALLETS.length} wallets...`); wsReady=true; reconnectDelay=5000;
    WALLETS.forEach((wallet,i)=>{const reqId=i+1;reqIdToWallet[reqId]=wallet;ws.send(JSON.stringify({jsonrpc:'2.0',id:reqId,method:'logsSubscribe',params:[{mentions:[wallet]},{commitment:'confirmed'}]}));});
    log(`[WS] All ${WALLETS.length} subscription requests sent`);
    const pi=setInterval(()=>{if(ws.readyState===WebSocket.OPEN)ws.ping();else clearInterval(pi);},30000);
  });
  ws.on('message',(data)=>{
    let msg; try{msg=JSON.parse(data.toString());}catch{return;}
    if(msg.id!==undefined&&msg.result!==undefined&&typeof msg.result==='number'&&!msg.method){
      const wallet=reqIdToWallet[msg.id];
      if(wallet){subIdToWallet[msg.result]=wallet;const c=Object.keys(subIdToWallet).length;if(c%10===0)log(`[WS] ${c}/${WALLETS.length} subscriptions confirmed`);if(c===WALLETS.length)log(`[WS] ✅ All ${WALLETS.length} subscriptions active`);}
      return;
    }
    if(msg.method==='logsNotification'){processLogNotification(msg.params).catch(e=>log(`[ERR] processLogNotification: ${e.message}`));}
  });
  ws.on('error',(e)=>log(`[WS] Error: ${e.message}`));
  ws.on('close',(code)=>{
    wsReady=false; log(`[WS] Disconnected (code: ${code}). Reconnecting in ${reconnectDelay/1000}s...`);
    if(reconnectDelay>=30000&&!usingFallback&&WSS_PRIMARY!==WSS_FALLBACK){log(`[WS] Switching to fallback`);usingFallback=true;reconnectDelay=5000;}
    setTimeout(()=>connect(),reconnectDelay); reconnectDelay=Math.min(reconnectDelay*2,60000);
  });
}

http.createServer((req,res)=>{
  if(req.url==='/logs'){res.writeHead(200,{'Content-Type':'text/plain'});res.end(logBuffer.join('\n'));return;}
  res.writeHead(200,{'Content-Type':'text/plain'});
  res.end(`SOLANA FAST TRACKER (60s) — LIVE\nWS: ${wsReady?'connected':'reconnecting'}\nSubscriptions: ${Object.keys(subIdToWallet).length}/${WALLETS.length}\nFired alerts: ${firedAlerts.size}\nActive windows: ${Object.keys(activeAlerts).length}\nSell watchlist: ${Object.keys(sellWatchlist).length} token(s)\n\nHit /logs to see last 500 log lines\n`);
}).listen(process.env.PORT||3000,()=>log(`[HTTP] Health server on port ${process.env.PORT||3000}`));

log(`[START] Launching FAST tracker | ${WALLETS.length} wallets | 60s window | 60s max age | Active 11am-6pm ET`);
log(`[START] WSS primary: ${WSS_PRIMARY.replace(/api_key=[^&]+/,'api_key=***')}`);

https.get('https://api.ipify.org?format=json',(res)=>{let d='';res.on('data',c=>d+=c);res.on('end',()=>{try{log(`[IP] Outbound IP: ${JSON.parse(d).ip}`);}catch{log(`[IP] Could not parse IP`);}});}).on('error',(e)=>log(`[IP] IP check failed: ${e.message}`));

connect();

const RENDER_URL=process.env.RENDER_EXTERNAL_URL||null;
setInterval(()=>{
  if(!RENDER_URL)return;
  try{const mod=RENDER_URL.startsWith('https')?https:http;const req=mod.get(RENDER_URL+'/',(res)=>{log(`[PING] Self-ping OK (${res.statusCode})`);});req.on('error',(e)=>log(`[PING] Self-ping failed: ${e.message}`));req.setTimeout(10000,()=>req.destroy());}
  catch(e){log(`[PING] Self-ping error: ${e.message}`);}
},10*60*1000);
