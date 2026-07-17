// ============================================================
//  SOLANA COMBINED BOT
//  ----------------------------------------------------------
//  >>> VERSION: 2026-07-17b  (bluechip 2-tier + age-floor + $30k MC min; both signals) <<<
//  ----------------------------------------------------------
//  ONE ACTIVE SIGNAL:
//
//  BLUECHIP TRENDING BUY (CHAT_ID_SLOW)
//    Fires when ALL of the following are true:
//      1. A tracked wallet (not the token's dev) BUYS the token
//      2. The token is in the TOP 10 TRENDING on GMGN — in ANY interval
//         (1m / 5m / 1h / 6h / 24h). Top-10 in even one interval qualifies.
//      3. The token is UNDER 24 HOURS old
//      4. The token has OVER 10% BLUECHIP HOLDERS
//    Fires once per token.
//
//  CHANGE LOG:
//   2026-07-17b — COMPLETE SIGNAL REWRITE. Removed ALL previous signals:
//         Migration detection, Post-Migration Big Buy, 10-Wallet coordination,
//         and Large Buy Cluster. Replaced with the single Bluechip Trending Buy
//         above. Added a trending poller that pulls the top 10 from all five
//         intervals of GMGN /v1/market/rank every 30s and caches the union,
//         carrying each token's bluechip_owner_percentage and creation_timestamp.
//
//         NOTE: /v1/market/rank was previously believed dead ("returns empty for
//         my key") and was deleted as dead code in 24u. That was a FALSE NEGATIVE
//         — the response is DOUBLE-nested (data.data.rank), so a single-level
//         parse found nothing. The route works fine. Re-added correctly.
//
//   [Prior change log retained below for reference]
//   24q — extractSolSpent stopped ADDING native + wSOL legs (double-count ~2x).
//   24r — extractSolSpent PREFERS the wSOL swap leg, native delta as fallback.
//   24s — MARKET CAP FIX. GMGN /v1/token/info has no market_cap field and price
//         is nested (info.price.price). Added tokenPriceUsd/tokenSupply/
//         tokenMarketCap helpers; routed all price/MC reads through them.
// ============================================================

const https     = require('https');
const http      = require('http');
const fs        = require('fs');
const WebSocket = require('ws');

// ── CONFIG ────────────────────────────────────────────────────
const GMGN_API_KEY  = process.env.GMGN_API_KEY;
const SHYFT_API_KEY = process.env.SHYFT_API_KEY;
const HELIUS_API_KEY  = process.env.HELIUS_API_KEY;
const ALCHEMY_API_KEY = process.env.ALCHEMY_API_KEY;

const TELEGRAM_TOKEN      = process.env.TELEGRAM_TOKEN;
const CHAT_ID_FAST        = process.env.CHAT_ID_FAST        || '-5081620734';
const CHAT_ID_SLOW        = process.env.CHAT_ID_SLOW        || '-1003888330833';
const RENDER_URL          = process.env.RENDER_EXTERNAL_URL || '';

const SOL_MINT = 'So11111111111111111111111111111111111111112';

// ══════════════════════════════════════════════════════════════
//  BLUECHIP TRENDING SIGNAL CONFIG
// ══════════════════════════════════════════════════════════════
// Fires to CHAT_ID_SLOW when a tracked wallet buys a token that is
// (a) top-10 trending in ANY interval, (b) under 8h old, (c) >10% bluechip.
const TREND_SIGNAL_CHAT   = process.env.TREND_SIGNAL_CHAT || CHAT_ID_SLOW;
const TREND_MAX_TOKEN_AGE = parseInt(process.env.TREND_MAX_TOKEN_AGE || '86400', 10); // 24h in seconds
const TREND_MIN_BLUECHIP  = parseFloat(process.env.TREND_MIN_BLUECHIP || '0.10');     // 10% (0-1 scale)
const TREND_TOP_N         = parseInt(process.env.TREND_TOP_N || '10', 10);            // fetch depth (widest tier)
const TREND_TOP_TIGHT     = parseInt(process.env.TREND_TOP_TIGHT || '5', 10);         // tier-1 rank cutoff (top 5)
const TREND_TOP_WIDE      = parseInt(process.env.TREND_TOP_WIDE || '10', 10);         // tier-2 rank cutoff (top 10)
const TREND_BLUECHIP_HI   = parseFloat(process.env.TREND_BLUECHIP_HI || '0.20');      // tier-2 bluechip threshold (>20%)
const TREND_MIN_AGE       = parseInt(process.env.TREND_MIN_AGE || '60', 10);          // >= 60s old
const MC_MIN_USD          = parseFloat(process.env.MC_MIN_USD || '30000');            // >= $30k market cap (both signals)
const TREND_POLL_SECS     = parseInt(process.env.TREND_POLL_SECS || '30', 10);        // refresh every 30s
// All five intervals — a token counts as trending if it's top-10 in ANY of them.
const TREND_INTERVALS     = ['1m', '5m', '1h', '6h', '24h'];

// ══════════════════════════════════════════════════════════════
//  8-WALLET CLUSTER SIGNAL CONFIG
// ══════════════════════════════════════════════════════════════
// Fires to CHAT_ID_FAST when 8 DISTINCT tracked wallets buy the same token,
// and the token's age is between CLUSTER_MIN_AGE and CLUSTER_MAX_AGE.
// No trending or bluechip requirement. Any buy size. Fires once per token.
const CLUSTER_SIGNAL_CHAT = process.env.CLUSTER_SIGNAL_CHAT || CHAT_ID_FAST;
const CLUSTER_MIN_WALLETS = parseInt(process.env.CLUSTER_MIN_WALLETS || '7', 10);
const CLUSTER_MIN_AGE     = parseInt(process.env.CLUSTER_MIN_AGE || '60', 10);      // >= 60 seconds old
const CLUSTER_MAX_AGE     = parseInt(process.env.CLUSTER_MAX_AGE || '86400', 10);   // <= 24 hours old

// Bluechip trending signal now needs this many DISTINCT tracked wallets (was 1).
const TREND_MIN_WALLETS   = parseInt(process.env.TREND_MIN_WALLETS || '2', 10);
// Volume gate shared by the cluster signal: token qualifies if it is trending
// (top-N any interval) OR its latest 5-minute candle volume (USD) >= this.
const VOL_GATE_USD        = parseFloat(process.env.VOL_GATE_USD || '100000');   // $100k 5-min volume

// ── RPC ───────────────────────────────────────────────────────
const HTTP_RPCS = [
  HELIUS_API_KEY  ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : null,
  SHYFT_API_KEY   ? `https://rpc.shyft.to?api_key=${SHYFT_API_KEY}` : null,
  ALCHEMY_API_KEY ? `https://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}` : null,
  'https://api.mainnet-beta.solana.com',
].filter(Boolean);
// WebSocket endpoints, tried IN ORDER. On repeated failure the bot rotates to
// the next one. Helius free tier caps at 100 concurrent subscriptions and we run
// exactly 100 wallets — during a reconnect the old subs may still be open, which
// briefly doubles the count and trips a 429. Alchemy is a real second tier so we
// don't have to drop straight to the throttled public endpoint.
// Endpoint definitions. Which ones are used, and in what ORDER, is controlled by
// WSS_ORDER (comma-separated names). Default puts PUBLIC first because Helius free
// credits are exhausted (resets monthly) and Alchemy doesn't honor logsSubscribe.
// Once Helius credits reset, set WSS_ORDER=HELIUS,PUBLIC to prefer it again — no
// code change needed.
const WSS_DEFS = {
  HELIUS:  HELIUS_API_KEY  ? { name: 'HELIUS',  url: `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` }  : null,
  ALCHEMY: ALCHEMY_API_KEY ? { name: 'ALCHEMY', url: `wss://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}` } : null,
  SHYFT:   SHYFT_API_KEY   ? { name: 'SHYFT',   url: `wss://rpc.shyft.to?api_key=${SHYFT_API_KEY}` }              : null,
  PUBLIC:  { name: 'PUBLIC', url: 'wss://api.mainnet-beta.solana.com' },
};
const WSS_ORDER = (process.env.WSS_ORDER || 'PUBLIC,HELIUS,ALCHEMY')
  .split(',').map(s => s.trim().toUpperCase());
const WSS_ENDPOINTS = WSS_ORDER.map(n => WSS_DEFS[n]).filter(Boolean);
if (WSS_ENDPOINTS.length === 0) WSS_ENDPOINTS.push(WSS_DEFS.PUBLIC);

// ── FIRED ALERTS ──────────────────────────────────────────────
const FIRED_FILE = '/tmp/sol_trend_fired.json';

function loadSet(path) {
  try {
    if (fs.existsSync(path)) return new Set(JSON.parse(fs.readFileSync(path, 'utf8')));
  } catch(e) {}
  return new Set();
}

function saveSet(path, set) {
  try { fs.writeFileSync(path, JSON.stringify([...set]), 'utf8'); } catch(e) {}
}

// ── WALLETS ───────────────────────────────────────────────────
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
  "GpTXmkdvrTajqkzX1fBmC4BUjSboF9dHgfnqPqj8WAc4",
  "EaVboaPxFCYanjoNWdkxTbPvt57nhXGu5i6m9m6ZS2kK","FAicXNV5FVqtfbpn4Zccs71XcfGeyxBSGbqLDyDJZjke",
  "BAr5csYtpWoNpwhUjixX7ZPHXkUciFZzjBp9uNxZXJPh","B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC",
  "8HcYptCBAaPFWkmupiSAmysZ6Z8jB7N1c4YhVjhX7zbg","FFEjC9MHhpQViBPrD2iU6LmV2hEigyhLJaL7MZUZzyD4",
  "FTaSBuVj6w2S7XUa8fw19xrLy57DDr6kZDL6sxDXtvTP","FSAmbD6jm6SZZQadSJeC1paX3oTtAiY9hTx1UYzVoXqj",
  "G6fUXjMKPJzCY1rveAE6Qm7wy5U3vZgKDJmN1VPAdiZC","Ar2Y6o1QmrRAskjii1cRfijeKugHH13ycxW5cd7rro1x",
  "5aLY85pyxiuX3fd4RgM3Yc1e3MAL6b7UgaZz6MS3JUfG","DYAn4XpAkN5mhiXkRB7dGq4Jadnx6XYgu8L5b3WGhbrt",
  "7BNaxx6KdUYrjACNQZ9He26NBFoFxujQMAfNLnArLGH5",
  "BCnqsPEtA1TkgednYEebRpkmwFRJDCjMQcKZMMtEdArc","4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk",
  "5ZuV8eqkvzYFVEKbLvGBdexL2tFv7E5BCd2HZpjqbdg","FM1YCKED2KaqB8Uat8aB1nsffR1vezr7s6FAEieXJgke",
  "AV7PjXHL5JXZ1YoYRoN9Dsstg1x2UciBupMCXcJP8gUz","Dzp1SrZ474xwGp6ZEP6cNKo39u9zeXe1YAuTkyZyv3t4",
  "5FqUo9aBjsp7QeeyN6Vi2ZmF2fjS4H5EU7wnAQwPy17z","7hHmfYYR7L8LsCKk5akjtvVu1BbJRgHGJ2n6s7gbeKG4",
  "CjtqWn4toBbJ1feRZBDhz3TwBjbZm5RpES8rvKWTuNtk","FAX4qRQdiSj2iWDYvkJ21VieVCXGREtwMhEyAHSJ1aqp",
  "9VXuNqqqzniYYW3fRDeaCtUUtqWsEeWWn5umh3aF9h17","DAEdBmTPEKM6xkwfzC3d411QUe6coKpkND6UURa4CvHC",
  "iPUp3qkm39ycMGbywWFMUyvaDhiiPGXeWXaDtmHNe6C","CfkaAru9ArJ2tAStYHvbAyRBJL3EhDzsWYV2KYg9shxB",
  "EeLjBXRELqrcWAXbnj8T4jQPS9Qh7UGWiKxovsJ36pZY","H5Wh4EDvWQT4mShH746V5VDqxHQkaQZyPWfuhy1PRVBg",
  "GH9yk8vgFvHnAD8JZqXxr3hBN1Lr1mJ9NPzrP5mVqiJe","AQdBYZNy3BZ1vouGUjA1w9Ay7aq7kH5UQSuh4LQWKotY",
  "HTM87R4mgjDdiF6Yfn8duK9vbDmZxiPCTRbGvm7eCAJY","8i5U2uNBEuTc4zskYP14zbebDg2RSwrrG8REhEnJb97K",
  "7E9jfxCczubz4FXkkVKzUMHXGwzJxyppC4m7y3ew8ATg","8v6ztxZwhPBNmA6aGrBzzrt6UBf2fZZfsWqZ9Lt47Kpv",
  "6nU2L7MQVUWjtdKHVpuZA9aind73nd3rXC4YFo8KQCy4","5zCkbcD74hFPeBHwYdwJLJAoLVgHX45AFeR7RzC8vFiD",
  "8HeDT75s5g4CtCimH5B5nySqCiQhtWii8UnZhxBtFo38","A8Z1ejQGk45EJibBPJviWnM3UvwKSuYun53nSCkWKM52",
  "D9gQ6RhKEpnobPBUdWY5bPQt2p3zGk3iVz6ChpUi2ArA","BZC7VEj5Y9Ege3cTRGBZW2zW7pjw3hpiSkcAoYKysvue",
  "EFaQQTGywnD4CjQQvTugUiyVT4LV9G6MsWqiub8X6unN",
  "HUgpmqL6r4Z4iEZiVuNZ6J6QnAsSZpsL8giVyVtz3QhT","FaBGrHWjcJ8vKnbgUtsdpZjvF7YAAajtQTWmmEHiKtQr",
  "HYWo71Wk9PNDe5sBaRKazPnVyGnQDiwgXCFKvgAQ1ENp","bwamJzztZsepfkteWRChggmXuiiCQvpLqPietdNfSXa",
  "7moqFjvm2MwAiMtCZoqYoTAPzRBxxMRT2ddyHThQuWjr",
  "DjM7Tu7whh6P3pGVBfDzwXAx2zaw51GJWrJE3PwtuN7s",
  "AvcWA3ngM55sSpjh1FZthmqA7V6BHo4f555a8w3Wv3ij",
  "6ujZxnphRxTqveaQtLAQHFoWz16xhLWZbTijcgZN4fRp",
  "nazikTJezTC3W2fxXE3wzs495PYzXMiq5o7co6YYACA",
  "BtMBMPkoNbnLF9Xn552guQq528KKXcsNBNNBre3oaQtr",
  "EYfdt8cNFyyTEJKp18dcoVbgUHDnM1SK3bT2uKj9XXHc",
  "2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f", // Cupsey
  "CtPxvpWo1pk7HtL6KwpCLMMdsXHC6fdqAN1bPiracaQq", // STINKDEX Dev
  "Bi4rd5FH5bYEN8scZ7wevxNZyNmKHdaBcvewdPFxYdLt", // Theo
  "6TbDFs2dkHETrRWVbheiC11bwg7EWLDgszsCADF1ML1b", // Notable 1
  "3dhwViJnxKhRJcJJznrVt6oYkuD1bULvsUXscuxpNBDs", // Notable 2
  "5Pr7D2d5WUM7j8fMF36DuzVDDGEHLtYsF7a6ezyzFG19", // Notable 3
  "GdRSPexhxbQz5H2zFQrNN2BAZUqEjAULBigTPvQ6oDMP", // NNC Dev
  "CEUA7zVoDRqRYoeHTP58UHU6TR8yvtVbeLrX1dppqoXJ", // Notable 13
  "yHCxHBEaJW5tbndqC8JciSThr7U1cqLpdcsvHcx6PRe", // Ansem Dev
  "PMJA8UQDyWTFw2Smhyp9jGA6aTaP7jKHR7BPudrgyYN", // Notable 7
  "ardinRsN1mNYVeoJWTBsWeYeXvuR9UUDGMsCDKpb6AT", // trunoest
  "8NJ7Ujpji8uMF2675mqaTSEm2DCbfJA7fiRKtiaqkaLN", // Nikita
  "6HJetMbdHBuk3mLUainxAPpBpWzDgYbHGTS2TqDAUSX2", // ljc
  "CCCCQCrL6zVjnDeucDzcxJgxAs5ahNmrhw1CDexPhqrd", // GhostTrader
  "yMBRVpuVm7bgASPEvEhVtKTbz4g4UhNFEDz8kBmHAv1", // Notable 16
  "HmBmSYwYEgEZuBUYuDs9xofyqBAkw4ywugB1d7R7sTGh", // tobx
];
const WALLET_SET = new Set(WALLETS);

// Wallet name lookup — all known names
const WALLET_NAMES = {
  "CzbN6T1gKkKutvuPXcxNmV8FLqzjsDWebWmg9o8e2ZbU": "Income Dev",
  "HiSo5kykqDPs3EG14Fk9QY4B5RvkuEs8oJTiqPX3EDAn": "CL1 Dev",
  "8ZN71XTdVo8yRovnGLmNgW3Tgniw6A4J3JGLvPD686FP": "nate91 Dev",
  "DPNPVvoGdwNBY849ryx2JZzakWuWbDTfSUYr8aNfKLwA": "Life Dev",
  "Hp34goKgAhAYW6sw9iFAZofvDTr3DAhtkSKF1R9bAk2P": "Machi Dev",
  "95ZCf3jKMHeFYvPXVZW3Ek6AEPDyjebosqnc7eNioVMo": "Win Dev",
  "FSAmbD6jm6SZZQadSJeC1paX3oTtAiY9hTx1UYzVoXqj": "Z(BIOLLM Dev)",
  "7moqFjvm2MwAiMtCZoqYoTAPzRBxxMRT2ddyHThQuWjr": "Smart 15",
  "DjM7Tu7whh6P3pGVBfDzwXAx2zaw51GJWrJE3PwtuN7s": "CHILLHOUSE Dev",
  "AvcWA3ngM55sSpjh1FZthmqA7V6BHo4f555a8w3Wv3ij": "Honeypot Dev",
  "6ujZxnphRxTqveaQtLAQHFoWz16xhLWZbTijcgZN4fRp": "BadBunny Dev",
  "nazikTJezTC3W2fxXE3wzs495PYzXMiq5o7co6YYACA": "YZY Dev",
  "BtMBMPkoNbnLF9Xn552guQq528KKXcsNBNNBre3oaQtr": "Letterbomb(horse)",
  "EYfdt8cNFyyTEJKp18dcoVbgUHDnM1SK3bT2uKj9XXHc": "Penguin Dev",
  "2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f": "Cupsey",
  "CtPxvpWo1pk7HtL6KwpCLMMdsXHC6fdqAN1bPiracaQq": "STINKDEX Dev",
  "H8s4GoDcABkvykQSS7mUSHTSKUcxivoULUXgZDkjuoUf": "Elon Dev",
  "AmNMqM5VbPwtG14gLBdtrqZpQrhSzavLkQPufS8CQ7LB": "VDKH Dev",
  "AMRsSeU5JpqwQWJGNLMpZzRCZSFEwYQYbMnms3dD4311": "Nothing Dev",
  "2bBRwhGoL4fRZk6g8NnhBZywsF8PdLJnBRfWDCEMogD2": "Maga Dev",
  "Aqje5DsN4u2PHmQxGF9PKfpsDGwQRCBhWeLKHCFhSMXk": "Eva Dev",
  "JDQKDrc1TQgBRvdFh56tkta5sYcDj1SoP52Eiu64rSrT": "ECC Dev",
  "HyYNVYmnFmi87NsQqWzLJhUTPBKQUfgfhdbBa554nMFF": "Fartcoin Dev",
  "GeUnv1jmtviRbR7Gu1JnXSGkUMUgFVBHuEVQVpTaUX1W": "Nothing Dev",
  "78N177fzNJpp8pG49xDv1efYcTMSzo9tPTKEA9mAVkh2": "Sheep",
  "DAEdBmTPEKM6xkwfzC3d411QUe6coKpkND6UURa4CvHC": "Coinbase Dev",
  "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o": "Cented 7",
  "HYWo71Wk9PNDe5sBaRKazPnVyGnQDiwgXCFKvgAQ1ENp": "Pigeon Dev",
  "FaBGrHWjcJ8vKnbgUtsdpZjvF7YAAajtQTWmmEHiKtQr": "Dale Dev",
  "HUgpmqL6r4Z4iEZiVuNZ6J6QnAsSZpsL8giVyVtz3QhT": "Sparkles Dev",
  "EFaQQTGywnD4CjQQvTugUiyVT4LV9G6MsWqiub8X6unN": "Bob Dev",
  "BZC7VEj5Y9Ege3cTRGBZW2zW7pjw3hpiSkcAoYKysvue": "Unipcs Dev",
  "D9gQ6RhKEpnobPBUdWY5bPQt2p3zGk3iVz6ChpUi2ArA": "Imagine Dev",
  "A8Z1ejQGk45EJibBPJviWnM3UvwKSuYun53nSCkWKM52": "Punch Dev",
  "8HeDT75s5g4CtCimH5B5nySqCiQhtWii8UnZhxBtFo38": "Lobstar Dev",
  "5zCkbcD74hFPeBHwYdwJLJAoLVgHX45AFeR7RzC8vFiD": "Charlie",
  "6nU2L7MQVUWjtdKHVpuZA9aind73nd3rXC4YFo8KQCy4": "VVM Dev",
  "8v6ztxZwhPBNmA6aGrBzzrt6UBf2fZZfsWqZ9Lt47Kpv": "Lmeow Dev",
  "7E9jfxCczubz4FXkkVKzUMHXGwzJxyppC4m7y3ew8ATg": "Mia Dev",
  "8i5U2uNBEuTc4zskYP14zbebDg2RSwrrG8REhEnJb97K": "Memeless Dev",
  "HTM87R4mgjDdiF6Yfn8duK9vbDmZxiPCTRbGvm7eCAJY": "Priceless Dev",
  "AQdBYZNy3BZ1vouGUjA1w9Ay7aq7kH5UQSuh4LQWKotY": "Pfp Dev",
  "GH9yk8vgFvHnAD8JZqXxr3hBN1Lr1mJ9NPzrP5mVqiJe": "Eagy",
  "4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk": "Jijo",
  "8deJ9xeUvXSJwicYptA9mHsU2rN2pDx37KWzkDkEXhU6": "Cooker",
  "H5Wh4EDvWQT4mShH746V5VDqxHQkaQZyPWfuhy1PRVBg": "Bonkyo Dev",
  "EeLjBXRELqrcWAXbnj8T4jQPS9Qh7UGWiKxovsJ36pZY": "LLM Dev",
  "CfkaAru9ArJ2tAStYHvbAyRBJL3EhDzsWYV2KYg9shxB": "67 Dev",
  "bwamJzztZsepfkteWRChggmXuiiCQvpLqPietdNfSXa": "Copper Inu Dev",
  "DYAn4XpAkN5mhiXkRB7dGq4Jadnx6XYgu8L5b3WGhbrt": "Doc",
  "6EDaVsS6enYgJ81tmhEkiKFcb4HuzPUVFZeom6PHUqN3": "Cowboy",
  "FxN3VZ4BosL5urG2yoeQ156JSdmavm9K5fdLxjkPmaMR": "Track 15",
  "G7NvZKjoVqBDWciSYtWWgUPB7DA1iJavdvH5jty2FAmM": "America Dev",
  "BCagckXeMChUKrHEd6fKFA1uiWDtcmCXMsqaheLiUPJd": "DV",
  "4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9": "Decu",
  "2T5NgDDidkvhJQg8AHDi74uCFwgp25pYFMRZXBaCUNBH": "idontpaytaxes",
  "515vh1DrPuwMATt9Zoq9kP4sJL9fyojA1dHJu4DQpNRp": "crypto",
  "GpTXmkdvrTajqkzX1fBmC4BUjSboF9dHgfnqPqj8WAc4": "Track 5",
  "EaVboaPxFCYanjoNWdkxTbPvt57nhXGu5i6m9m6ZS2kK": "Danny",
  "FAicXNV5FVqtfbpn4Zccs71XcfGeyxBSGbqLDyDJZjke": "Radiance",
  "BAr5csYtpWoNpwhUjixX7ZPHXkUciFZzjBp9uNxZXJPh": "Jack Duval",
  "B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC": "Track 35",
  "8HcYptCBAaPFWkmupiSAmysZ6Z8jB7N1c4YhVjhX7zbg": "Smart 1",
  "FFEjC9MHhpQViBPrD2iU6LmV2hEigyhLJaL7MZUZzyD4": "Smart 2",
  "FTaSBuVj6w2S7XUa8fw19xrLy57DDr6kZDL6sxDXtvTP": "Smart 5",
  "G6fUXjMKPJzCY1rveAE6Qm7wy5U3vZgKDJmN1VPAdiZC": "Clukz",
  "Bi4rd5FH5bYEN8scZ7wevxNZyNmKHdaBcvewdPFxYdLt": "Theo",
  "Ar2Y6o1QmrRAskjii1cRfijeKugHH13ycxW5cd7rro1x": "Track 12",
  "5aLY85pyxiuX3fd4RgM3Yc1e3MAL6b7UgaZz6MS3JUfG": "Track 9",
  "7BNaxx6KdUYrjACNQZ9He26NBFoFxujQMAfNLnArLGH5": "Track 13",
  "BCnqsPEtA1TkgednYEebRpkmwFRJDCjMQcKZMMtEdArc": "Kreo",
  "5ZuV8eqkvzYFVEKbLvGBdexL2tFv7E5BCd2HZpjqbdg": "Doji",
  "FM1YCKED2KaqB8Uat8aB1nsffR1vezr7s6FAEieXJgke": "Pom Dev",
  "AV7PjXHL5JXZ1YoYRoN9Dsstg1x2UciBupMCXcJP8gUz": "Butthole Dev",
  "Dzp1SrZ474xwGp6ZEP6cNKo39u9zeXe1YAuTkyZyv3t4": "Distorted Dev",
  "5FqUo9aBjsp7QeeyN6Vi2ZmF2fjS4H5EU7wnAQwPy17z": "Aloka Dev",
  "7hHmfYYR7L8LsCKk5akjtvVu1BbJRgHGJ2n6s7gbeKG4": "Goldcoin",
  "CjtqWn4toBbJ1feRZBDhz3TwBjbZm5RpES8rvKWTuNtk": "Vibecodoor",
  "FAX4qRQdiSj2iWDYvkJ21VieVCXGREtwMhEyAHSJ1aqp": "Petah Dev",
  "9VXuNqqqzniYYW3fRDeaCtUUtqWsEeWWn5umh3aF9h17": "Cancer Dev",
  "iPUp3qkm39ycMGbywWFMUyvaDhiiPGXeWXaDtmHNe6C": "Runner Dev",
  "6TbDFs2dkHETrRWVbheiC11bwg7EWLDgszsCADF1ML1b": "Notable 1",
  "3dhwViJnxKhRJcJJznrVt6oYkuD1bULvsUXscuxpNBDs": "Notable 2",
  "5Pr7D2d5WUM7j8fMF36DuzVDDGEHLtYsF7a6ezyzFG19": "Notable 3",
  "GdRSPexhxbQz5H2zFQrNN2BAZUqEjAULBigTPvQ6oDMP": "NNC Dev",
  "CEUA7zVoDRqRYoeHTP58UHU6TR8yvtVbeLrX1dppqoXJ": "Notable 13",
  "yHCxHBEaJW5tbndqC8JciSThr7U1cqLpdcsvHcx6PRe": "Ansem Dev",
  "PMJA8UQDyWTFw2Smhyp9jGA6aTaP7jKHR7BPudrgyYN": "Notable 7",
  "ardinRsN1mNYVeoJWTBsWeYeXvuR9UUDGMsCDKpb6AT": "trunoest",
  "8NJ7Ujpji8uMF2675mqaTSEm2DCbfJA7fiRKtiaqkaLN": "Nikita",
  "6HJetMbdHBuk3mLUainxAPpBpWzDgYbHGTS2TqDAUSX2": "ljc",
  "CCCCQCrL6zVjnDeucDzcxJgxAs5ahNmrhw1CDexPhqrd": "GhostTrader",
  "yMBRVpuVm7bgASPEvEhVtKTbz4g4UhNFEDz8kBmHAv1": "Notable 16",
  "HmBmSYwYEgEZuBUYuDs9xofyqBAkw4ywugB1d7R7sTGh": "tobx",
};

function walletName(addr) {
  return WALLET_NAMES[addr] ?? addr.substring(0, 8) + '...';
}

// ── STATE ─────────────────────────────────────────────────────
let trendFired      = loadSet(FIRED_FILE);   // tokens that already fired — one alert per token
let trendBuyers     = {};         // mint -> Set of distinct tracked wallets that bought it (for TREND_MIN_WALLETS gate)
let tokenInfoCache  = {};
let tokenInfoInflight = {};
let devWalletCache  = {};
let pendingSigs     = new Set();
let seenPairs       = new Set();  // "wallet:mint" — only the first buy per wallet+token matters
let clusterBuyers   = {};         // mint -> Set of distinct tracked wallets that bought it (today)
let clusterFired    = loadSet('/tmp/sol_cluster_fired.json');  // tokens that already fired the cluster signal
let walletEventTimes = {};        // wallet -> recent event timestamps (ms), flood throttle
const processing    = new Set();  // synchronous guard against duplicate concurrent signals

// ── TRENDING CACHE ────────────────────────────────────────────
// address -> { symbol, bluechip, created, mc, ath, volume, holders,
//              smart, kol, intervals:Set }
// Rebuilt every TREND_POLL_SECS from the top-N of all five intervals.
let trendingMap  = new Map();
let trendLastOk  = 0;   // unix secs of the last successful refresh
let trendRefreshes = 0;

// ── WS STATE ──────────────────────────────────────────────────
let ws             = null;
let wsReady        = false;
let reconnectDelay = 5000;
let wssIndex       = 0;   // index into WSS_ENDPOINTS
let subIdToWallet  = {};
let reqIdToWallet  = {};
let lastMessageAt  = Date.now();

// ── LOG ───────────────────────────────────────────────────────
const LOG_MAX_LINES = 500;
let logBuffer = [];

function log(msg) {
  const t = new Date().toLocaleTimeString('en-US', {
    timeZone: 'America/Toronto', hour12: true,
    hour: '2-digit', minute: '2-digit', second: '2-digit'
  });
  const line = `[${t}] ${msg}`;
  console.log(line);
  logBuffer.push(line);
  if (logBuffer.length > LOG_MAX_LINES) logBuffer.shift();
}

function isActiveHours() {
  return true;  // 24/7
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function fmtUsd(n) {
  if (!n || isNaN(n)) return 'N/A';
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `$${Math.round(n).toLocaleString()}`;
  return `$${n.toFixed(2)}`;
}

function fmtAge(secs) {
  if (secs == null || isNaN(secs)) return 'N/A';
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs/60)}m`;
  const h = Math.floor(secs/3600), m = Math.floor((secs%3600)/60);
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

// ── PRICE / MARKET CAP READERS ────────────────────────────────
// GMGN's /v1/token/info does NOT return market_cap, and price is a NESTED object
// (info.price.price, a string). These helpers are the single correct way to read
// price and MC from a token/info object anywhere in the bot.
function tokenPriceUsd(info) {
  if (!info) return 0;
  if (info.price && typeof info.price === 'object') {
    const p = parseFloat(info.price.price ?? 0);
    return p > 0 ? p : 0;
  }
  const flat = parseFloat(info.price ?? 0);
  return flat > 0 ? flat : 0;
}

function tokenSupply(info) {
  if (!info) return 0;
  const s = parseFloat(info.circulating_supply ?? info.total_supply ?? 0);
  return s > 0 ? s : 0;
}

function tokenMarketCap(info) {
  const price = tokenPriceUsd(info);
  const supply = tokenSupply(info);
  return (price > 0 && supply > 0) ? price * supply : 0;
}

// ── HTTP ──────────────────────────────────────────────────────
function httpsGet(hostname, path, headers = {}) {
  return new Promise((resolve) => {
    const req = https.request({ hostname, path, method: 'GET', headers }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode !== 200) { resolve(null); return; }
        try { resolve(JSON.parse(data)); } catch { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(15000, () => { req.destroy(); resolve(null); });
    req.end();
  });
}

function httpsPost(url, body) {
  return new Promise((resolve) => {
    const payload = JSON.stringify(body);
    const u = new URL(url);
    const req = https.request({
      hostname: u.hostname, path: u.pathname + u.search, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) },
    }, (res) => {
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
  for (const rpc of HTTP_RPCS) {
    const r = await httpsPost(rpc, {
      jsonrpc: '2.0', id: 1, method: 'getTransaction',
      params: [signature, { encoding: 'json', maxSupportedTransactionVersion: 0, commitment: 'confirmed' }]
    });
    if (r?.result) return r.result;
  }
  return null;
}

// ── GMGN ──────────────────────────────────────────────────────
// Auth requires ALL of: X-APIKEY header, a browser User-Agent (GMGN 403s the
// default Node/Python UA), plus timestamp + client_id query params.
async function gmgnGet(path, params = {}, skipAuth = false) {
  if (!skipAuth) {
    params.timestamp = Math.floor(Date.now() / 1000).toString();
    params.client_id = Math.random().toString(36).substring(2) + Date.now().toString(36);
  }
  const query = new URLSearchParams(params).toString();
  const headers = {
    'X-APIKEY': GMGN_API_KEY,
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  };
  const fullPath = query ? `${path}?${query}` : path;
  const parsed = await httpsGet('openapi.gmgn.ai', fullPath, headers);
  if (parsed?.code === 0 && parsed?.data) return parsed.data;
  if (parsed && !parsed.code && !parsed.error) return parsed;
  return null;
}

async function fetchTokenInfo(mint) {
  return await gmgnGet('/v1/token/info', { chain: 'sol', address: mint });
}

// Latest 5-minute candle volume (USD) for any token, via the kline endpoint.
// GMGN's token_kline needs MILLISECOND timestamps (from/to in ms) — seconds
// silently returns an empty list. Each candle carries a USD `volume` field.
// Returns the most recent COMPLETE candle's volume, or 0 on any failure.
// Cached ~60s per token to avoid hammering the endpoint on every buy.
let vol5mCache = {};
async function gmgn5mVolumeUsd(mint) {
  const cached = vol5mCache[mint];
  if (cached && (Date.now() - cached.at) < 60000) return cached.v;
  try {
    const nowMs = Date.now();
    const data = await gmgnGet('/v1/market/token_kline', {
      chain: 'sol', address: mint, resolution: '5m',
      from: String(nowMs - 3600000), to: String(nowMs),
    });
    const list = data?.list || [];
    let v = 0;
    if (Array.isArray(list) && list.length) {
      // Use the last fully-formed candle. The final element may be the current
      // in-progress 5m bar; prefer the second-to-last if we have >= 2.
      const idx = list.length >= 2 ? list.length - 2 : list.length - 1;
      v = parseFloat(list[idx]?.volume ?? 0) || 0;
    }
    vol5mCache[mint] = { v, at: Date.now() };
    return v;
  } catch (e) {
    return 0;
  }
}

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

// ══════════════════════════════════════════════════════════════
//  TRENDING POLLER  — /v1/market/rank
// ══════════════════════════════════════════════════════════════
// Pulls the top TREND_TOP_N tokens for EACH of the five intervals and builds a
// union map. A token is "trending" if it is top-N in ANY interval.
//
// IMPORTANT — response shape: /v1/market/rank is DOUBLE-nested. The raw body is
//   { code:0, data: { code:0, data: { rank: [ ... ] } } }
// gmgnGet() unwraps one layer, so the rank array is at data.data.rank. A single
// -level parse (data.rank) finds nothing and looks like "the endpoint returns
// empty" — that false negative is why this route was previously written off as
// dead. We read data.data.rank and fall back to data.rank just in case.
function extractRank(data) {
  if (!data) return [];
  if (Array.isArray(data?.data?.rank)) return data.data.rank;   // real shape
  if (Array.isArray(data?.rank))       return data.rank;        // tolerate flat
  if (Array.isArray(data))             return data;
  return [];
}

async function fetchTrendingInterval(interval) {
  const data = await gmgnGet('/v1/market/rank', {
    chain: 'sol',
    interval,
    order_by: 'volume',
    direction: 'desc',
    limit: String(TREND_TOP_N),
  });
  const rank = extractRank(data);
  return rank.slice(0, TREND_TOP_N);
}

async function refreshTrending() {
  try {
    const next = new Map();
    let okIntervals = 0;

    for (const interval of TREND_INTERVALS) {
      const rows = await fetchTrendingInterval(interval);
      if (rows.length) okIntervals++;
      for (let ri = 0; ri < rows.length; ri++) {
        const t = rows[ri];
        const addr = t.address;
        if (!addr) continue;
        const rankPos = ri + 1;   // 1-based rank within this interval
        const bluechip = parseFloat(t.bluechip_owner_percentage ?? 0) || 0;
        const created  = parseInt(t.creation_timestamp ?? 0, 10) || 0;
        const existing = next.get(addr);
        if (existing) {
          existing.intervals.add(interval);
          if (rankPos < existing.bestRank) existing.bestRank = rankPos;
          if (bluechip > existing.bluechip) existing.bluechip = bluechip;  // keep highest bluechip seen
        } else {
          next.set(addr, {
            symbol:   t.symbol ?? 'UNKNOWN',
            bluechip,                                        // 0-1 scale (highest across intervals)
            bestRank: rankPos,                               // best (lowest) rank across intervals
            created,                                         // unix secs
            mc:       parseFloat(t.market_cap ?? 0) || 0,
            ath:      parseFloat(t.history_highest_market_cap ?? 0) || 0,
            volume:   parseFloat(t.volume ?? 0) || 0,
            holders:  parseInt(t.holder_count ?? 0, 10) || 0,
            smart:    parseInt(t.smart_degen_count ?? 0, 10) || 0,
            kol:      parseInt(t.renowned_count ?? 0, 10) || 0,
            liquidity: parseFloat(t.liquidity ?? 0) || 0,
            intervals: new Set([interval]),
          });
        }
      }
      await sleep(150);  // gentle pacing — rank is weight 1, 5 calls is cheap
    }

    if (next.size > 0) {
      trendingMap = next;
      trendLastOk = Math.floor(Date.now() / 1000);
      trendRefreshes++;
      // Log how many of the trending tokens would actually qualify, so it's
      // obvious at a glance whether the thresholds are ever satisfiable.
      const now = Math.floor(Date.now() / 1000);
      let eligible = 0;
      for (const [, v] of next) {
        const age = v.created > 0 ? now - v.created : Infinity;
        if (age <= TREND_MAX_TOKEN_AGE && v.bluechip > TREND_MIN_BLUECHIP) eligible++;
      }
      if (trendRefreshes % 10 === 1) {  // every ~5 min, not every 30s
        log(`[TREND] refreshed: ${next.size} unique tokens across ${okIntervals}/${TREND_INTERVALS.length} intervals | ${eligible} currently meet age<${TREND_MAX_TOKEN_AGE/3600}h + bluechip>${(TREND_MIN_BLUECHIP*100).toFixed(0)}%`);
      }
    } else {
      log(`[TREND] ⚠️ refresh returned NO tokens across all intervals — check GMGN auth / rate limit`);
    }
  } catch (e) {
    log(`[ERR] refreshTrending: ${e.message}`);
  }
}

// ══════════════════════════════════════════════════════════════
//  BLUECHIP TRENDING SIGNAL
// ══════════════════════════════════════════════════════════════
// A tracked wallet (not the dev) bought `tokenMint`. Fire if the token is
// top-10 trending in ANY interval, under 8h old, and >10% bluechip holders.
async function sendTrendSignal(trackedWallet, tokenMint, tx) {
  try {
    if (trendFired.has(tokenMint) || processing.has(tokenMint)) return;

    // GATE 1: must be in the trending set (top-N of any interval).
    const t = trendingMap.get(tokenMint);
    if (!t) return;   // not trending — silent, this is the common case

    // GATE 1b: require TREND_MIN_WALLETS distinct tracked wallets to have bought.
    if (!trendBuyers[tokenMint]) trendBuyers[tokenMint] = new Set();
    trendBuyers[tokenMint].add(trackedWallet);
    if (trendBuyers[tokenMint].size < TREND_MIN_WALLETS) {
      log(`[TREND] ${t.symbol} — ${trendBuyers[tokenMint].size}/${TREND_MIN_WALLETS} wallets`);
      return;
    }

    const now = Math.floor(Date.now() / 1000);

    // GATE 2: token age < 8h. Prefer the trending row's creation_timestamp;
    // fall back to token/info if the rank row didn't carry one.
    let created = t.created;
    if (!(created > 0)) {
      const info = await getCachedTokenInfo(tokenMint);
      created = parseInt(info?.creation_timestamp ?? 0, 10) || 0;
    }
    if (!(created > 0)) {
      log(`[TREND] ${t.symbol} ${tokenMint.substring(0,8)} — no creation timestamp, can't check age; skipping`);
      return;
    }
    const age = now - created;
    // GATE 2: age must be within 60s .. 24h.
    if (age < TREND_MIN_AGE) {
      log(`[TREND] SKIP ${t.symbol} — too new (${age}s < ${TREND_MIN_AGE}s)`);
      return;
    }
    if (age > TREND_MAX_TOKEN_AGE) {
      log(`[TREND] SKIP ${t.symbol} — age ${fmtAge(age)} > ${TREND_MAX_TOKEN_AGE/3600}h`);
      return;
    }

    // GATE 3: two-tier trending + bluechip.
    //   Tier 1: rank <= top-5  AND bluechip > 10%
    //   Tier 2: rank <= top-10 AND bluechip > 20%
    const rank = t.bestRank || 999;
    const tier1 = (rank <= TREND_TOP_TIGHT) && (t.bluechip > TREND_MIN_BLUECHIP);
    const tier2 = (rank <= TREND_TOP_WIDE)  && (t.bluechip > TREND_BLUECHIP_HI);
    if (!tier1 && !tier2) {
      log(`[TREND] SKIP ${t.symbol} — rank ${rank}, bluechip ${(t.bluechip*100).toFixed(1)}% (need top${TREND_TOP_TIGHT}+>${(TREND_MIN_BLUECHIP*100).toFixed(0)}% or top${TREND_TOP_WIDE}+>${(TREND_BLUECHIP_HI*100).toFixed(0)}%)`);
      return;
    }
    const tierReason = tier1 ? `top${TREND_TOP_TIGHT} +${(t.bluechip*100).toFixed(1)}%bc` : `top${TREND_TOP_WIDE} +${(t.bluechip*100).toFixed(1)}%bc`;

    // GATE 3b: market cap >= $30k.
    const info = await getCachedTokenInfo(tokenMint);
    let _mcCheck = tokenMarketCap(info);
    if (!(_mcCheck > 0)) _mcCheck = t.mc;
    if (!(_mcCheck >= MC_MIN_USD)) {
      log(`[TREND] SKIP ${t.symbol} — MC ${fmtUsd(_mcCheck)} < ${fmtUsd(MC_MIN_USD)}`);
      return;
    }

    // GATE 4: not the token's dev.
    const devFromCache = (devWalletCache[tokenMint] && devWalletCache[tokenMint] !== 'unknown') ? devWalletCache[tokenMint] : null;
    const devFromInfo = info?.dev?.creator_address ?? null;
    if ((devFromCache && trackedWallet === devFromCache) || (devFromInfo && trackedWallet === devFromInfo)) {
      log(`[TREND] SKIP ${t.symbol} — buyer is the dev`);
      return;
    }

    // All gates passed — fire once.
    if (trendFired.has(tokenMint) || processing.has(tokenMint)) return;
    processing.add(tokenMint);            // synchronous — no await between check and add
    trendFired.add(tokenMint);
    saveSet(FIRED_FILE, trendFired);

    // Display fields. Prefer live token/info MC; fall back to the rank row's.
    const symbol = info?.symbol ?? t.symbol ?? 'UNKNOWN';
    let mc = tokenMarketCap(info);
    if (!(mc > 0)) mc = t.mc;
    const mcStr  = mc > 0 ? fmtUsd(mc) : 'N/A';
    const athStr = t.ath > 0 ? fmtUsd(t.ath) : 'N/A';
    const liqStr = t.liquidity > 0 ? fmtUsd(t.liquidity) : 'N/A';
    const volStr = t.volume > 0 ? fmtUsd(t.volume) : 'N/A';
    const intervalsStr = [...t.intervals].join(', ');
    const signalTime = new Date().toLocaleTimeString('en-US', {
      timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true
    });

    sendTelegram(TREND_SIGNAL_CHAT,
      `💎 <b>Bluechip Trending Buy — ${(t.bluechip*100).toFixed(1)}% bluechip</b>\n\n` +
      `Token: #${symbol}\n` +
      `Contract: <code>${tokenMint}</code>\n` +
      `Bought by: ${walletName(trackedWallet)}\n\n` +
      `Bluechip Holders: <b>${(t.bluechip*100).toFixed(1)}%</b>\n` +
      `Token Age: ${fmtAge(age)}\n` +
      `Market Cap: ${mcStr}\n` +
      `ATH MC: ${athStr}\n` +
      `Liquidity: ${liqStr}\n` +
      `Volume: ${volStr}\n` +
      `Holders: ${t.holders || 'N/A'}\n` +
      `Smart / KOL: ${t.smart} / ${t.kol}\n` +
      `Trending in: ${intervalsStr}\n\n` +
      `Signal Time: ${signalTime}\n\n` +
      `🔗 <a href="https://gmgn.ai/sol/token/${tokenMint}">View on GMGN</a>`
    );
    log(`[TREND] 🔥 FIRED #${symbol} — ${walletName(trackedWallet)} bought | bluechip ${(t.bluechip*100).toFixed(1)}% | age ${fmtAge(age)} | trending [${intervalsStr}]`);
    processing.delete(tokenMint);
  } catch (e) {
    processing.delete(tokenMint);
    log(`[ERR] sendTrendSignal: ${e.message}`);
  }
}

// ══════════════════════════════════════════════════════════════
//  8-WALLET CLUSTER SIGNAL
// ══════════════════════════════════════════════════════════════
// Called after each detected buy. Tracks how many DISTINCT tracked wallets have
// bought this token; when that reaches CLUSTER_MIN_WALLETS and the token age is
// within [CLUSTER_MIN_AGE, CLUSTER_MAX_AGE], fire once to CLUSTER_SIGNAL_CHAT.
async function checkClusterSignal(trackedWallet, tokenMint) {
  try {
    if (clusterFired.has(tokenMint)) return;

    // Record this wallet as a buyer of the token.
    if (!clusterBuyers[tokenMint]) clusterBuyers[tokenMint] = new Set();
    clusterBuyers[tokenMint].add(trackedWallet);
    const count = clusterBuyers[tokenMint].size;

    if (count < CLUSTER_MIN_WALLETS) return;   // not enough distinct wallets yet
    if (clusterFired.has(tokenMint)) return;

    // Age gate — need creation time. Use token/info's creation_timestamp.
    const info = await getCachedTokenInfo(tokenMint);
    const created = parseInt(info?.creation_timestamp ?? 0, 10) || 0;
    if (!(created > 0)) {
      log(`[CLUSTER] ${tokenMint.substring(0,8)} — no creation timestamp, can't check age; skipping`);
      return;
    }
    const now = Math.floor(Date.now() / 1000);
    const age = now - created;
    if (age < CLUSTER_MIN_AGE) {
      log(`[CLUSTER] SKIP ${tokenMint.substring(0,8)} — too new (${age}s < ${CLUSTER_MIN_AGE}s)`);
      return;
    }
    if (age > CLUSTER_MAX_AGE) {
      log(`[CLUSTER] SKIP ${tokenMint.substring(0,8)} — too old (${fmtAge(age)} > ${CLUSTER_MAX_AGE/3600}h)`);
      return;
    }

    // GATE: market cap >= $30k.
    let _mcCheck = tokenMarketCap(info);
    if (!(_mcCheck > 0)) { const tt = trendingMap.get(tokenMint); if (tt) _mcCheck = tt.mc; }
    if (!(_mcCheck >= MC_MIN_USD)) {
      log(`[CLUSTER] SKIP ${info?.symbol || tokenMint.substring(0,8)} — MC ${fmtUsd(_mcCheck)} < ${fmtUsd(MC_MIN_USD)}`);
      return;
    }

    // GATE: token must be TRENDING (top-N any interval) OR have >= $100k 5-min volume.
    const isTrending = trendingMap.has(tokenMint);
    let vol5m = 0;
    if (!isTrending) {
      vol5m = await gmgn5mVolumeUsd(tokenMint);
    }
    if (!isTrending && !(vol5m >= VOL_GATE_USD)) {
      log(`[CLUSTER] SKIP ${info?.symbol || tokenMint.substring(0,8)} — not trending & 5m vol ${fmtUsd(vol5m)} < ${fmtUsd(VOL_GATE_USD)}`);
      return;
    }

    // Fire once.
    if (clusterFired.has(tokenMint)) return;
    clusterFired.add(tokenMint);
    saveSet('/tmp/sol_cluster_fired.json', clusterFired);
    const gateReason = isTrending ? 'trending' : `5m vol ${fmtUsd(vol5m)}`;

    const buyers = [...clusterBuyers[tokenMint]].map(walletName);
    const symbol = info?.symbol ?? 'UNKNOWN';
    let mc = tokenMarketCap(info);
    const mcStr = mc > 0 ? fmtUsd(mc) : 'N/A';
    const buyerList = buyers.map(b => `  • ${b}`).join('\n');
    const signalTime = new Date().toLocaleTimeString('en-US', {
      timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true
    });

    sendTelegram(CLUSTER_SIGNAL_CHAT,
      `⚡ <b>${count} Wallet Cluster — ${symbol}</b>\n\n` +
      `Contract: <code>${tokenMint}</code>\n\n` +
      `Wallets: <b>${count}</b>\n` +
      `Token Age: ${fmtAge(age)}\n` +
      `Market Cap: ${mcStr}\n` +
      `Qualified: ${gateReason}\n\n` +
      `<b>Bought by:</b>\n${buyerList}\n\n` +
      `Signal Time: ${signalTime}\n\n` +
      `🔗 <a href="https://gmgn.ai/sol/token/${tokenMint}">View on GMGN</a>`
    );
    log(`[CLUSTER] 🔥 FIRED ${symbol} ${tokenMint.substring(0,8)} — ${count} wallets | age ${fmtAge(age)} | ${gateReason}`);
  } catch (e) {
    log(`[ERR] checkClusterSignal: ${e.message}`);
  }
}

// ── TELEGRAM ──────────────────────────────────────────────────
function sendTelegram(chatId, message) {
  const body = JSON.stringify({ chat_id: chatId, text: message, parse_mode: 'HTML' });
  const req = https.request({
    hostname: 'api.telegram.org',
    path: `/bot${TELEGRAM_TOKEN}/sendMessage`,
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
  }, (res) => {
    let d = ''; res.on('data', c => d += c);
    res.on('end', () => {
      try { const p = JSON.parse(d); if (!p.ok) log(`[TG Error] ${p.description}`); else log(`[TG] Delivered to ${chatId}`); }
      catch { log(`[TG Error] Parse failed`); }
    });
  });
  req.on('error', e => log(`[TG ERR] ${e.message}`));
  req.write(body); req.end();
}

// ── MINT EXTRACTION ───────────────────────────────────────────
// Returns the mint the TRACKED WALLET actually received in this tx (post balance
// > pre balance). Returns null for sells, transfers out, or tokens that merely
// passed through other accounts — this is what prevents false "wallet bought X"
// attributions.
function extractMint(tx, trackedWallet) {
  const meta = tx?.meta; const msg = tx?.transaction?.message;
  if (!meta || !msg) return null;
  const postBals = meta.postTokenBalances ?? [];
  const preBals  = meta.preTokenBalances ?? [];

  const preByMint = {};
  for (const b of preBals) {
    if (b.owner !== trackedWallet) continue;
    preByMint[b.mint] = parseFloat(b.uiTokenAmount?.uiAmount ?? 0) || 0;
  }

  let bestMint = null, bestDelta = 0;
  for (const b of postBals) {
    if (b.owner !== trackedWallet) continue;          // only the tracked wallet's own accounts
    if (!b.mint || b.mint === SOL_MINT) continue;     // ignore SOL/wSOL
    const postAmt = parseFloat(b.uiTokenAmount?.uiAmount ?? 0) || 0;
    const preAmt  = preByMint[b.mint] ?? 0;
    const delta   = postAmt - preAmt;
    if (delta > 0 && delta > bestDelta) { bestDelta = delta; bestMint = b.mint; }
  }
  return bestMint;
}

// ── LOG NOTIFICATION PROCESSING ──────────────────────────────
async function processLogNotification(params) {
  const value = params?.result?.value;
  const subId = params?.subscription;
  if (!value || (value.err !== null && value.err !== undefined)) return;

  const signature     = value.signature;
  const trackedWallet = subIdToWallet[subId];
  if (!trackedWallet) return;

  if (!isActiveHours()) return;

  if (pendingSigs.has(signature)) return;
  pendingSigs.add(signature);
  setTimeout(() => pendingSigs.delete(signature), 30000);

  // Per-wallet flood throttle. A wallet firing absurdly fast (e.g. a dev spamming
  // its own token) can't add anything — the signal fires once per token — so
  // dropping its excess events avoids hundreds of wasted getTransaction calls.
  const nowMs = Date.now();
  const times = (walletEventTimes[trackedWallet] ?? []).filter(t => nowMs - t < 10000);
  times.push(nowMs);
  walletEventTimes[trackedWallet] = times;
  if (times.length > 15) return;

  let tx = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    tx = await getTransaction(signature);
    if (tx) break;
    await sleep(2000);
  }
  if (!tx) return;

  const mint = extractMint(tx, trackedWallet);
  if (!mint) return;

  // Only the first buy of a token by a given wallet matters — the signal fires
  // once per token regardless.
  const pairKey = `${trackedWallet}:${mint}`;
  if (seenPairs.has(pairKey)) return;
  seenPairs.add(pairKey);
  log(`[MINT] ${walletName(trackedWallet)} bought ${mint.substring(0,8)}`);

  // Cache the dev wallet so the dev-exclusion gate can use it.
  if (!devWalletCache[mint]) {
    const devInfo = await getCachedTokenInfo(mint);
    devWalletCache[mint] = devInfo?.dev?.creator_address ?? 'unknown';
    setTimeout(() => delete devWalletCache[mint], 600000);
  }

  // ── SIGNAL 1: Bluechip trending buy ──
  await sendTrendSignal(trackedWallet, mint, tx);

  // ── SIGNAL 2: 8-wallet cluster ──
  await checkClusterSignal(trackedWallet, mint);
}

// ── WEBSOCKET ─────────────────────────────────────────────────
const WATCHDOG_MS = 3 * 60 * 1000;

setInterval(() => {
  if (!wsReady) return;
  const silent = Date.now() - lastMessageAt;
  if (silent > WATCHDOG_MS) {
    log(`[WS] Watchdog: ${Math.round(silent/1000)}s silent — reconnecting...`);
    wsReady = false;
    try { ws.terminate(); } catch(e) {}
    wssIndex = (wssIndex + 1) % WSS_ENDPOINTS.length;   // rotate to the next provider
    reconnectDelay = 5000;
    connect();
  }
}, 60000);

function connect() {
  const ep = WSS_ENDPOINTS[wssIndex];
  const url = ep.url;
  log(`[WS] Connecting to ${ep.name} (${wssIndex + 1}/${WSS_ENDPOINTS.length})...`);
  ws = new WebSocket(url, { handshakeTimeout: 30000 });
  subIdToWallet = {}; reqIdToWallet = {}; wsReady = false;

  ws.on('open', async () => {
    log(`[WS] Connected — subscribing to ${WALLETS.length} wallets...`);
    wsReady = true; reconnectDelay = 5000; lastMessageAt = Date.now();
    // Send subscriptions PACED, not all at once. A burst of 100+ logsSubscribe
    // messages the instant the socket opens trips public-RPC rate limits (close 1013).
    for (let i = 0; i < WALLETS.length; i++) {
      if (ws.readyState !== WebSocket.OPEN) { log(`[WS] Socket closed mid-subscribe at ${i}/${WALLETS.length}`); return; }
      const wallet = WALLETS[i];
      const reqId = i + 1; reqIdToWallet[reqId] = wallet;
      ws.send(JSON.stringify({ jsonrpc: '2.0', id: reqId, method: 'logsSubscribe',
        params: [{ mentions: [wallet] }, { commitment: 'confirmed' }] }));
      await sleep(40);
    }
    log(`[WS] All ${WALLETS.length} subscriptions sent`);
    const pi = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.ping(); else clearInterval(pi); }, 30000);
  });

  ws.on('pong', () => { lastMessageAt = Date.now(); });

  ws.on('message', (data) => {
    lastMessageAt = Date.now();
    let msg; try { msg = JSON.parse(data.toString()); } catch { return; }
    if (msg.id !== undefined && typeof msg.result === 'number' && !msg.method) {
      const wallet = reqIdToWallet[msg.id];
      if (wallet) {
        subIdToWallet[msg.result] = wallet;
        const confirmed = Object.keys(subIdToWallet).length;
        if (confirmed === WALLETS.length) log(`[WS] ✅ All ${WALLETS.length} subscriptions active`);
      }
      return;
    }
    if (msg.method === 'logsNotification') {
      processLogNotification(msg.params).catch(e => log(`[ERR] ${e.message}`));
    }
  });

  ws.on('error', e => log(`[WS] Error: ${e.message}`));
  ws.on('close', (code) => {
    wsReady = false;
    log(`[WS] Disconnected (${code}). Reconnecting in ${reconnectDelay/1000}s...`);
    // After ~3 failed attempts on this endpoint, rotate to the next provider.
    if (reconnectDelay >= 20000) {
      wssIndex = (wssIndex + 1) % WSS_ENDPOINTS.length;
      reconnectDelay = 5000;
      log(`[WS] Rotating to ${WSS_ENDPOINTS[wssIndex].name} after repeated failures`);
    }
    setTimeout(() => connect(), reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 300000);  // cap 5 min — don't torch credits on a persistent outage
  });
}

// ── CLEANUP ───────────────────────────────────────────────────
setInterval(() => {
  if (seenPairs.size > 20000) { seenPairs.clear(); log(`[CLEANUP] seenPairs cleared`); }
  if (trendFired.size > 20000) { trendFired.clear(); saveSet(FIRED_FILE, trendFired); log(`[CLEANUP] trendFired cleared`); }
  if (clusterFired.size > 20000) { clusterFired.clear(); saveSet('/tmp/sol_cluster_fired.json', clusterFired); log(`[CLEANUP] clusterFired cleared`); }
  if (Object.keys(clusterBuyers).length > 20000) { clusterBuyers = {}; log(`[CLEANUP] clusterBuyers cleared`); }
  if (Object.keys(trendBuyers).length > 20000) { trendBuyers = {}; log(`[CLEANUP] trendBuyers cleared`); }
  if (Object.keys(vol5mCache).length > 5000) { vol5mCache = {}; }
  const cutMs = Date.now() - 10000;
  for (const w of Object.keys(walletEventTimes)) {
    walletEventTimes[w] = walletEventTimes[w].filter(t => t > cutMs);
    if (walletEventTimes[w].length === 0) delete walletEventTimes[w];
  }
}, 60000);

// ── HEALTH CHECK ──────────────────────────────────────────────
http.createServer((req, res) => {
  if (req.url === '/logs') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end(logBuffer.join('\n'));
    return;
  }
  if (req.url === '/trending') {
    // Dump the current trending cache — handy for eyeballing whether anything
    // is close to qualifying.
    const now = Math.floor(Date.now() / 1000);
    const rows = [...trendingMap.entries()]
      .sort((a, b) => b[1].bluechip - a[1].bluechip)
      .map(([addr, v]) => {
        const age = v.created > 0 ? now - v.created : null;
        const ok = (age !== null && age <= TREND_MAX_TOKEN_AGE && v.bluechip > TREND_MIN_BLUECHIP);
        return `${ok ? '✅' : '  '} ${(v.symbol||'?').padEnd(14)} bluechip=${(v.bluechip*100).toFixed(1).padStart(5)}%  age=${fmtAge(age).padStart(7)}  mc=${fmtUsd(v.mc).padStart(9)}  [${[...v.intervals].join(',')}]  ${addr}`;
      });
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end(
      `TRENDING CACHE — ${trendingMap.size} tokens\n` +
      `Last refresh: ${trendLastOk ? new Date(trendLastOk*1000).toLocaleTimeString('en-US',{timeZone:'America/Toronto'}) : 'never'}\n` +
      `Gates: age < ${TREND_MAX_TOKEN_AGE/3600}h AND bluechip > ${(TREND_MIN_BLUECHIP*100).toFixed(0)}%\n` +
      `(✅ = currently meets both gates; a tracked-wallet buy on one of these fires)\n\n` +
      rows.join('\n') + '\n'
    );
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(
    `SOLANA BLUECHIP TRENDING BOT — LIVE\n` +
    `WS: ${wsReady ? 'connected' : 'reconnecting'}\n` +
    `Subscriptions: ${Object.keys(subIdToWallet).length}/${WALLETS.length}\n` +
    `\nSIGNAL: tracked wallet buys a token that is\n` +
    `  • top ${TREND_TOP_N} trending in ANY of [${TREND_INTERVALS.join(', ')}]\n` +
    `  • under ${TREND_MAX_TOKEN_AGE/3600}h old\n` +
    `  • over ${(TREND_MIN_BLUECHIP*100).toFixed(0)}% bluechip holders\n` +
    `  → fires to ${TREND_SIGNAL_CHAT} (once per token)\n` +
    `\nTrending cache: ${trendingMap.size} tokens | refreshes: ${trendRefreshes}\n` +
    `Last trend refresh: ${trendLastOk ? new Date(trendLastOk*1000).toLocaleTimeString('en-US',{timeZone:'America/Toronto'}) : 'never'}\n` +
    `Fired: ${trendFired.size}\n` +
    `\nHit /logs for the last 500 log lines\n` +
    `Hit /trending to see the current trending cache + which tokens qualify\n`
  );
}).listen(process.env.PORT || 3000, () => log(`[HTTP] Health server on port ${process.env.PORT || 3000}`));

// ── START ─────────────────────────────────────────────────────
log(`═══ SOL BLUECHIP TRENDING BOT — VERSION 2026-07-17b ═══`);
log(`[START] ${WALLETS.length} wallets | SOLE SIGNAL: tracked buy + top-${TREND_TOP_N} trending (any interval) + age < ${TREND_MAX_TOKEN_AGE/3600}h + bluechip > ${(TREND_MIN_BLUECHIP*100).toFixed(0)}%`);
log(`[START] Signal chat: ${TREND_SIGNAL_CHAT} | Trending refresh: every ${TREND_POLL_SECS}s across [${TREND_INTERVALS.join(', ')}]`);
log(`[START] WSS chain: ${WSS_ENDPOINTS.map(e => e.name).join(' -> ')}`);

https.get('https://api.ipify.org?format=json', (res) => {
  let d = ''; res.on('data', c => d += c);
  res.on('end', () => { try { log(`[IP] ${JSON.parse(d).ip}`); } catch {} });
}).on('error', () => {});

// Prime the trending cache immediately, then refresh on a timer.
refreshTrending().then(() => {
  log(`[TREND] initial load: ${trendingMap.size} tokens`);
});
setInterval(refreshTrending, TREND_POLL_SECS * 1000);

// Connect WebSocket
connect();

// Self-ping (Render only — harmless on a VPS where RENDER_EXTERNAL_URL is unset)
if (RENDER_URL) {
  setInterval(() => {
    const mod = RENDER_URL.startsWith('https') ? https : http;
    mod.get(RENDER_URL + '/', res => log(`[PING] ${res.statusCode}`))
      .on('error', e => log(`[PING] ${e.message}`));
  }, 10 * 60_000);
}
