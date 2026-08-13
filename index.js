// ============================================================
//  SOLANA COMBINED BOT
//  ----------------------------------------------------------
//  >>> VERSION: 2026-08-12b  (order_by stays 'volume' — 12a's change was wrong; adds hot_level capture) <<<
//  ----------------------------------------------------------
//  2026-08-12b: 12a switched the rank query from order_by:'volume' to 'default',
//  reasoning that "rank by volume" wasn't "rank on GMGN's Trending tab". Direct
//  observation of the live Trending tab on 2026-08-12 DISPROVED that: the tab's
//  column header reads "<interval> Vol ↓" and the rows descend by that interval's
//  volume. The Trending tab IS a volume sort. order_by:'volume' was correct and
//  is kept. 12a never shipped to sol; it did briefly ship to evm and is reverted
//  there too.
//  THE REAL DISCREPANCY IS FILTERING, NOT SORTING. GMGN's Trending tab shows a
//  FILTERED set (the UI Filter control is active), while this bot calls the raw
//  rank endpoint with NO filter params and so sees tokens the site hides.
//  Measured on SOL 1h, 2026-08-12: the site's top 3 were GTA / CALLOOOR /
//  Plumber, but in the unfiltered API response those sat at positions 6 / 7 / 9,
//  behind seven higher-volume tokens (Fool, AFP, TOAD, VOL, App, CATE, SPERP)
//  that the site did not list at all. Same relative order, different pool. So the
//  bot's "#1" means "#1 of a LARGER pool than the site shows" — which is exactly
//  how a token can be #1 here while absent from the Trending tab (the GMEB case).
//  Matching the site requires passing the same filter tags; that work is OPEN.
//  ADDED in 12b: capture the `hot_level` response field per token, shown in the
//  #1-Everywhere alert as a cross-check against the live site. NOTE hot_level is
//  a COARSE bucket — observed values are only 0-3, with many ties — so it is
//  usable as a filter threshold but NEVER as a rank.
//  >>> PREVIOUS: 2026-07-17w (fix: bluechip carry-forward only fills a 0, no longer locks in a stale HIGH) <<<
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
//   2026-07-17i — CLUSTER BIG-BUY GATE. The 8-wallet cluster signal now also
//         requires that AT LEAST ONE of the 8 distinct wallets spent >= $500 USD
//         (CLUSTER_MIN_BIG_BUY_USD). Sub-$500 buys STILL count toward the wallet
//         count — 7 x $50 + 1 x $500 fires, 8 x $50 does not.
//         Supporting changes:
//           - clusterBuyers[mint] changed from a Set of addresses to a Map of
//             address -> USD spent. Size is captured at buy-time; the tx is not
//             available later.
//           - extractSolSpent() REINSTATED (removed in the 17d rewrite). Per the
//             24q/24r history it PREFERS the wSOL swap leg and falls back to the
//             native lamport delta (minus fee) — it never adds both legs, which
//             was the old ~2x double-count bug.
//           - getSolPriceUsd() (cached 60s) + buyUsdValue() convert to USD.
//           - FAIL-OPEN on unknown size: if extraction or the price lookup fails,
//             buyUsdValue returns null and the wallet counts as qualifying, so a
//             GMGN blip can never silently suppress a real cluster (same failure
//             class as the FABLE bug). A clean parsed $0 is fail-CLOSED — that's
//             a transfer-in, not a buy. Unknowns are logged.
//           - Alert now shows "Largest Buy" and per-wallet sizes, sorted desc.
//         The bluechip trending signal is UNCHANGED by this version.
//
//   2026-07-17h — FABLE BLUECHIP BUG FIX (cache staleness, NOT a merge bug).
//         Symptom: FABLE was rank 1-3 with 6+ tracked wallets buying but never
//         fired; log showed "bluechip 0.0%" while /trending showed 65.5%.
//         Root cause: refreshTrending() rebuilds trendingMap from scratch every
//         30s and commits the partial map whenever ANY interval returns tokens
//         (next.size > 0). When the 6h interval call blipped (rate limit/timeout)
//         for a cycle, FABLE landed carrying only the 1m/5m rank rows — which
//         report bluechip_owner_percentage 0 — so its cached bluechip regressed
//         to 0.0% and the signal gate skipped it. /trending was viewed on a later
//         healthy cycle, hence 65.5%. Same map, different moment.
//         Fix (two parts, display/cache only — signal gate & fire logic untouched):
//           (1) CARRY-FORWARD: after building the fresh map each cycle, keep the
//               highest bluechip a token had while it stays in the trending set,
//               so a partial refresh can't zero out a known value.
//           (2) INTERVAL RETRY: fetchTrendingInterval retries once (300ms) on a
//               blank/failed interval before giving up, reducing how often 6h
//               drops out of a rebuild in the first place.
//         The gate `rank <= TREND_TOP_WIDE && t.bluechip > TREND_MIN_BLUECHIP`,
//         all thresholds, the wallet-count/age/MC gates, and the fire decision
//         are byte-identical to 17g.
//
//   2026-07-17d — COMPLETE SIGNAL REWRITE. Removed ALL previous signals:
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
// Full wss:// URLs (the API key is embedded in the URL, so we read the whole URL,
// not a bare key). Set SB_DRPC_WSS_URL / SB_CHAINSTACK_WSS_URL in .env-vars.
const DRPC_WSS_URL       = process.env.DRPC_WSS_URL;
const CHAINSTACK_WSS_URL = process.env.CHAINSTACK_WSS_URL;

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
const TREND_POLL_SECS     = parseInt(process.env.TREND_POLL_SECS || '60', 10);        // refresh every 60s — the 429 circuit breaker (17q) handles bans now, and rank is weight-1 (~20 req/sec headroom), so the 300s panic setting is unnecessary and was causing missed #1-everywhere signals between polls
// All five intervals — a token counts as trending if it's top-10 in ANY of them.
const TREND_INTERVALS     = ['1m', '5m', '1h', '6h', '24h'];
// Filter tags sent with every /v1/market/rank call, set 2026-08-12 to N's ACTUAL
// SOL Trending tab settings: NoMint + No Blacklist.
// UI label -> API tag mapping (counterintuitive, do not "correct" these):
//   "NoMint"       -> renounced  (mint authority revoked, renounced_mint == 1)
//   "No Blacklist" -> frozen     (freeze authority revoked,
//                                 renounced_freeze_account == 1). The tag is
//                                 named for the AUTHORITY, not the state — it
//                                 does NOT mean "this token is frozen".
// Comma-separated, env-tunable. Set TREND_FILTERS= (empty) to send no filters.
const TREND_FILTERS = (process.env.TREND_FILTERS ?? 'renounced,frozen')
  .split(',').map(s => s.trim()).filter(Boolean);
// MAX TOKEN AGE ON THE POOL — N's SOL tab has a 2880-minute (48h) age filter, so
// the site ranks the top N *among tokens younger than 48h*. The bot previously
// ranked the top N of ALL ages and only checked age later, per token, in
// checkTop1Everywhere — so older high-volume tokens could occupy pool slots and
// crowd out genuinely new trending ones before any bot-side gate ran.
// Sent as the rank endpoint's `max_created` (duration string with m/h/d suffix;
// a bare number is rejected). Empty string disables the age filter.
const TREND_MAX_CREATED = (process.env.TREND_MAX_CREATED ?? '2880m').trim();

// ══════════════════════════════════════════════════════════════
//  8-WALLET CLUSTER SIGNAL CONFIG
// ══════════════════════════════════════════════════════════════
// Fires to CHAT_ID_FAST when 8 DISTINCT tracked wallets buy the same token,
// and the token's age is between CLUSTER_MIN_AGE and CLUSTER_MAX_AGE.
// No trending or bluechip requirement. Any buy size. Fires once per token.
const CLUSTER_SIGNAL_CHAT = process.env.CLUSTER_SIGNAL_CHAT || CHAT_ID_FAST;
// #1-EVERYWHERE signal: a token that is rank #1 in ALL FIVE intervals in the
// SAME refresh. Poller-driven (fires from the trending refresh, not a wallet
// buy). Rare, high-signal. Routes to its own chat.
const TOP1_SIGNAL_CHAT = process.env.TOP1_SIGNAL_CHAT || "-5305037806";
// MASTER SWITCH: cluster signal. Default OFF (2026-08-05) to cut GMGN load while
// isolating the rate-limit bans — bluechip + #1-everywhere stay on. Set
// ENABLE_CLUSTER=1 in .env-vars to turn it back on (no code change needed).
const ENABLE_CLUSTER = (process.env.ENABLE_CLUSTER || '1') === '1';   // re-enabled 2026-08-11 with revised rule
const CLUSTER_MIN_WALLETS = parseInt(process.env.CLUSTER_MIN_WALLETS || '5', 10);   // 5+ wallets, each with a >=$500 buy
// BUY DETECTION MODE (2026-08-11): 'POLL' = HTTP getSignaturesForAddress every
// BUY_POLL_SECS (works on any free RPC — no WebSocket subscriptions, which every
// free tier gates). 'WS' = the old logsSubscribe WebSocket. Default POLL because
// free WSS endpoints (public/dRPC/Chainstack) all refuse 94-wallet logsSubscribe.
const BUY_MODE      = (process.env.BUY_MODE || 'POLL').toUpperCase();
const BUY_POLL_SECS = parseInt(process.env.BUY_POLL_SECS || '60', 10);
const CLUSTER_MIN_AGE     = parseInt(process.env.CLUSTER_MIN_AGE || '60', 10);      // >= 60 seconds old
const CLUSTER_MAX_AGE     = parseInt(process.env.CLUSTER_MAX_AGE || '3600', 10);   // <= 60 minutes old (revised 2026-08-11)

// Cluster signal additionally requires that AT LEAST ONE of the distinct wallets
// spent >= this much USD on its buy. Sub-threshold buys still count toward the
// wallet count — this is a "someone had real conviction" filter, not a size floor.
// 7 x $50 + 1 x $500 fires; 8 x $50 does not.
const CLUSTER_MIN_BIG_BUY_USD = parseFloat(process.env.CLUSTER_MIN_BIG_BUY_USD || '500');
// WHALE HOLDER signal (2026-08-11): fires when a token has >WHALE_MIN_HOLDERS
// holders, is < WHALE_MAX_AGE old, and is #1 in >=1 trending interval. Poll-driven
// (reads the trending rows — no wallet buys, no WebSocket, no extra GMGN call).
const WHALE_MIN_HOLDERS   = parseInt(process.env.WHALE_MIN_HOLDERS || '5000', 10);
const WHALE_MAX_AGE       = parseInt(process.env.WHALE_MAX_AGE || '3600', 10);   // < 60 min old
const WHALE_SIGNAL_CHAT   = process.env.WHALE_SIGNAL_CHAT || CLUSTER_SIGNAL_CHAT; // cluster chat

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
  HELIUS:     HELIUS_API_KEY   ? { name: 'HELIUS',     url: `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` }  : null,
  ALCHEMY:    ALCHEMY_API_KEY  ? { name: 'ALCHEMY',    url: `wss://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}` } : null,
  SHYFT:      SHYFT_API_KEY     ? { name: 'SHYFT',      url: `wss://rpc.shyft.to?api_key=${SHYFT_API_KEY}` }              : null,
  // dRPC + Chainstack: full private wss:// URLs from .env-vars (key is in the URL).
  DRPC:       DRPC_WSS_URL       ? { name: 'DRPC',       url: DRPC_WSS_URL }       : null,
  CHAINSTACK: CHAINSTACK_WSS_URL ? { name: 'CHAINSTACK', url: CHAINSTACK_WSS_URL } : null,
  PUBLIC:     { name: 'PUBLIC', url: 'wss://api.mainnet-beta.solana.com' },
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
let top1Fired       = loadSet('/tmp/sol_top1_fired.json');  // tokens that already fired the #1-everywhere signal
let top1Primed      = false;  // first #1-everywhere pass after boot is SILENT — it records what's ALREADY #1 without alerting, so we only fire on tokens that hit #1 AFTER the bot started watching (avoids a stale startup burst)
let top1PrimedSet   = new Set();  // tokens seen as #1 during the silent boot prime — NON-persisted; NOT the fired-set. A token here was #1 at boot; if it's still #1 on a later (non-silent) cycle it fires normally.
let trendBuyers     = {};         // mint -> Set of distinct tracked wallets that bought it (for TREND_MIN_WALLETS gate)
let tokenInfoCache  = {};
let tokenInfoInflight = {};
let devWalletCache  = {};
let pendingSigs     = new Set();
let seenPairs       = new Set();  // "wallet:mint" — only the first buy per wallet+token matters
// mint -> Map of trackedWallet -> USD spent on that wallet's first buy.
// Was a Set of addresses; became a Map in 17i so the big-buy gate can check
// whether any one of the distinct wallets spent >= CLUSTER_MIN_BIG_BUY_USD.
// A value of null means "size unknown" (price lookup or extraction failed) —
// treated as QUALIFYING (fail-open), see clusterHasBigBuySol().
let clusterBuyers   = {};
let clusterFired    = loadSet('/tmp/sol_cluster_fired.json');  // tokens that already fired the cluster signal
let whaleFired      = loadSet('/tmp/sol_whale_fired.json');    // tokens that already fired the whale-holder signal
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
// ── GMGN RATE-LIMIT CIRCUIT BREAKER ───────────────────────────
// GMGN's docs: on a 429 RATE_LIMIT_BANNED, EVERY request during the cooldown
// EXTENDS the ban by 5s (up to 5 min). So the old behavior — keep polling through
// a ban — fed the ban and made it permanent. This gate makes the bot STOP calling
// GMGN until the ban's reset_at passes. gmgnBannedUntil is a unix-secs timestamp.
let gmgnBannedUntil = 0;
function gmgnIsBanned() { return Math.floor(Date.now() / 1000) < gmgnBannedUntil; }

function httpsGet(hostname, path, headers = {}) {
  return new Promise((resolve) => {
    const req = https.request({ hostname, path, method: 'GET', headers }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        // On a 429 from GMGN, read reset_at and trip the breaker so we stop
        // calling until the ban lifts (calling during cooldown extends it 5s each).
        if (res.statusCode === 429 && hostname === 'openapi.gmgn.ai') {
          let resetAt = 0;
          try { resetAt = parseInt(JSON.parse(data)?.reset_at ?? 0, 10) || 0; } catch {}
          // fall back to the X-RateLimit-Reset header, then a 5-min default
          if (!resetAt) resetAt = parseInt(res.headers['x-ratelimit-reset'] ?? 0, 10) || 0;
          if (!resetAt) resetAt = Math.floor(Date.now() / 1000) + 300;
          if (resetAt > gmgnBannedUntil) {
            gmgnBannedUntil = resetAt;
            log(`[GMGN] 🛑 429 RATE_LIMIT_BANNED — pausing ALL GMGN calls until ${new Date(resetAt*1000).toLocaleTimeString('en-US',{timeZone:'America/Toronto'})} (calling during cooldown would extend the ban)`);
          }
          resolve(null); return;
        }
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

// Recent signatures for a wallet (newest first). `untilSig` stops the walk once
// we reach a signature we've already processed, so we only ever fetch NEW activity.
async function getSignaturesForAddress(wallet, untilSig) {
  const params = untilSig
    ? [wallet, { limit: 25, until: untilSig, commitment: 'confirmed' }]
    : [wallet, { limit: 5, commitment: 'confirmed' }];   // first pass: just seed the cursor
  for (const rpc of HTTP_RPCS) {
    const r = await httpsPost(rpc, { jsonrpc: '2.0', id: 1, method: 'getSignaturesForAddress', params });
    if (Array.isArray(r?.result)) return r.result;   // [] is a valid answer (no new txs)
  }
  return null;   // all RPCs failed
}

// ── GMGN ──────────────────────────────────────────────────────
// Auth requires ALL of: X-APIKEY header, a browser User-Agent (GMGN 403s the
// default Node/Python UA), plus timestamp + client_id query params.
async function gmgnGet(path, params = {}, skipAuth = false) {
  // Circuit breaker: if we're inside a known ban window, do NOT call GMGN —
  // returning null here is what lets the ban actually expire instead of being
  // extended 5s by every request during cooldown.
  if (gmgnIsBanned()) return null;
  if (!skipAuth) {
    params.timestamp = Math.floor(Date.now() / 1000).toString();
    params.client_id = Math.random().toString(36).substring(2) + Date.now().toString(36);
  }
  // Build the query manually so ARRAY values become REPEATED params
  // (filters=a&filters=b&filters=c), which is how gmgn-cli encodes them —
  // verified 2026-08-12 in its buildUrl(). `new URLSearchParams(obj)` would
  // comma-join an array into "a,b,c", which GMGN silently ignores: the request
  // still succeeds and returns an UNFILTERED list, so a broken filter is
  // indistinguishable from a working one. Hence the hand-rolled loop.
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (Array.isArray(v)) { for (const item of v) qs.append(k, String(item)); }
    else if (v !== undefined && v !== null) qs.set(k, String(v));
  }
  const query = qs.toString();
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

// Fetch one interval's top-N. Retries ONCE (300ms) on a blank/failed response
// before giving up. This reduces how often an interval (esp. 6h) drops out of a
// rebuild due to a transient GMGN rate-limit/timeout — part of the FABLE fix.
async function fetchTrendingInterval(interval) {
  for (let attempt = 0; attempt < 2; attempt++) {
    // MATCHES gmgn.ai's Trending tab: volume sort + the chain's filter tags.
    // Established 2026-08-12 from screenshots of the live tab — each interval's
    // column header reads "<interval> Vol ↓" with rows descending by that
    // interval's volume, so order_by=volume + direction=desc is correct. (12c
    // dropped the sort entirely, which moved AWAY from the site.) TREND_FILTERS
    // supplies the chain defaults the raw endpoint does NOT apply on its own.
    const data = await gmgnGet('/v1/market/rank', {
      chain: 'sol',
      interval,
      order_by: 'volume',
      direction: 'desc',
      limit: String(TREND_TOP_N),
      ...(TREND_FILTERS.length ? { filters: TREND_FILTERS } : {}),
      ...(TREND_MAX_CREATED ? { max_created: TREND_MAX_CREATED } : {}),
    });
    const rank = extractRank(data);
    if (rank.length) return rank.slice(0, TREND_TOP_N);
    await sleep(300);  // one retry before giving up on this interval
  }
  return [];
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
        const hotLevel = parseFloat(t.hot_level ?? 0) || 0;   // GMGN's own trending-intensity metric (2026-08-12a)
        const existing = next.get(addr);
        if (existing) {
          existing.intervals.add(interval);
          existing.ranks[interval] = rankPos;   // per-interval rank (for #1-everywhere)
          if (rankPos < existing.bestRank) existing.bestRank = rankPos;
          if (bluechip > existing.bluechip) existing.bluechip = bluechip;  // within THIS cycle, take the highest non-zero interval reading (all are "now")
          if (hotLevel > (existing.hotLevel || 0)) existing.hotLevel = hotLevel;
        } else {
          next.set(addr, {
            symbol:   t.symbol ?? 'UNKNOWN',
            bluechip,                                        // 0-1 scale (highest across intervals)
            bestRank: rankPos,                               // best (lowest) rank across intervals
            ranks:    { [interval]: rankPos },                // per-interval rank (for #1-everywhere)
            created,                                         // unix secs
            mc:       parseFloat(t.market_cap ?? 0) || 0,
            ath:      parseFloat(t.history_highest_market_cap ?? 0) || 0,
            volume:   parseFloat(t.volume ?? 0) || 0,
            holders:  parseInt(t.holder_count ?? 0, 10) || 0,
            smart:    parseInt(t.smart_degen_count ?? 0, 10) || 0,
            kol:      parseInt(t.renowned_count ?? 0, 10) || 0,
            liquidity: parseFloat(t.liquidity ?? 0) || 0,
            hotLevel,                                        // GMGN trending intensity (2026-08-12a)
            intervals: new Set([interval]),
          });
        }
      }
      await sleep(150);  // gentle pacing — rank is weight 1, 5 calls is cheap
    }

    if (next.size > 0) {
      // ── FABLE BLUECHIP CARRY-FORWARD ──────────────────────────
      // A partial refresh (e.g. the 6h interval rate-limited this cycle) must NOT
      // zero out a bluechip value we already knew. The 1m/5m rank rows report
      // bluechip 0, so if 6h drops out of a rebuild a still-trending token would
      // regress to 0.0% and the signal gate would skip it — exactly the FABLE
      // failure. Keep the highest bluechip a token had while it stays trending.
      // Bounded by design: a token only survives here while it remains in the
      // top-N of at least one interval; once it falls out of trending entirely
      // it's absent from `next` and drops from the map (correct — no more fires).
      for (const [addr, v] of next) {
        const prev = trendingMap.get(addr);
        // ONLY carry the prior value when THIS cycle read 0 (missing data — the
        // bluechip-bearing interval dropped out this refresh). A non-zero reading
        // is current truth and REPLACES the prior, even when it's lower — a token
        // whose bluechip genuinely fell must reflect the new value, not the stale
        // peak. (Bug fix 2026-08-12: tokens were firing on a locked-in high after
        // their real bluechip had dropped, e.g. GTA fired 26.8% when live was 3.2%.)
        if (prev && v.bluechip === 0 && prev.bluechip > 0) {
          log(`[TREND] carry-forward ${v.symbol} bluechip 0.0% -> ${(prev.bluechip*100).toFixed(1)}% (missing this cycle, kept prior)`);
          v.bluechip = prev.bluechip;
        }
      }

      trendingMap = next;
      trendLastOk = Math.floor(Date.now() / 1000);
      trendRefreshes++;

      // #1-EVERYWHERE: strict — only evaluate when ALL five intervals reported
      // this cycle, so a partial refresh can never produce a false #1-everywhere
      // (a missing interval isn't a #1). Reads `ranks` already built above; makes
      // NO new GMGN calls.
      if (okIntervals === TREND_INTERVALS.length) {
        // First full pass after boot primes SILENTLY (records current #1s without
        // alerting). Every pass after fires normally.
        await checkTop1Everywhere(next, !top1Primed);
        checkWhaleHolders(next);
        top1Primed = true;
      }
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
//  #1-EVERYWHERE SIGNAL
// ══════════════════════════════════════════════════════════════
// Fires when a token is rank #1 in ALL FIVE intervals in the same refresh.
// Poller-driven (not tied to a wallet buy). Fires once per token. Reads the
// `ranks` map the poller already built — no extra network calls. Caller only
// invokes this when all five intervals reported (strict), so "#1 in all five"
// is real, not an artifact of a missing interval.
// WHALE HOLDER: scan the current trending map for tokens with > WHALE_MIN_HOLDERS
// holders, age < WHALE_MAX_AGE, and #1 in at least one interval. Poll-driven; fires
// once per token to the cluster chat. No wallet/WebSocket dependency.
function checkWhaleHolders(map) {
  if (!ENABLE_CLUSTER) return;   // gated with the cluster (same "add-back" package)
  const now = Math.floor(Date.now() / 1000);
  for (const [addr, v] of map.entries()) {
    if (whaleFired.has(addr)) continue;
    if ((v.holders || 0) <= WHALE_MIN_HOLDERS) continue;         // need > 5000 holders
    if ((v.bestRank || 999) !== 1) continue;                    // #1 in >=1 interval
    if (!(v.created > 0)) continue;                             // need age; fail-closed
    if ((now - v.created) >= WHALE_MAX_AGE) continue;           // must be < 60 min old

    whaleFired.add(addr);
    saveSet('/tmp/sol_whale_fired.json', whaleFired);
    const ageMin = Math.floor((now - v.created) / 60);
    const msg =
      `🐳 <b>WHALE HOLDER — ${v.symbol || '?'}</b>\n\n` +
      `<b>Holders:</b> ${v.holders.toLocaleString()}\n` +
      `<b>Age:</b> ${ageMin}m\n` +
      `<b>Trending Rank:</b> #${v.bestRank} (best across intervals)\n` +
      `<b>Market Cap:</b> ${fmtUsd(v.mc || 0)}\n` +
      `<b>Contract:</b> <code>${addr}</code>\n\n` +
      `🔗 <a href="https://gmgn.ai/sol/token/${addr}">View on GMGN</a>`;
    sendTelegram(WHALE_SIGNAL_CHAT, msg);
    log(`[WHALE] 🐳 FIRED ${v.symbol || addr.substring(0,8)} — ${v.holders} holders, ${ageMin}m old, rank #${v.bestRank}`);
  }
}

async function checkTop1Everywhere(map, silent) {
  try {
    for (const [addr, v] of map) {
      if (top1Fired.has(addr)) continue;
      const r = v.ranks || {};
      // Every interval must be present AND equal to 1.
      const allOne = TREND_INTERVALS.every(iv => r[iv] === 1);
      if (!allOne) continue;

      // FILTER: at least one smart-money or KOL holder (GMGN counts).
      if (((v.smart || 0) + (v.kol || 0)) < 1) continue;

      // FILTER: token age <= 48h. The trending/rank row often omits
      // creation_timestamp (common on some tokens), so when it's missing, fall
      // back to a token/info lookup which DOES carry it — same data GMGN has, the
      // rank row is just lean. Only fail-closed if BOTH sources lack it.
      const _now = Math.floor(Date.now() / 1000);
      let _created = v.created;
      if (!(_created > 0)) {
        const _info = await getCachedTokenInfo(addr);
        _created = parseInt(_info?.creation_timestamp ?? 0, 10) || 0;
      }
      if (!(_created > 0)) continue;                       // truly unknown -> skip
      if ((_now - _created) > 48 * 3600) continue;         // too old
      v.created = _created;                                // cache for the alert

      // Silent prime pass: record what's ALREADY #1 at boot in a SEPARATE,
      // non-persisted set so we don't alert on stale boot state. Do NOT add to
      // top1Fired here — that set is only for tokens that actually ALERTED. (Old
      // bug: silent prime added to the persisted fired-set, so a token that was
      // #1 across restarts got permanently skipped and never fired — which is
      // exactly what happened with restarts while a token sat at #1.)
      if (silent) {
        top1PrimedSet.add(addr);
        log(`[TOP1] prime (silent) ${v.symbol} ${addr.substring(0,8)} — already #1 everywhere at boot, not alerting`);
        continue;
      }

      // A token primed at boot should NOT be suppressed forever — only skipped
      // while it's still the same boot-state token. But once we're past the prime
      // pass (silent=false) and it's still #1, it's worth firing. So we clear it
      // from the primed set and fire normally below.
      top1PrimedSet.delete(addr);

      // Mark as fired ONLY now that we're actually alerting.
      top1Fired.add(addr);
      saveSet('/tmp/sol_top1_fired.json', top1Fired);

      const now = Math.floor(Date.now() / 1000);
      const age = v.created > 0 ? now - v.created : null;
      const mcStr  = v.mc > 0 ? fmtUsd(v.mc) : 'N/A';
      const athStr = v.ath > 0 ? fmtUsd(v.ath) : 'N/A';
      const volStr = v.volume > 0 ? fmtUsd(v.volume) : 'N/A';
      const hotStr = v.hotLevel > 0 ? v.hotLevel.toLocaleString() : 'N/A';
      const signalTime = new Date().toLocaleTimeString('en-US', {
        timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true
      });

      sendTelegram(TOP1_SIGNAL_CHAT,
        `\ud83d\udc51 <b>#1 Trending Everywhere — ${v.symbol}</b>\n\n` +
        `Rank #1 on ALL timeframes: ${TREND_INTERVALS.join(', ')}\n` +
        `(by interval volume, GMGN's Trending sort — unfiltered pool)\n\n` +
        `Chain: Solana\n` +
        `Contract: <code>${addr}</code>\n` +
        `Token Age: ${fmtAge(age)}\n` +
        `Market Cap: ${mcStr}\n` +
        `ATH MC: ${athStr}\n` +
        `Volume: ${volStr}\n` +
        `Hot Level: ${hotStr}\n` +
        `Bluechip: ${(v.bluechip*100).toFixed(1)}%\n` +
        `Holders: ${v.holders || 'N/A'}\n` +
        `Smart / KOL: ${v.smart} / ${v.kol}\n\n` +
        `Signal Time: ${signalTime}\n\n` +
        `\ud83d\udd17 <a href="https://gmgn.ai/sol/token/${addr}">View on GMGN</a>`
      );
      log(`[TOP1] \ud83d\udc51 FIRED ${v.symbol} ${addr.substring(0,8)} — #1 on all ${TREND_INTERVALS.length} intervals (hot_level=${v.hotLevel || 'N/A'})`);
    }
  } catch (e) {
    log(`[ERR] checkTop1Everywhere: ${e.message}`);
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
    // Single rule: top-10 trending (any interval) AND bluechip > 10%.
    if (!(rank <= TREND_TOP_WIDE && t.bluechip > TREND_MIN_BLUECHIP)) {
      log(`[TREND] SKIP ${t.symbol} — rank ${rank}, bluechip ${(t.bluechip*100).toFixed(1)}% (need top${TREND_TOP_WIDE} + >${(TREND_MIN_BLUECHIP*100).toFixed(0)}%)`);
      return;
    }
    const tierReason = `top${TREND_TOP_WIDE} +${(t.bluechip*100).toFixed(1)}%bc`;

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
      `Chain: Solana\n` +
      `Contract: <code>${tokenMint}</code>\n` +
      `Bought by: ${walletName(trackedWallet)}\n\n` +
      `Bluechip Holders: <b>${(t.bluechip*100).toFixed(1)}%</b>\n` +
      `Trending Rank: <b>#${t.bestRank || '?'}</b> (best across intervals)\n` +
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
async function checkClusterSignal(trackedWallet, tokenMint, tx) {
  try {
    if (clusterFired.has(tokenMint)) return;

    // Record this wallet's buy SIZE IN SOL — extracted from the tx with NO API
    // call (17j: previously we priced every buy via GMGN, which got the VPS IP
    // rate-limit-banned). SOL amount is stored now; USD conversion happens ONCE
    // below, only after the wallet threshold is hit. null = tx couldn't be parsed.
    if (!clusterBuyers[tokenMint]) clusterBuyers[tokenMint] = new Map();
    if (!clusterBuyers[tokenMint].has(trackedWallet)) {
      const sol = extractSolSpent(tx, trackedWallet);   // free — no network
      clusterBuyers[tokenMint].set(trackedWallet, sol); // SOL amount, 0, or null
    }
    const count = clusterBuyers[tokenMint].size;

    // Quick pre-check: can't possibly have 5 big buyers with fewer than 5 wallets.
    if (count < CLUSTER_MIN_WALLETS) return;
    if (clusterFired.has(tokenMint)) return;

    // GATE (revised 2026-08-11): require >= CLUSTER_MIN_WALLETS wallets that EACH
    // spent >= $500 (not just one). Convert SOL->USD ONCE here, only when a cluster
    // is close to firing. FAIL-OPEN per-wallet: an unparseable size counts as big.
    const bigBuy = await clusterBigBuyerCount(clusterBuyers[tokenMint]);
    if (bigBuy.bigCount < CLUSTER_MIN_WALLETS) {
      log(`[CLUSTER] SKIP ${tokenMint.substring(0,8)} — ${count} wallets, only ${bigBuy.bigCount} spent >= ${fmtUsd(CLUSTER_MIN_BIG_BUY_USD)} (need ${CLUSTER_MIN_WALLETS}) [${bigBuy.sizesStr}]`);
      return;
    }

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

    // GATE: token must be #1 TRENDING in ANY interval. (Was top-5 OR $100k 5m
    // volume; tightened — the volume path is gone, and top-5 is now strictly #1.)
    const _ct = trendingMap.get(tokenMint);
    const isNum1 = !!(_ct && (_ct.bestRank || 999) === 1);
    if (!isNum1) {
      const _r = _ct ? (_ct.bestRank || '?') : 'not trending';
      log(`[CLUSTER] SKIP ${info?.symbol || tokenMint.substring(0,8)} — rank ${_r}, need #1 trending`);
      return;
    }

    // Fire once.
    if (clusterFired.has(tokenMint)) return;
    clusterFired.add(tokenMint);
    saveSet('/tmp/sol_cluster_fired.json', clusterFired);
    const gateReason = `#1 trending`;

    // Buyer lines carry each wallet's buy size in USD (SOL amount x cached price
    // from getSolPriceUsd — already fetched by the gate, so no new network call).
    // Sorted biggest-first. '?' where the buy couldn't be parsed or price is down.
    const _solPrice = await getSolPriceUsd();
    const buyerEntries = [...clusterBuyers[tokenMint].entries()]
      .map(([w, sol]) => [w, (sol === null || !(_solPrice > 0)) ? null : sol * _solPrice])
      .sort((a, b) => (b[1] ?? 0) - (a[1] ?? 0));
    const buyers = buyerEntries.map(([w, usd]) =>
      `${walletName(w)}${usd === null ? ' — size ?' : ` — ${fmtUsd(usd)}`}`);
    const knownSizes = buyerEntries.map(e => e[1]).filter(v => v !== null);
    const largestBuy = knownSizes.length ? Math.max(...knownSizes) : null;
    const anyUnknown = buyerEntries.some(e => e[1] === null);
    const largestStr = largestBuy !== null
      ? `${fmtUsd(largestBuy)}${anyUnknown ? ' (some sizes unknown)' : ''}`
      : 'unknown';
    const symbol = info?.symbol ?? 'UNKNOWN';
    let mc = tokenMarketCap(info);
    const mcStr = mc > 0 ? fmtUsd(mc) : 'N/A';
    const _tt = trendingMap.get(tokenMint);
    const rankStr = _tt ? `#${_tt.bestRank || '?'}` : 'not trending';
    const buyerList = buyers.map(b => `  • ${b}`).join('\n');
    const signalTime = new Date().toLocaleTimeString('en-US', {
      timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true
    });

    sendTelegram(CLUSTER_SIGNAL_CHAT,
      `⚡ <b>${count} Wallet Cluster — ${symbol}</b>\n\n` +
      `Contract: <code>${tokenMint}</code>\n\n` +
      `Wallets: <b>${count}</b>\n` +
      `Chain: Solana\n` +
      `Token Age: ${fmtAge(age)}\n` +
      `Market Cap: ${mcStr}\n` +
      `Trending Rank: ${rankStr}\n` +
      `Largest Buy: <b>${largestStr}</b>\n\n` +
      `<b>Bought by:</b>\n${buyerList}\n\n` +
      `Signal Time: ${signalTime}\n\n` +
      `🔗 <a href="https://gmgn.ai/sol/token/${tokenMint}">View on GMGN</a>`
    );
    log(`[CLUSTER] 🔥 FIRED ${symbol} ${tokenMint.substring(0,8)} — ${count} wallets | largest ${largestStr} | age ${fmtAge(age)} | ${gateReason}`);
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

// ── SOL SPENT / BUY SIZE ──────────────────────────────────────
// Reinstated in 17i (was removed in the 17d signal rewrite) to support the
// cluster big-buy gate. Per the 24q/24r history: do NOT add the native and wSOL
// legs together — that double-counts roughly 2x. PREFER the wSOL swap leg; fall
// back to the native lamport delta only when there is no wSOL leg.
// Returns SOL spent (float), or null if it can't be determined.
function extractSolSpent(tx, trackedWallet) {
  const meta = tx?.meta;
  const msg  = tx?.transaction?.message;
  if (!meta || !msg) return null;

  // ── Preferred: the wSOL token leg for this wallet (pre - post, i.e. spent).
  const preBals  = meta.preTokenBalances ?? [];
  const postBals = meta.postTokenBalances ?? [];
  let wsolPre = null, wsolPost = null;
  for (const b of preBals) {
    if (b.owner === trackedWallet && b.mint === SOL_MINT) {
      wsolPre = (wsolPre ?? 0) + (parseFloat(b.uiTokenAmount?.uiAmount ?? 0) || 0);
    }
  }
  for (const b of postBals) {
    if (b.owner === trackedWallet && b.mint === SOL_MINT) {
      wsolPost = (wsolPost ?? 0) + (parseFloat(b.uiTokenAmount?.uiAmount ?? 0) || 0);
    }
  }
  if (wsolPre !== null || wsolPost !== null) {
    const spent = (wsolPre ?? 0) - (wsolPost ?? 0);
    if (spent > 0) return spent;
    // A wSOL leg that nets <= 0 means this wasn't paid in wSOL — fall through.
  }

  // ── Fallback: native SOL lamport delta for the wallet's own account.
  const keys = msg.accountKeys ?? [];
  let idx = -1;
  for (let i = 0; i < keys.length; i++) {
    const k = typeof keys[i] === 'string' ? keys[i] : keys[i]?.pubkey;
    if (k === trackedWallet) { idx = i; break; }
  }
  if (idx < 0) return null;
  const pre  = meta.preBalances?.[idx];
  const post = meta.postBalances?.[idx];
  if (pre == null || post == null) return null;
  const fee = meta.fee ?? 0;
  const lamports = pre - post - fee;   // exclude the tx fee from "spent"
  if (!(lamports > 0)) return 0;       // clean zero — a transfer-in, not a buy
  return lamports / 1e9;
}

// SOL/USD price via Jupiter's free price API (lite-api.jup.ag) — NOT GMGN.
// GMGN's rate limit is shared with the trending poller and per-token lookups; a
// per-buy price call there got the VPS IP banned (17i). Jupiter's price endpoint
// is free and unmetered for this. Cached 5 min — SOL barely moves intra-window.
// Returns 0 on failure; callers treat 0 as "unknown", never as a real price.
let solPriceCache = { v: 0, at: 0 };
async function getSolPriceUsd() {
  if (solPriceCache.v > 0 && (Date.now() - solPriceCache.at) < 300000) return solPriceCache.v;
  try {
    const j = await httpsGet('lite-api.jup.ag', `/price/v2?ids=${SOL_MINT}`);
    const p = parseFloat(j?.data?.[SOL_MINT]?.price ?? 0) || 0;
    if (p > 0) { solPriceCache = { v: p, at: Date.now() }; return p; }
  } catch (e) {}
  // Fallback: GMGN token/info (only if Jupiter fails — rare, and this is one
  // call every 5 min at most, not per buy).
  try {
    const info = await fetchTokenInfo(SOL_MINT);
    const p = tokenPriceUsd(info);
    if (p > 0) { solPriceCache = { v: p, at: Date.now() }; return p; }
  } catch (e) {}
  return 0;
}

// Given a Map(wallet -> SOL amount | 0 | null), decide whether the cluster has a
// qualifying >= $500 buy. Prices ONCE (one getSolPriceUsd call, cached 5 min).
// FAIL-OPEN: any null (unparseable buy) qualifies; if the price lookup itself
// fails, the whole cluster qualifies (can't prove it doesn't). Returns
// { qualifies, sizesStr } — sizesStr is a "$123, $45, ?" display for logs/alert.
// Revised rule (2026-08-11): count how many DISTINCT wallets EACH spent >= the
// big-buy threshold. The cluster now requires >= CLUSTER_MIN_WALLETS wallets that
// INDIVIDUALLY cleared $500 (not just one). Unknown size (null) or a failed price
// lookup counts as qualifying for that wallet (fail-open, so a parse blip can't
// suppress a real cluster). Returns { bigCount, sizesStr }.
async function clusterBigBuyerCount(solMap) {
  const price = await getSolPriceUsd();
  const parts = [];
  let bigCount = 0;
  for (const [, sol] of solMap) {
    if (sol === null || !(price > 0)) { parts.push('?'); bigCount++; continue; }  // fail-open
    const usd = sol * price;
    parts.push(`$${Math.round(usd)}`);
    if (usd >= CLUSTER_MIN_BIG_BUY_USD) bigCount++;
  }
  return { bigCount, sizesStr: parts.join(', '), price };
}

async function clusterHasBigBuySol(solMap) {
  // null (unknown) always qualifies, no price needed.
  for (const [, sol] of solMap) { if (sol === null) { /* still need sizesStr */ } }
  const price = await getSolPriceUsd();
  const parts = [];
  let qualifies = false;
  if (!(price > 0)) qualifies = true;   // price failed -> fail open
  for (const [, sol] of solMap) {
    if (sol === null) { parts.push('?'); qualifies = true; continue; }
    const usd = price > 0 ? sol * price : null;
    parts.push(usd === null ? '?' : `$${Math.round(usd)}`);
    if (usd !== null && usd >= CLUSTER_MIN_BIG_BUY_USD) qualifies = true;
  }
  return { qualifies, sizesStr: parts.join(', '), price };
}

// USD value of a single buy, for the alert display. Uses the 5-min-cached price,
// so calling it per-buyer at fire time costs at most one network call total.
async function buyUsdValue(tx, trackedWallet) {
  const sol = extractSolSpent(tx, trackedWallet);
  if (sol === null) return null;
  if (sol === 0) return 0;
  const price = await getSolPriceUsd();
  if (!(price > 0)) return null;
  return sol * price;
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
  const trackedWallet = subIdToWallet[subId];
  if (!trackedWallet) return;
  await processBuySignature(value.signature, trackedWallet);
}

// Shared buy-processing path used by BOTH the WebSocket handler and the HTTP
// poller. Given a signature + which tracked wallet it belongs to, fetch the tx,
// find the bought mint, and run the signal checks. Idempotent per (wallet,mint).
async function processBuySignature(signature, trackedWallet) {
  if (!signature || !trackedWallet) return;
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
  if (ENABLE_CLUSTER) await checkClusterSignal(trackedWallet, mint, tx);
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
  if (top1Fired.size > 20000) { top1Fired.clear(); saveSet('/tmp/sol_top1_fired.json', top1Fired); log(`[CLEANUP] top1Fired cleared`); }
  if (clusterFired.size > 20000) { clusterFired.clear(); saveSet('/tmp/sol_cluster_fired.json', clusterFired); log(`[CLEANUP] clusterFired cleared`); }
  if (whaleFired.size > 20000) { whaleFired.clear(); saveSet('/tmp/sol_whale_fired.json', whaleFired); log(`[CLEANUP] whaleFired cleared`); }
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
log(`═══ SOL BLUECHIP TRENDING BOT — VERSION 2026-08-12f ═══`);
log(`[START] ${WALLETS.length} wallets | SOLE SIGNAL: tracked buy + top-${TREND_TOP_N} trending (any interval) + age < ${TREND_MAX_TOKEN_AGE/3600}h + bluechip > ${(TREND_MIN_BLUECHIP*100).toFixed(0)}%`);
log(`[START] Signal chat: ${TREND_SIGNAL_CHAT} | Trending refresh: every ${TREND_POLL_SECS}s across [${TREND_INTERVALS.join(', ')}]`);
log(`[START] WSS chain: ${WSS_ENDPOINTS.map(e => e.name).join(' -> ')}`);
log(`[START] Signals: bluechip=ON, #1-everywhere=ON, cluster(5w/$500/60m)=${ENABLE_CLUSTER ? 'ON' : 'OFF'}, whale-holder(>5k/60m)=${ENABLE_CLUSTER ? 'ON' : 'OFF'} | buy-detection=${BUY_MODE}`);

https.get('https://api.ipify.org?format=json', (res) => {
  let d = ''; res.on('data', c => d += c);
  res.on('end', () => { try { log(`[IP] ${JSON.parse(d).ip}`); } catch {} });
}).on('error', () => {});

// Prime the trending cache immediately, then refresh on a timer.
refreshTrending().then(() => {
  log(`[TREND] initial load: ${trendingMap.size} tokens`);
});
setInterval(refreshTrending, TREND_POLL_SECS * 1000);

// ── HTTP BUY POLLER ───────────────────────────────────────────
// Watches all wallets via getSignaturesForAddress over HTTP — the free-tier-safe
// alternative to logsSubscribe (which every free WSS endpoint gates). Each cycle,
// for each wallet, fetch signatures newer than the last one we saw and process
// them. Paced so 94 wallets spread across the cycle instead of bursting.
let lastSeenSig = {};   // wallet -> most recent signature already processed
let pollerRunning = false;

async function pollWalletsForBuys() {
  if (pollerRunning) return;   // don't overlap cycles if one runs long
  pollerRunning = true;
  try {
    if (!isActiveHours()) return;
    const perWalletGapMs = Math.max(50, Math.floor((BUY_POLL_SECS * 1000) / Math.max(1, WALLETS.length)) - 20);
    let newBuys = 0, rpcFails = 0;
    for (const wallet of WALLETS) {
      const prev = lastSeenSig[wallet];
      const sigs = await getSignaturesForAddress(wallet, prev);
      if (sigs === null) { rpcFails++; await sleep(perWalletGapMs); continue; }
      if (sigs.length > 0) {
        // Newest is first. Update cursor to the newest signature immediately.
        lastSeenSig[wallet] = sigs[0].signature;
        if (prev) {
          // Process oldest->newest so ordering matches real buy order. Skip errored txs.
          const fresh = sigs.filter(x => !x.err).reverse();
          for (const x of fresh) { await processBuySignature(x.signature, wallet); newBuys++; }
        }
        // If prev was undefined this was just the seeding pass — cursor set, no processing.
      }
      await sleep(perWalletGapMs);
    }
    if (rpcFails > 0) log(`[POLL] cycle done — ${newBuys} new buys, ${rpcFails}/${WALLETS.length} wallets had RPC failures`);
  } catch (e) {
    log(`[POLL] error: ${e.message}`);
  } finally {
    pollerRunning = false;
  }
}

// Start buy detection in the configured mode.
if (BUY_MODE === 'POLL') {
  log(`[START] Buy detection: HTTP POLL every ${BUY_POLL_SECS}s across ${WALLETS.length} wallets (free-tier safe, no WebSocket)`);
  // First cycle seeds cursors silently (no alerts for pre-existing history), then polls.
  pollWalletsForBuys().then(() => log(`[POLL] cursors seeded — now watching for new buys`));
  setInterval(pollWalletsForBuys, BUY_POLL_SECS * 1000);
} else {
  log(`[START] Buy detection: WebSocket (logsSubscribe)`);
  connect();
}

// Self-ping (Render only — harmless on a VPS where RENDER_EXTERNAL_URL is unset)
if (RENDER_URL) {
  setInterval(() => {
    const mod = RENDER_URL.startsWith('https') ? https : http;
    mod.get(RENDER_URL + '/', res => log(`[PING] ${res.statusCode}`))
      .on('error', e => log(`[PING] ${e.message}`));
  }, 10 * 60_000);
}
