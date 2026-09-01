// ============================================================
//  SOLANA COMBINED BOT
//  ----------------------------------------------------------
//  >>> VERSION: 2026-08-28a  (FOMO trader list + 3-entry/24h-from-mint signal); prior: 2026-08-12b  (order_by stays 'volume' — 12a's change was wrong; adds hot_level capture) <<<
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
// Intervals the #1-EVERYWHERE signal must ALL be rank 1 on. Narrowed 2026-08-12
// from all five to the three SHORT windows at N's request — 6h/24h rarely agree
// with the fast ones, so requiring all five almost never fired.
// DELIBERATELY SEPARATE from TREND_INTERVALS: the poller still FETCHES all five
// (the bluechip signal's bestRank and "Trending in: ..." depend on 6h/24h); this
// only narrows what the #1 signal is judged on. Env: TOP1_INTERVALS=1m,5m,1h
const TOP1_INTERVALS = (process.env.TOP1_INTERVALS || '1m,5m,1h')
  .split(',').map(x => x.trim()).filter(Boolean);
// #1-EVERYWHERE also now requires >= 1 buy of this size from a TRACKED wallet.
const TOP1_MIN_BIG_BUY_USD = parseFloat(process.env.TOP1_MIN_BIG_BUY_USD || '500');
// How many DISTINCT tracked wallets must EACH clear TOP1_MIN_BIG_BUY_USD.
// Raised 1 -> 3 on 2026-08-12 at N's request. Counted per WALLET, not per buy:
// one wallet buying $2000 counts as 1, not 4.
const TOP1_MIN_BIG_BUYS = parseInt(process.env.TOP1_MIN_BIG_BUYS || '3', 10);
// Max token age for the #1 signal. Was hardcoded inline as 48*3600; named here
// so the buy window below can be DERIVED from it and the two cannot drift.
const TOP1_MAX_AGE_SECS = parseInt(process.env.TOP1_MAX_AGE || '172800', 10);   // 48h
// The qualifying tracked buy may have happened ANY TIME FROM MINT up to the
// moment the intervals line up — so the window is simply the token's maximum
// eligible age, not an independent number. (12i used an arbitrary 6h here;
// that was wrong and would have rejected a buy made early in a token's run.)
const TOP1_BUY_WINDOW_MS = parseInt(process.env.TOP1_BUY_WINDOW_MS || String(TOP1_MAX_AGE_SECS * 1000), 10);
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
// SMART MONEY / KOL floor on the POOL (added 2026-08-12 at N's request).
// Sent as the rank endpoint's server-side min_smart_degen_count /
// min_renowned_count, so GMGN drops thin tokens BEFORE the top-N cut and the
// bot's 10 slots go to tokens with real smart-money and KOL participation.
// Applies to the whole trendingMap, so BOTH the bluechip signal and
// #1-everywhere inherit it — no per-signal code needed.
// NOTE: this deliberately makes the bot's pool STRICTER than gmgn.ai's Trending
// tab, which has no such filter, so the two lists will no longer match
// one-for-one. Intended. Set either to 0 to disable that bound.
const TREND_MIN_SMART = parseInt(process.env.TREND_MIN_SMART || '2', 10);   // >= 2 smart-money holders
const TREND_MIN_KOL   = parseInt(process.env.TREND_MIN_KOL   || '2', 10);   // >= 2 KOL / renowned holders

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
const ENABLE_CLUSTER = (process.env.ENABLE_CLUSTER || '1') === '1';
// Per-signal kill switches. Default ON, so setting nothing keeps today's
// behaviour exactly. Set to 0 in .env-vars to silence a signal without touching
// code — ENABLE_CLUSTER already covers cluster AND whale-holder, these two cover
// the rest. With all of them off the bot still runs the FOMO signal.
const ENABLE_TREND = (process.env.ENABLE_TREND || '1') === '1';   // bluechip trending
const ENABLE_TOP1  = (process.env.ENABLE_TOP1  || '1') === '1';   // #1-everywhere   // re-enabled 2026-08-11 with revised rule
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

// ── SIGNAL 5: FOMO TRADER ENTRY ──────────────────────────────────────────────
// FOMO_MIN_WALLETS distinct tracked wallets take an entry in the same token, and
// every one of those entries lands within FOMO_MAX_MINT_AGE of the mint. No
// trending / MC / volume gate — the wallets ARE the thesis. The Telegram-call
// gate still applies, same as every other signal.
const ENABLE_FOMO       = (process.env.ENABLE_FOMO || '1') === '1';
const FOMO_MIN_WALLETS  = parseInt(process.env.FOMO_MIN_WALLETS || '2', 10);
const FOMO_MAX_MINT_AGE = parseInt(process.env.FOMO_MAX_MINT_AGE || '86400', 10); // seconds from mint
const FOMO_MIN_BUY_USD  = parseFloat(process.env.FOMO_MIN_BUY_USD || '0');        // 0 = any size
const FOMO_SIGNAL_CHAT  = process.env.FOMO_SIGNAL_CHAT || '-5174318212';   // dedicated FOMO chat
// How many DISTINCT telegram channels must have called the mint. 1 = the old
// behaviour (any single call). 2+ means two independent channels.
const FOMO_MIN_CALLS    = parseInt(process.env.FOMO_MIN_CALLS || '2', 10);
// Count "$TICKER" calls toward the gate, not just mints. Requires a tracker new
// enough to write a `tickers` map into the feed; harmless without one.
const TG_MATCH_TICKERS  = (process.env.TG_MATCH_TICKERS || '1') === '1';
const TG_TICKER_MIN_LEN = parseInt(process.env.TG_TICKER_MIN_LEN || '3', 10);
// WHALE HOLDER extra gates (added 2026-08-12 at N's request): on top of
// >5000 holders + <60min + #1 trending, the token must ALSO have >=2 smart-money
// holders, >=2 KOL holders, and at least ONE buy >= $500 from a TRACKED wallet.
// NOTE this makes the signal no longer purely poll-driven — it now also needs a
// tracked-wallet buy, so it depends on buy detection (BUY_MODE) being healthy.
// ── AIRDROP FILTER (2026-08-21) ──
// Holder count alone cannot tell a bought-into token from an airdropped one.
// Two ratios do:
//   1. MARKET CAP PER HOLDER — a real holder paid for something worth real money;
//      airdrop recipients hold dust (observed on BSC: 278,569 holders on $28,648
//      MC = $0.10 each; 1,210,331 on $56,755 = $0.05).
//   2. HOLDERS PER BUY — every real holder generated a buy, so this sits near 1;
//      an airdrop mints hundreds of thousands of holders from a few transactions.
// Set either to 0 to disable that check.
const WHALE_MIN_MC_PER_HOLDER   = parseFloat(process.env.WHALE_MIN_MC_PER_HOLDER || '10');
const WHALE_MAX_HOLDERS_PER_BUY = parseFloat(process.env.WHALE_MAX_HOLDERS_PER_BUY || '3');
const WHALE_MAX_HOLDERS         = parseInt(process.env.WHALE_MAX_HOLDERS || '100000', 10);
const WHALE_MIN_SMART       = parseInt(process.env.WHALE_MIN_SMART || '2', 10);
const WHALE_MIN_KOL         = parseInt(process.env.WHALE_MIN_KOL   || '2', 10);
const WHALE_MIN_BIG_BUY_USD = parseFloat(process.env.WHALE_MIN_BIG_BUY_USD || '500');
// Window for that tracked buy. Matches WHALE_MAX_AGE (60 min).
const WHALE_BUY_WINDOW_MS   = parseInt(process.env.WHALE_BUY_WINDOW_MS || '3600000', 10);
// trackedBuysWhale is shared by the whale (60m) and #1 (6h) gates, so it must be
// pruned at the LONGER of the two — pruning at the shorter one would silently
// starve the longer-window consumer. Declared here, AFTER both deps exist.
const TRACKED_BUY_RETENTION_MS = Math.max(WHALE_BUY_WINDOW_MS, TOP1_BUY_WINDOW_MS);
const TRACKED_BUY_MAX_TOKENS = parseInt(process.env.TRACKED_BUY_MAX_TOKENS || '20000', 10);

// ══════════════════════════════════════════════════════════════
//  TELEGRAM CALL GATE (2026-08-12m)
// ══════════════════════════════════════════════════════════════
// Every signal additionally requires the mint to have been CALLED in one of N's
// Telegram channels. This bot cannot read Telegram; profit_tracker.py runs a
// Telethon USER session across ~55 channels and writes TG_CALLS_FILE. We only
// read that file — no network, no credentials, no Telegram code here.
// A call may arrive BEFORE or AFTER the on-chain conditions, so a signal whose
// on-chain side is satisfied but has no call yet is held PENDING and re-checked.
const TG_CALLS_FILE = process.env.TG_CALLS_FILE || '/tmp/tg_calls.json';
// Feed older than this => tracker presumed down => FAIL OPEN (fire, flagged)
// rather than silently muting every signal.
const TG_CALLS_MAX_STALE_SEC = parseInt(process.env.TG_CALLS_MAX_STALE_SEC || '900', 10);
const TG_RECHECK_SECS = parseInt(process.env.TG_RECHECK_SECS || '30', 10);
const TG_GATE_ENABLED = (process.env.TG_GATE_ENABLED || '1') === '1';
// FAIL-CLOSED (changed 2026-08-21 at N's request: "i don't need the signal
// without the telegram calls"). Feed missing or stale => signals are HELD, not
// fired. The danger is silence — a dead tracker muting everything unnoticed — so
// an outage is announced to Telegram instead (see the watchdog below), and held
// signals still fire later if the feed returns while they still qualify.
// Set TG_FAIL_OPEN=1 to revert to firing unchecked when the feed is down.
const TG_FAIL_OPEN = (process.env.TG_FAIL_OPEN || '0') === '1';
const TG_DOWN_NOTIFY_SECS = parseInt(process.env.TG_DOWN_NOTIFY_SECS || '1800', 10);
let _tgDownSince = 0, _tgDownNotifiedAt = 0;

// Bluechip trending signal now needs this many DISTINCT tracked wallets (was 1).
const TREND_MIN_WALLETS   = parseInt(process.env.TREND_MIN_WALLETS || '2', 10);
// Volume gate shared by the cluster signal: token qualifies if it is trending
// (top-N any interval) OR its latest 5-minute candle volume (USD) >= this.
const VOL_GATE_USD        = parseFloat(process.env.VOL_GATE_USD || '100000');   // $100k 5-min volume

// ── RPC ───────────────────────────────────────────────────────
// HTTP RPC endpoints, tried IN ORDER, first success wins.
// 2026-08-12: this list used to be hardcoded with HELIUS FIRST and had no env
// control — WSS_ORDER only ever governed WebSockets. That is why BUY_MODE=POLL
// silently burned Helius credits: 94 wallets x getSignaturesForAddress every 60s
// = ~135k paid calls/day, all landing on Helius, versus ~free logsSubscribe.
// HTTP_RPC_ORDER now controls it. Default keeps the historical order; set
// HTTP_RPC_ORDER=PUBLIC,HELIUS to keep polling off the paid endpoint.
const HTTP_RPC_DEFS = {
  HELIUS:  HELIUS_API_KEY  ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : null,
  SHYFT:   SHYFT_API_KEY   ? `https://rpc.shyft.to?api_key=${SHYFT_API_KEY}` : null,
  ALCHEMY: ALCHEMY_API_KEY ? `https://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}` : null,
  PUBLIC:  'https://api.mainnet-beta.solana.com',
};
const HTTP_RPC_ORDER = (process.env.HTTP_RPC_ORDER || 'HELIUS,SHYFT,ALCHEMY,PUBLIC')
  .split(',').map(x => x.trim().toUpperCase()).filter(Boolean);
const HTTP_RPCS = HTTP_RPC_ORDER.map(n => HTTP_RPC_DEFS[n]).filter(Boolean);
const HTTP_RPC_NAMES = HTTP_RPC_ORDER.filter(n => HTTP_RPC_DEFS[n]);
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
const LEGACY_WALLETS = [
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
// FOMO leaderboard traders (fomo.family), Solana side. Collected 2026-08-28.
// ADDITIVE — these do NOT replace the list above. They feed exactly one signal,
// the FOMO entry signal; the original signals still judge on LEGACY_WALLETS only.
const FOMO_WALLETS = [
  "DrXBn8eeRZXsc1h7qsj4V1W38at1sEynyZNC8mVrbuV5",  // AdmiralFinest
  "H2QSGECp13sFLJgdTsDtayX3dk18Dm6sQMSQKcew7Xzk",  // AJC
  "DVFYHVKFYLxws4bV97va6EceVRrKjddHSWYq3is4ad49",  // Altcoinist
  "9BMzTpSo4URse1oN666pmexhdjpU1vA5p7LtroCFQdLU",  // Aurelius
  "8xL8S7P4QLdTGRquHas8NP5EVjp2qUGbmSgrkh97mvmq",  // Avast
  "HWYpE693cw8AWxHSynrHhUsKDbfzubAFYWpTcMZdNWKR",  // B The Bezel
  "4ZZW5ePCAsHJdnHHUdgozdvLqYNVnkE9CgrW3iyZdskb",  // Bluntz
  "F11mt2PsYTR7RF7hzfvFyeFbWREiCS3soa5pGbPX88Lb",  // bull.path
  "GFRjGNXY8JrGSPC46inqrH4XPdUFMDLkE1oNm1nXiPsJ",  // Burgz
  "J9WiAZKf8JnCkHFL8fLCCXdEgdoLjLRqU2EGsDjdqYga",  // Change
  "HvkKCEUWFYK1ZdzoshDYFddLHoVyECwFReo9qxi6ioD7",  // Claymore
  "FcTTjHafvSrXu11BLGBRsLoKGbVnLgkZxz3hACpwN4A8",  // Conviction
  "6b5JivZqr5G8SCjgDQxqm8DgtiN4PufJRFS8yCWSFxRk",  // Cryptoolin
  "9QXT3u8x98z2RaDzRacopXJpBYbKrkG3Sjck868jpDou",  // Dtrain22
  "5FGoPPj1nL8LCnfVnpTmreqQtqLuMXXAwuS1uahMrp8V",  // DumbCrayonEater
  "GpHkiRzoJDNPpDVgMx2E3xamrxogHKGaZg4HahQxhRVB",  // Dxrant
  "2xUbYAVq1oJGj45d6JjnaYHAke3NQecUcqWvvVbwmYw8",  // Ethermonk
  "5VRgqb2qbVqaWVGsM2k1b2bnPJk7up2xYbn4ziEjFgNt",  // Figaro
  "DAejzMs5cUeCCENNvapy9KWFwzwegh7LvcgNkZ6hnf1y",  // FomoPumpGuy
  "498g1rVnFcnjBjpfw1xyqA1WvgQXUU8RWuELjxkjAayQ",  // Frank
  "C481ZKiw4TZGs4EjNkhm6TB7s17swzmqAmCcNm73vGnW",  // Guava Guy
  "8QrAeBvyNj1f54pMwqKkxXXK95bStDZRcqGEcp3nSa4s",  // IKnowWhyy
  "33vFB2rtG9FDpJReNaJrBF4RZgri5HTVo5gSxP1XfVCr",  // Insentos
  "CBKJmhjDp7mxBYGAFWxCaPjD2BSJWpb4nQqnLd7rhSaM",  // InYourWalls
  "EVqxB3F6iUBeWsTpBFQqWwxpqUS8s4NrzgxBvQ2VRbTq",  // Jack
  "2QQFwT3QV1LrH9vvaUiGjhpNCmGqVDxRAkmGKvjruBUS",  // King
  "EJ1izM56eBS5baVLk7Q4iaQvsgCPa3W5bZgc44wwHz8U",  // Kyle
  "3qBVJLMAWXCbW3nhz99T99in82zZeUHkbDeSDJThbN4Z",  // Leo
  "9EhcDGGJ6NpetxNumZKwD1o1kuHBwvfNGKrmS6zekP7C",  // LordArbiter
  "GAsnqm4XkNkPVgrAofNQ65jWf8f3tKCLHhE9ZqSy2AP1",  // Nach
  "DCeH3aCsstGUSxQqS72VBZwTydoor1nQ6dWaxrgGQk39",  // Nate
  "2MxSnHSRgfEKLe9noU9wZSA3AYR9FnfgdrLefg7wCMC8",  // Onchainmetrics
  "9zZCjLr9xfXfp3qdqvPh6YeaEaagepz21khLhca69B18",  // Paterniq
  "2VR13Sh1zxPjP2ufj94CkzigTT9kokBV2Ffu1PqVegRf",  // Picadura
  "HDixbrzwwLXczhDBk1JVrurPQsuLE8FUKnW2pucSXN3o",  // PoorGoat
  "8f39XhhZoRD8sYb6K5K9N7iSHXkL7BDmFFQNDF3TtsEr",  // Qwerty
  "CzU8MaRcwvwUoNkwJFLbvtFWJugcEXAhDDQqNFE4ybb7",  // Rowdy
  "Ak6gsstZwaRDYKnzdyNg2HvCXDFvv21afjVix9VGRMQv",  // Rugdalio
  "2GtmuqG31LuuWQmKzeK72YjH5iALPkJycuQaEQGsFzLw",  // SadCrissy
  "2yXwy5Dsa1XtEXcsrkFVRJeyuWD3qKkMN3pP3p5VTW3V",  // Salem
  "mDR91ufq4S2uSHa9E2PYL7xg4LwcQXaQFP7YK864a1z",  // Smol Intern
  "5opd5KBmodmoNuAThQ5cmXKbRxHbQDfomWGKAs3uEUP9",  // Soby
  "m4CkbwCZbbmEXB2EJhzQmAVX5LikLsyTozqwxXA9wEk",  // Still in the game
  "CpKuhcFHogvrq7Fx3enu57xTRkh1WyzER1TVVBimC5mo",  // Swizzle
  "4ugDhHJ8XDXAeABmrNmGffFaLbJb9BkPyiFGVSV9ocwo",  // TheSolstice
  "9QuHMjmYXxhqCPmNYnrjviD5SPNbFnXCt8SYDcZnhq8q",  // TrenchHog
  "2heJbC32Tpfcb3nbUb5ER61K11FGZVfVGtVnDm6LDogF",  // Unipcs
  "F5hkYsi8JxjyA2JHN5CA7MbnnhWubkXB2ZQB7Gkaxqs6",  // Vee
  "26W5Tq8LUTWct7pWbAHsmvsxQkXxPghbdwXj6MYiwQLU",  // Vydamo
  "7iPPqPyrqcmfenRs4xZ72ab4pyuUofXB5YaQB83WJmT9",  // Wood
];
// The two lists drive DIFFERENT signals and are deliberately not merged into one
// pool. LEGACY_WALLETS keep feeding the signals they have always fed (bluechip,
// cluster, #1-trending, whale); FOMO_WALLETS feed ONLY the FOMO entry signal.
// WALLETS is just the union — the addresses worth watching on-chain. Which
// signal a buy counts toward is decided per buy by the two Sets below.
const LEGACY_ADDR_SET = new Set(LEGACY_WALLETS);
const FOMO_ADDR_SET   = new Set(FOMO_WALLETS);
// Watch only the wallets a LIVE signal consumes. Subscribing to wallets nothing
// reads is not free: the public RPC cuts the socket at ~100 logsSubscribe calls,
// so carrying 94 unused legacy wallets alongside the 50 FOMO ones put us at 144
// and the bot could never finish subscribing ("closed mid-subscribe at 101/144").
const _needLegacy = ENABLE_TREND || ENABLE_TOP1 || ENABLE_CLUSTER;
const WALLETS = [...new Set([
  ...(_needLegacy ? LEGACY_WALLETS : []),
  ...(ENABLE_FOMO ? FOMO_WALLETS   : []),
])];
const WALLET_SET = new Set(WALLETS);

// Wallet name lookup — all known names
const WALLET_NAMES = {
  // ── FOMO leaderboard traders (2026-08-28) ──
  "DrXBn8eeRZXsc1h7qsj4V1W38at1sEynyZNC8mVrbuV5": "AdmiralFinest",
  "H2QSGECp13sFLJgdTsDtayX3dk18Dm6sQMSQKcew7Xzk": "AJC",
  "DVFYHVKFYLxws4bV97va6EceVRrKjddHSWYq3is4ad49": "Altcoinist",
  "9BMzTpSo4URse1oN666pmexhdjpU1vA5p7LtroCFQdLU": "Aurelius",
  "8xL8S7P4QLdTGRquHas8NP5EVjp2qUGbmSgrkh97mvmq": "Avast",
  "HWYpE693cw8AWxHSynrHhUsKDbfzubAFYWpTcMZdNWKR": "B The Bezel",
  "4ZZW5ePCAsHJdnHHUdgozdvLqYNVnkE9CgrW3iyZdskb": "Bluntz",
  "F11mt2PsYTR7RF7hzfvFyeFbWREiCS3soa5pGbPX88Lb": "bull.path",
  "GFRjGNXY8JrGSPC46inqrH4XPdUFMDLkE1oNm1nXiPsJ": "Burgz",
  "J9WiAZKf8JnCkHFL8fLCCXdEgdoLjLRqU2EGsDjdqYga": "Change",
  "HvkKCEUWFYK1ZdzoshDYFddLHoVyECwFReo9qxi6ioD7": "Claymore",
  "FcTTjHafvSrXu11BLGBRsLoKGbVnLgkZxz3hACpwN4A8": "Conviction",
  "6b5JivZqr5G8SCjgDQxqm8DgtiN4PufJRFS8yCWSFxRk": "Cryptoolin",
  "9QXT3u8x98z2RaDzRacopXJpBYbKrkG3Sjck868jpDou": "Dtrain22",
  "5FGoPPj1nL8LCnfVnpTmreqQtqLuMXXAwuS1uahMrp8V": "DumbCrayonEater",
  "GpHkiRzoJDNPpDVgMx2E3xamrxogHKGaZg4HahQxhRVB": "Dxrant",
  "2xUbYAVq1oJGj45d6JjnaYHAke3NQecUcqWvvVbwmYw8": "Ethermonk",
  "5VRgqb2qbVqaWVGsM2k1b2bnPJk7up2xYbn4ziEjFgNt": "Figaro",
  "DAejzMs5cUeCCENNvapy9KWFwzwegh7LvcgNkZ6hnf1y": "FomoPumpGuy",
  "498g1rVnFcnjBjpfw1xyqA1WvgQXUU8RWuELjxkjAayQ": "Frank",
  "C481ZKiw4TZGs4EjNkhm6TB7s17swzmqAmCcNm73vGnW": "Guava Guy",
  "8QrAeBvyNj1f54pMwqKkxXXK95bStDZRcqGEcp3nSa4s": "IKnowWhyy",
  "33vFB2rtG9FDpJReNaJrBF4RZgri5HTVo5gSxP1XfVCr": "Insentos",
  "CBKJmhjDp7mxBYGAFWxCaPjD2BSJWpb4nQqnLd7rhSaM": "InYourWalls",
  "EVqxB3F6iUBeWsTpBFQqWwxpqUS8s4NrzgxBvQ2VRbTq": "Jack",
  "2QQFwT3QV1LrH9vvaUiGjhpNCmGqVDxRAkmGKvjruBUS": "King",
  "EJ1izM56eBS5baVLk7Q4iaQvsgCPa3W5bZgc44wwHz8U": "Kyle",
  "3qBVJLMAWXCbW3nhz99T99in82zZeUHkbDeSDJThbN4Z": "Leo",
  "9EhcDGGJ6NpetxNumZKwD1o1kuHBwvfNGKrmS6zekP7C": "LordArbiter",
  "GAsnqm4XkNkPVgrAofNQ65jWf8f3tKCLHhE9ZqSy2AP1": "Nach",
  "DCeH3aCsstGUSxQqS72VBZwTydoor1nQ6dWaxrgGQk39": "Nate",
  "2MxSnHSRgfEKLe9noU9wZSA3AYR9FnfgdrLefg7wCMC8": "Onchainmetrics",
  "9zZCjLr9xfXfp3qdqvPh6YeaEaagepz21khLhca69B18": "Paterniq",
  "2VR13Sh1zxPjP2ufj94CkzigTT9kokBV2Ffu1PqVegRf": "Picadura",
  "HDixbrzwwLXczhDBk1JVrurPQsuLE8FUKnW2pucSXN3o": "PoorGoat",
  "8f39XhhZoRD8sYb6K5K9N7iSHXkL7BDmFFQNDF3TtsEr": "Qwerty",
  "CzU8MaRcwvwUoNkwJFLbvtFWJugcEXAhDDQqNFE4ybb7": "Rowdy",
  "Ak6gsstZwaRDYKnzdyNg2HvCXDFvv21afjVix9VGRMQv": "Rugdalio",
  "2GtmuqG31LuuWQmKzeK72YjH5iALPkJycuQaEQGsFzLw": "SadCrissy",
  "2yXwy5Dsa1XtEXcsrkFVRJeyuWD3qKkMN3pP3p5VTW3V": "Salem",
  "mDR91ufq4S2uSHa9E2PYL7xg4LwcQXaQFP7YK864a1z": "Smol Intern",
  "5opd5KBmodmoNuAThQ5cmXKbRxHbQDfomWGKAs3uEUP9": "Soby",
  "m4CkbwCZbbmEXB2EJhzQmAVX5LikLsyTozqwxXA9wEk": "Still in the game",
  "CpKuhcFHogvrq7Fx3enu57xTRkh1WyzER1TVVBimC5mo": "Swizzle",
  "4ugDhHJ8XDXAeABmrNmGffFaLbJb9BkPyiFGVSV9ocwo": "TheSolstice",
  "9QuHMjmYXxhqCPmNYnrjviD5SPNbFnXCt8SYDcZnhq8q": "TrenchHog",
  "2heJbC32Tpfcb3nbUb5ER61K11FGZVfVGtVnDm6LDogF": "Unipcs",
  "F5hkYsi8JxjyA2JHN5CA7MbnnhWubkXB2ZQB7Gkaxqs6": "Vee",
  "26W5Tq8LUTWct7pWbAHsmvsxQkXxPghbdwXj6MYiwQLU": "Vydamo",
  "7iPPqPyrqcmfenRs4xZ72ab4pyuUofXB5YaQB83WJmT9": "Wood",
  // ── legacy ──
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
// trackedBuysWhale: mint -> Map(walletAddr -> { ts, sol })
// SEPARATE from clusterBuyers on purpose. clusterBuyers has no per-entry
// timestamp (so it can't be windowed), is only written when ENABLE_CLUSTER is on,
// and is cleared wholesale by the size-based cleanup — any of which would make
// the whale-holder signal miss buys for reasons that have nothing to do with it.
// This store is written on EVERY tracked buy and pruned at WHALE_BUY_WINDOW_MS.
let trackedBuysWhale = {};
// Pending signals: on-chain side satisfied, waiting on a Telegram call.
let pendingTop1  = {};   // mint -> { since }
let pendingTrend = {};   // mint -> { buyerName, since }
let _tgCache = { mtime: -1, data: null };
let clusterFired    = loadSet('/tmp/sol_cluster_fired.json');  // tokens that already fired the cluster signal
let whaleFired      = loadSet('/tmp/sol_whale_fired.json');    // tokens that already fired the whale-holder signal
let fomoFired       = loadSet('/tmp/sol_fomo_fired.json');     // tokens that already fired the FOMO-trader signal
// fomoBuyers: mint -> Map(wallet -> { ts, sol })
// Separate from clusterBuyers (no timestamps, cleared wholesale) and from
// trackedBuysWhale (pruned at 60m) because this signal needs a 24h memory: a
// token minted this morning can collect its third entry tonight.
let fomoBuyers      = {};
// FOMO signals that are on-chain complete but short of telegram calls. Without a
// registry + timer, a second channel calling AFTER the third trader bought would
// sit unseen until a fourth trader happened to buy — which with a 24h window may
// never happen.
let pendingFomo     = {};
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
      ...(TREND_MIN_SMART > 0 ? { min_smart_degen_count: String(TREND_MIN_SMART) } : {}),
      ...(TREND_MIN_KOL   > 0 ? { min_renowned_count:    String(TREND_MIN_KOL)   } : {}),
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
    const okSet = new Set();   // WHICH intervals reported this cycle (was a bare count)

    for (const interval of TREND_INTERVALS) {
      const rows = await fetchTrendingInterval(interval);
      if (rows.length) okSet.add(interval);
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
            buys:     parseInt(t.buys ?? 0, 10) || 0,
            swaps:    parseInt(t.swaps ?? 0, 10) || 0,
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
      // Strict: every interval the #1 signal JUDGES ON must have reported this
      // cycle. Previously required all five; now exactly the TOP1_INTERVALS set,
      // since 6h/24h no longer affect the verdict.
      if (TOP1_INTERVALS.every(iv => okSet.has(iv))) {
        // First full pass after boot primes SILENTLY (records current #1s without
        // alerting). Every pass after fires normally.
        if (ENABLE_TOP1) await checkTop1Everywhere(next, !top1Primed);
        await checkWhaleHolders(next);   // async since 12h (prices a tracked buy); no-ops when ENABLE_CLUSTER is off
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
        log(`[TREND] refreshed: ${next.size} unique tokens across ${okSet.size}/${TREND_INTERVALS.length} intervals | ${eligible} currently meet age<${TREND_MAX_TOKEN_AGE/3600}h + bluechip>${(TREND_MIN_BLUECHIP*100).toFixed(0)}%`);
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
// ── TELEGRAM CALL FEED ────────────────────────────────────────────────────────
// Reads the file profit_tracker.py writes. Cached on mtime so the 30s re-check
// loop does not re-parse an unchanged file. Never throws.
function readTgCalls() {
  try {
    const st = fs.statSync(TG_CALLS_FILE);
    if (_tgCache.data && st.mtimeMs === _tgCache.mtime) return _tgCache.data;
    const parsed = JSON.parse(fs.readFileSync(TG_CALLS_FILE, 'utf8'));
    _tgCache = { mtime: st.mtimeMs, data: parsed };
    return parsed;
  } catch (e) {
    return null;   // missing / unreadable / mid-write — treat as tracker down
  }
}
// Has this mint been called? Returns { ok, stale, detail }.
// FAIL-OPEN: missing or stale feed => ok:true, stale:true (fires, flagged).
// A FRESH feed that simply lacks the mint is a real "no call" => ok:false.
// CASE: Solana mints are base58 and CASE-SENSITIVE — the tracker stores them
// with exact case, so we look up exactly. (Only EVM addresses get lowercased.)
// Was this mint called in telegram, by enough distinct channels?
//
//   minChannels — how many DISTINCT channels must have called it. 1 (the default)
//                 is the original behaviour: any single call opens the gate.
//   symbol      — the token's ticker. Plenty of channels call a token as
//                 "$ROBINCAT" and never post the mint, so those calls are
//                 invisible if we only match addresses. When a symbol is given,
//                 channels that called the CASHTAG are unioned with channels that
//                 called the mint, and the threshold applies to the union. One
//                 channel posting both still counts once.
//
// Cashtags are ambiguous — two tokens can share a ticker. Tolerable here because
// this gate only ADDS evidence on top of the bot's own on-chain conditions.
function tgCallCheck(mint, minChannels = 1, symbol = null) {
  if (!TG_GATE_ENABLED) return { ok: true, stale: false, detail: 'gate disabled' };
  const feed = readTgCalls();
  if (!feed) return { ok: TG_FAIL_OPEN, stale: true, down: true, detail: 'call feed unavailable — tracker down' };
  const age = Math.floor(Date.now() / 1000) - (parseInt(feed.updated, 10) || 0);
  const e = (feed.calls || {})[mint];

  const chanSet = new Set();
  let caCount = 0, tickerCount = 0, tickerName = null, firstTs = 0;
  if (e) {
    caCount = e.count || 0;
    firstTs = e.first || 0;
    const list = e.channels || [];
    // Older entries may predate channel tracking; count them as one, not zero.
    if (list.length) for (const c of list) chanSet.add(c);
    else if (caCount > 0) chanSet.add('(channel unrecorded)');
  }
  if (TG_MATCH_TICKERS && symbol) {
    const t = String(symbol).replace(/^\$/, '').trim().toUpperCase();
    if (t.length >= TG_TICKER_MIN_LEN) {
      const te = (feed.tickers || {})[t];
      if (te) {
        tickerCount = te.count || 0;
        tickerName = t;
        if (!firstTs || (te.first && te.first < firstTs)) firstTs = te.first || firstTs;
        for (const c of (te.channels || [])) chanSet.add(c);
      }
    }
  }

  const nChans = chanSet.size;
  const need = Math.max(1, minChannels);
  if (nChans > 0) {
    const names = [...chanSet];
    const chans = names.slice(0, 3).join(', ') + (names.length > 3 ? ' +more' : '');
    if (nChans < need) {
      return { ok: false, stale: false, channels: nChans,
               detail: `only ${nChans}/${need} channels called it — ${chans}` };
    }
    const parts = [];
    if (caCount) parts.push(`${caCount} by contract`);
    if (tickerCount) parts.push(`${tickerCount} by $${tickerName}`);
    const mins = firstTs ? Math.max(0, Math.floor((Date.now() / 1000 - firstTs) / 60)) : null;
    return { ok: true, stale: false, channels: nChans,
             detail: `${parts.join(' + ')} across ${nChans} channel(s) — ${chans}${mins === null ? '' : ` (first ${mins}m ago)`}` };
  }

  // Nothing called it. Distinguish "genuinely uncalled" from "the tracker died",
  // because fail-open only applies to the second.
  if (age > TG_CALLS_MAX_STALE_SEC) {
    return { ok: TG_FAIL_OPEN, stale: true, down: true, detail: `call feed stale ${Math.round(age / 60)}m — tracker down` };
  }
  return { ok: false, stale: false, detail: 'no telegram call yet' };
}

// Does any TRACKED wallet have a buy >= minUsd on this mint inside the window?
// Reads trackedBuysWhale (see its declaration for why it is separate).
// Returns { ok, detail }. FAIL-OPEN when size is unknown (unparseable tx) or the
// SOL price lookup fails — a dead price feed must never silently mute the signal.
async function whaleTrackedBigBuy(mint, minUsd, windowMs, minCount) {
  const need = Math.max(1, minCount || 1);
  const bucket = trackedBuysWhale[mint];
  if (!bucket || bucket.size === 0) return { ok: false, count: 0, detail: 'no tracked buys' };
  const cutoff = Date.now() - (windowMs || WHALE_BUY_WINDOW_MS);
  const live = [...bucket.entries()].filter(([, r]) => r && r.ts >= cutoff);
  if (!live.length) return { ok: false, count: 0, detail: 'no tracked buys in window' };
  const price = await getSolPriceUsd();
  let count = 0, unknown = 0;
  const parts = [];
  for (const [, r] of live) {
    // FAIL-OPEN per wallet: unparseable size or a dead price feed counts as
    // qualifying, matching the cluster gate, so a blip cannot mute a signal.
    if (r.sol === null || r.sol === undefined || !(price > 0)) { unknown++; count++; parts.push('?'); continue; }
    const usd = r.sol * price;
    if (usd >= minUsd) { count++; parts.push(fmtUsd(usd)); }
  }
  if (count >= need) {
    return { ok: true, count, detail: (parts.join(', ') || `${count} qualifying`) + (unknown ? ` (${unknown} size unknown, fail-open)` : '') };
  }
  return { ok: false, count, detail: `only ${count}/${need} tracked wallets bought >= ${fmtUsd(minUsd)}` };
}
async function checkWhaleHolders(map) {
  if (!ENABLE_CLUSTER) return;   // gated with the cluster (same "add-back" package)
  const now = Math.floor(Date.now() / 1000);
  for (const [addr, v] of map.entries()) {
    if (whaleFired.has(addr)) continue;
    if ((v.holders || 0) <= WHALE_MIN_HOLDERS) continue;         // need > 5000 holders
    if ((v.bestRank || 999) !== 1) continue;                    // #1 in >=1 interval
    if (!(v.created > 0)) continue;                             // need age; fail-closed
    if ((now - v.created) >= WHALE_MAX_AGE) continue;           // must be < 60 min old
    // ── AIRDROP FILTERS ──
    if (WHALE_MAX_HOLDERS > 0 && (v.holders || 0) > WHALE_MAX_HOLDERS) {
      log(`[WHALE] SKIP ${v.symbol || addr.substring(0,8)} — ${v.holders.toLocaleString()} holders > ${WHALE_MAX_HOLDERS.toLocaleString()} (airdrop scale)`);
      continue;
    }
    const _mcPerHolder = (v.holders > 0 && v.mc > 0) ? (v.mc / v.holders) : 0;
    if (WHALE_MIN_MC_PER_HOLDER > 0 && _mcPerHolder < WHALE_MIN_MC_PER_HOLDER) {
      log(`[WHALE] SKIP ${v.symbol || addr.substring(0,8)} — ${fmtUsd(_mcPerHolder)}/holder < ${fmtUsd(WHALE_MIN_MC_PER_HOLDER)} (${v.holders.toLocaleString()} holders on ${fmtUsd(v.mc)} MC — airdropped, not bought)`);
      continue;
    }
    const _holdersPerBuy = (v.buys > 0) ? (v.holders / v.buys) : Infinity;
    if (WHALE_MAX_HOLDERS_PER_BUY > 0 && _holdersPerBuy > WHALE_MAX_HOLDERS_PER_BUY) {
      log(`[WHALE] SKIP ${v.symbol || addr.substring(0,8)} — ${_holdersPerBuy === Infinity ? 'no buys recorded' : _holdersPerBuy.toFixed(1) + ' holders per buy'} > ${WHALE_MAX_HOLDERS_PER_BUY} (holders did not buy in)`);
      continue;
    }
    // Smart-money / KOL floors. The 12g pool filter already asks GMGN for
    // min_smart_degen_count / min_renowned_count, so these are usually already
    // met — kept explicit so this signal holds its own floor even if
    // TREND_MIN_SMART / TREND_MIN_KOL are lowered.
    if ((v.smart || 0) < WHALE_MIN_SMART) {
      log(`[WHALE] SKIP ${v.symbol || addr.substring(0,8)} — smart ${v.smart || 0} < ${WHALE_MIN_SMART}`);
      continue;
    }
    if ((v.kol || 0) < WHALE_MIN_KOL) {
      log(`[WHALE] SKIP ${v.symbol || addr.substring(0,8)} — KOL ${v.kol || 0} < ${WHALE_MIN_KOL}`);
      continue;
    }
    // At least one >= $500 buy from a TRACKED wallet in the window. Checked LAST
    // because it is the only gate that can cost a network call.
    const big = await whaleTrackedBigBuy(addr, WHALE_MIN_BIG_BUY_USD, WHALE_BUY_WINDOW_MS, 1);
    if (!big.ok) {
      log(`[WHALE] SKIP ${v.symbol || addr.substring(0,8)} — ${big.detail}`);
      continue;
    }
    // TELEGRAM CALL GATE (added 2026-08-21 — this signal was missed when the gate
    // went into the other three). No pending registry needed: whaleFired is NOT
    // set on a miss and checkWhaleHolders re-runs every trending refresh, so the
    // token is re-checked each cycle until it fires or ages past WHALE_MAX_AGE.
    const tgw = tgCallCheck(addr);
    if (!tgw.ok) {
      log(`[WHALE] HOLD ${v.symbol || addr.substring(0,8)} — ${v.holders} holders + buys, but ${tgw.detail}`);
      continue;
    }

    whaleFired.add(addr);
    saveSet('/tmp/sol_whale_fired.json', whaleFired);
    const ageMin = Math.floor((now - v.created) / 60);
    const msg =
      `🐳 <b>WHALE HOLDER — ${v.symbol || '?'}</b>\n\n` +
      `<b>Holders:</b> ${v.holders.toLocaleString()} (${fmtUsd(_mcPerHolder)}/holder, ${_holdersPerBuy.toFixed(1)} per buy)\n` +
      `<b>Age:</b> ${ageMin}m\n` +
      `<b>Trending Rank:</b> #${v.bestRank} (best across intervals)\n` +
      `<b>Smart / KOL:</b> ${v.smart || 0} / ${v.kol || 0}\n` +
      `<b>Tracked Buys (${big.count}):</b> ${big.detail}\n` +
      `<b>Telegram:</b> ${tgw.detail}\n` +
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
      const allOne = TOP1_INTERVALS.every(iv => r[iv] === 1);
      if (!allOne) {
        if (pendingTop1[addr]) {
          log(`[TOP1] pending DROPPED ${v.symbol || addr.substring(0,8)} — no longer #1 on all of ${TOP1_INTERVALS.join('/')}`);
          delete pendingTop1[addr];
        }
        continue;
      }

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
      if ((_now - _created) > TOP1_MAX_AGE_SECS) continue;  // too old
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

      // At least one >= $500 buy from a TRACKED wallet inside TOP1_BUY_WINDOW_MS.
      // Placed AFTER the silent-prime block on purpose: at boot trackedBuysWhale
      // is empty, so gating the prime pass on buy history would leave every
      // already-#1 token unprimed and let it fire later as if it were new.
      // Runs BEFORE top1Fired.add so a token blocked here stays eligible and can
      // still fire on a later cycle once a tracked wallet buys it.
      const big = await whaleTrackedBigBuy(addr, TOP1_MIN_BIG_BUY_USD, TOP1_BUY_WINDOW_MS, TOP1_MIN_BIG_BUYS);
      if (!big.ok) {
        log(`[TOP1] SKIP ${v.symbol || addr.substring(0,8)} — #1 on ${TOP1_INTERVALS.join('/')} but ${big.detail}`);
        continue;
      }
      const tg = tgCallCheck(addr);
      if (!tg.ok) {
        // On-chain side satisfied; only the call is missing. HOLD, don't drop —
        // and do NOT add to top1Fired, or it could never fire once a call lands.
        if (!pendingTop1[addr]) {
          pendingTop1[addr] = { since: Date.now() };
          log(`[TOP1] PENDING ${v.symbol || addr.substring(0,8)} — #1 + ${big.count} buys, waiting on a telegram call (re-checking every ${TG_RECHECK_SECS}s)`);
        }
        continue;
      }
      delete pendingTop1[addr];

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
        `Rank #1 on: ${TOP1_INTERVALS.join(', ')}\n` +
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
        `Smart / KOL: ${v.smart} / ${v.kol}\n` +
        `Tracked Buys (${big.count}): ${big.detail}\n` +
        `Telegram: ${tg.detail}\n` +
        (tg.stale ? '\u26a0\ufe0f call gate NOT enforced — feed unavailable\n' : '') +
        `\n` +
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

    // TELEGRAM CALL GATE. On-chain side is fully satisfied here; if there is no
    // call yet we HOLD rather than discard, and crucially do NOT add to
    // trendFired — adding it would permanently block the token from ever firing
    // once a call arrives.
    const tgT = tgCallCheck(tokenMint);
    if (!tgT.ok) {
      if (!pendingTrend[tokenMint]) {
        // Keep the originating buy so the re-check can re-enter sendTrendSignal
        // itself rather than rebuilding the alert (one fire path, no drift).
        pendingTrend[tokenMint] = { trackedWallet, tx, since: Date.now() };
        log(`[TREND] PENDING ${t.symbol} — bluechip ${(t.bluechip*100).toFixed(1)}%, waiting on a telegram call (re-checking every ${TG_RECHECK_SECS}s)`);
      }
      return;
    }
    delete pendingTrend[tokenMint];

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
      `Trending in: ${intervalsStr}\n` +
      `Telegram: ${tgT.detail}\n` +
      (tgT.stale ? '\u26a0\ufe0f call gate NOT enforced — feed unavailable\n' : '') +
      `\n` +
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
//  PENDING RE-CHECK  (telegram call arriving AFTER the on-chain signal)
// ══════════════════════════════════════════════════════════════
// Runs every TG_RECHECK_SECS. Cheap: in-memory trendingMap + one mtime-cached
// file read. No GMGN or RPC calls unless a signal is actually about to fire.
//
// #1: keep waiting while the mint is STILL rank 1 on all TOP1_INTERVALS.
// BLUECHIP: keep waiting while it STILL qualifies (rank + bluechip%) and is
// under TREND_MAX_TOKEN_AGE (24h). Both re-read the live map, so "still
// qualifies" is verified fresh rather than trusted from when it went pending.
async function recheckPendingSignals() {
  // ── #1 TRENDING ──
  for (const mint of (ENABLE_TOP1 ? Object.keys(pendingTop1) : [])) {
    try {
      if (top1Fired.has(mint)) { delete pendingTop1[mint]; continue; }
      const v = trendingMap.get(mint);
      const r = (v && v.ranks) || {};
      if (!v || !TOP1_INTERVALS.every(iv => r[iv] === 1)) {
        log(`[TOP1] pending DROPPED ${(v && v.symbol) || mint.substring(0,8)} — no longer #1 on all of ${TOP1_INTERVALS.join('/')}`);
        delete pendingTop1[mint]; continue;
      }
      const now = Math.floor(Date.now() / 1000);
      if (v.created > 0 && (now - v.created) > TOP1_MAX_AGE_SECS) {
        log(`[TOP1] pending DROPPED ${v.symbol || mint.substring(0,8)} — aged out`);
        delete pendingTop1[mint]; continue;
      }
      const tg = tgCallCheck(mint);
      if (!tg.ok) continue;                    // still no call — keep waiting
      const big = await whaleTrackedBigBuy(mint, TOP1_MIN_BIG_BUY_USD, TOP1_BUY_WINDOW_MS, TOP1_MIN_BIG_BUYS);
      if (!big.ok) continue;                   // buys aged out of the window
      // Re-enter the normal path: clearing the pending flag and letting the next
      // trending refresh fire it keeps ONE fire path instead of a duplicate.
      delete pendingTop1[mint];
      log(`[TOP1] pending SATISFIED ${v.symbol || mint.substring(0,8)} — call landed (${tg.detail}); will fire on the next trending refresh`);
      await checkTop1Everywhere(new Map([[mint, v]]), false);
    } catch (e) {
      log(`[ERR] recheck top1 ${mint.substring(0,8)}: ${e.message}`);
    }
  }
  // ── BLUECHIP ──
  for (const mint of (ENABLE_TREND ? Object.keys(pendingTrend) : [])) {
    try {
      if (trendFired.has(mint)) { delete pendingTrend[mint]; continue; }
      const t = trendingMap.get(mint);
      if (!t) {
        log(`[TREND] pending DROPPED ${mint.substring(0,8)} — fell out of the trending pool`);
        delete pendingTrend[mint]; continue;
      }
      const rank = t.bestRank || 999;
      if (!(rank <= TREND_TOP_WIDE && t.bluechip > TREND_MIN_BLUECHIP)) {
        log(`[TREND] pending DROPPED ${t.symbol} — no longer qualifies (rank ${rank}, bc ${(t.bluechip*100).toFixed(1)}%)`);
        delete pendingTrend[mint]; continue;
      }
      const info = await getCachedTokenInfo(mint);
      const created = parseInt(info?.creation_timestamp ?? t.created ?? 0, 10) || 0;
      const age = created > 0 ? Math.floor(Date.now()/1000) - created : null;
      if (age == null || age > TREND_MAX_TOKEN_AGE) {
        log(`[TREND] pending DROPPED ${t.symbol} — aged out (${fmtAge(age)})`);
        delete pendingTrend[mint]; continue;
      }
      const tg = tgCallCheck(mint);
      if (!tg.ok) continue;                    // still no call — keep waiting
      log(`[TREND] pending SATISFIED ${t.symbol} — call landed (${tg.detail}), firing`);
      // Re-enter sendTrendSignal with the ORIGINAL buy. Every gate re-runs (now
      // including the call, which passes), and the alert is built by the one
      // existing code path — no duplicated message to drift out of sync.
      const p = pendingTrend[mint];
      delete pendingTrend[mint];
      await sendTrendSignal(p.trackedWallet, mint, p.tx);
    } catch (e) {
      log(`[ERR] recheck trend ${mint.substring(0,8)}: ${e.message}`);
    }
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
    // TELEGRAM CALL GATE. No call yet -> do NOT mark fired and do NOT clear the
    // buyer bucket: the whole cluster state stays intact so the NEXT tracked buy
    // on this mint re-runs this function and re-checks for a call. That is N's
    // "only check again when another wallet buys" rule; it needs no timer.
    const tgC = tgCallCheck(tokenMint);
    if (!tgC.ok) {
      log(`[CLUSTER] PENDING ${tokenMint.substring(0,8)} — cluster complete, waiting on a telegram call (re-checks on the next tracked buy)`);
      return;
    }
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
      `Largest Buy: <b>${largestStr}</b>\n` +
      `Telegram: ${tgC.detail}\n` +
      (tgC.stale ? '\u26a0\ufe0f call gate NOT enforced — feed unavailable\n' : '') +
      `\n` +
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

  // Which signals may this buy count toward? A legacy wallet feeds the original
  // signals; a FOMO wallet feeds only the FOMO entry signal. Keeping the pools
  // separate is the point — merging them would silently change what the existing
  // signals mean.
  const isLegacy = LEGACY_ADDR_SET.has(trackedWallet);
  const isFomo   = FOMO_ADDR_SET.has(trackedWallet);

  // ── SIGNAL 5: FOMO trader entry ──
  if (ENABLE_FOMO && isFomo) await checkFomoSignal(trackedWallet, mint, tx);

  // ── SIGNAL 1: Bluechip trending buy ──
  if (ENABLE_TREND && isLegacy) await sendTrendSignal(trackedWallet, mint, tx);

  // Record EVERY legacy tracked buy for the whale-holder signal — independent of
  // the ENABLE_CLUSTER gate below, so the whale signal never depends on the
  // cluster being enabled. extractSolSpent is free (no network). FOMO wallets are
  // excluded on purpose: this store also feeds the #1-trending gate, and letting
  // FOMO traders count there would quietly change what that signal means.
  if (isLegacy) {
    try {
      if (!trackedBuysWhale[mint]) trackedBuysWhale[mint] = new Map();
      trackedBuysWhale[mint].set(trackedWallet, { ts: Date.now(), sol: extractSolSpent(tx, trackedWallet) });
    } catch (e) { /* never let bookkeeping break the buy path */ }
  }

  // ── SIGNAL 2: 8-wallet cluster ──
  if (ENABLE_CLUSTER && isLegacy) await checkClusterSignal(trackedWallet, mint, tx);
}

// ── SIGNAL 5: FOMO TRADER ENTRY ───────────────────────────────
// FOMO_MIN_WALLETS of the tracked wallets take an entry in the same mint, every
// one of them inside FOMO_MAX_MINT_AGE of the token's mint time. Fires once per
// mint, gated on a Telegram call like every other signal.
async function checkFomoSignal(trackedWallet, mint, tx) {
  try {
    if (fomoFired.has(mint)) return;

    if (!fomoBuyers[mint]) fomoBuyers[mint] = new Map();
    const bucket = fomoBuyers[mint];
    // Keep the FIRST entry per wallet. A trader adding again later must not drag
    // their own timestamp past the 24h line and disqualify a buy that was in time.
    if (!bucket.has(trackedWallet)) {
      // tx is null when the pending re-check re-enters; that path only reaches
      // here for wallets already in the bucket, but guard it so a future caller
      // can't blow up on a missing tx.
      bucket.set(trackedWallet, { ts: Date.now(), sol: tx ? extractSolSpent(tx, trackedWallet) : null });
    }
    log(`[FOMO] ${walletName(trackedWallet)} — ${mint.substring(0,8)} — ${bucket.size}/${FOMO_MIN_WALLETS} traders`);
    if (bucket.size < FOMO_MIN_WALLETS) return;

    // Mint time, fetched only once the threshold is reached — a token that never
    // gets there costs no API call.
    const info = await getCachedTokenInfo(mint);
    const created = parseInt(info?.creation_timestamp ?? 0, 10) || 0;
    if (!(created > 0)) {
      log(`[FOMO] SKIP ${mint.substring(0,8)} — no creation timestamp`);
      return;
    }

    // Every qualifying entry must have landed within FOMO_MAX_MINT_AGE of mint.
    // Buys are stamped when detected, so this is the age the token actually was.
    const inTime = [];
    const late = [];
    for (const [w, rec] of bucket) {
      const ageAtBuy = Math.floor(rec.ts / 1000) - created;
      rec.ageAtBuy = ageAtBuy;
      rec.wallet = w;
      if (ageAtBuy <= FOMO_MAX_MINT_AGE) inTime.push(rec);
      else late.push(`${walletName(w)} ${fmtAge(ageAtBuy)}`);
    }
    if (inTime.length < FOMO_MIN_WALLETS) {
      log(`[FOMO] SKIP ${mint.substring(0,8)} — ${bucket.size} traders but only ${inTime.length} within ${fmtAge(FOMO_MAX_MINT_AGE)} of mint (late: ${late.join(', ') || 'none'})`);
      return;
    }

    // Optional size floor. 0 (the default) means "any entry counts" and skips the
    // price lookup entirely. FAIL-OPEN per wallet: an unparseable buy or a dead
    // price feed qualifies rather than silently suppressing a real entry.
    const solPrice = await getSolPriceUsd();
    for (const rec of inTime) {
      rec.usd = (rec.sol === null || rec.sol === undefined || !(solPrice > 0)) ? null : rec.sol * solPrice;
    }
    const sized = FOMO_MIN_BUY_USD > 0
      ? inTime.filter(rec => rec.usd === null || rec.usd >= FOMO_MIN_BUY_USD)
      : inTime;
    if (sized.length < FOMO_MIN_WALLETS) {
      log(`[FOMO] SKIP ${mint.substring(0,8)} — ${inTime.length} in time, only ${sized.length} >= ${fmtUsd(FOMO_MIN_BUY_USD)}`);
      return;
    }

    // TELEGRAM CALL GATE. No call yet -> do NOT mark fired and do NOT clear the
    // bucket, so the next entry on this mint re-runs this and re-checks.
    const tg = tgCallCheck(mint, FOMO_MIN_CALLS, info?.symbol);
    if (!tg.ok) {
      if (!pendingFomo[mint]) {
        pendingFomo[mint] = { since: Date.now(), symbol: info?.symbol || null };
        log(`[FOMO] PENDING ${mint.substring(0,8)} — ${sized.length} traders in, ${tg.detail} (need ${FOMO_MIN_CALLS} channels)`);
      }
      return;
    }
    delete pendingFomo[mint];

    fomoFired.add(mint);
    saveSet('/tmp/sol_fomo_fired.json', fomoFired);
    delete fomoBuyers[mint];

    const symbol = info?.symbol ?? 'UNKNOWN';
    const mc     = tokenMarketCap(info);
    const mcStr  = mc > 0 ? fmtUsd(mc) : 'N/A';
    const age    = Math.floor(Date.now()/1000) - created;
    // Earliest entry first — who got there first is the interesting part.
    const ordered = [...sized].sort((a, b) => (a.ageAtBuy ?? 0) - (b.ageAtBuy ?? 0));
    const buyerList = ordered.map(r =>
      `  \u2022 ${walletName(r.wallet)} — ${fmtAge(r.ageAtBuy)} after mint` +
      (r.usd === null ? '' : ` (${fmtUsd(r.usd)})`)
    ).join('\n');
    const signalTime = new Date().toLocaleTimeString('en-US', {
      timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true
    });

    sendTelegram(FOMO_SIGNAL_CHAT,
      `\u{1F3AF} <b>${ordered.length} FOMO Traders Entered — ${symbol}</b>\n\n` +
      `Contract: <code>${mint}</code>\n\n` +
      `Traders: <b>${ordered.length}</b> (all within ${fmtAge(FOMO_MAX_MINT_AGE)} of mint)\n` +
      `Chain: Solana\n` +
      `Token Age: ${fmtAge(age)}\n` +
      `Market Cap: ${mcStr}\n` +
      `Telegram: ${tg.detail}\n` +
      (tg.stale ? '\u26a0\ufe0f call gate NOT enforced — feed unavailable\n' : '') +
      `\n` +
      `<b>Entered by:</b>\n${buyerList}\n\n` +
      `Signal Time: ${signalTime}\n\n` +
      `\u{1F517} <a href="https://gmgn.ai/sol/token/${mint}">View on GMGN</a>`
    );
    log(`[FOMO] \u{1F525} FIRED ${symbol} ${mint.substring(0,8)} — ${ordered.length} traders | age ${fmtAge(age)}`);
  } catch (e) {
    log(`[FOMO] error on ${mint?.substring(0,8)}: ${e.message}`);
  }
}

// Re-check FOMO signals waiting on calls. Cheap: one mtime-cached file read per
// pass, no RPC or GMGN calls unless a mint actually clears the gate.
async function recheckPendingFomo() {
  for (const mint of Object.keys(pendingFomo)) {
    if (fomoFired.has(mint)) { delete pendingFomo[mint]; continue; }
    const bucket = fomoBuyers[mint];
    if (!bucket || bucket.size < FOMO_MIN_WALLETS) {
      log(`[FOMO] pending DROPPED ${mint.substring(0,8)} — buys aged out`);
      delete pendingFomo[mint];
      continue;
    }
    if (!tgCallCheck(mint, FOMO_MIN_CALLS, pendingFomo[mint].symbol).ok) continue;
    try {
      log(`[FOMO] pending SATISFIED ${mint.substring(0,8)} — calls landed, re-evaluating`);
      // Re-enter the one existing fire path so the alert can't drift out of sync.
      // The tracked wallet/tx args are only used to RECORD a buy, and this mint
      // already has its buyers, so passing the first recorded one is safe.
      const [firstWallet] = [...bucket.keys()];
      await checkFomoSignal(firstWallet, mint, null);
    } catch (e) {
      log(`[ERR] FOMO recheck ${mint.substring(0,8)}: ${e.message}`);
    }
  }
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
  // Prune fomoBuyers. An entry only matters while the token can still be under
  // FOMO_MAX_MINT_AGE; drop at 2x that so a token minted just before a restart
  // can still complete its set.
  for (const mint of Object.keys(fomoBuyers)) {
    const fb = fomoBuyers[mint];
    for (const [w, rec] of fb) { if (Date.now() - rec.ts > FOMO_MAX_MINT_AGE * 2000) fb.delete(w); }
    if (fb.size === 0) delete fomoBuyers[mint];
  }
  if (fomoFired.size > 20000) { fomoFired.clear(); saveSet('/tmp/sol_fomo_fired.json', fomoFired); log(`[CLEANUP] fomoFired cleared`); }
  // Prune trackedBuysWhale by its own time window (not by size) so entries expire
  // predictably and the whale gate never reads a stale buy.
  {
    const wCut = Date.now() - TRACKED_BUY_RETENTION_MS;
    for (const m of Object.keys(trackedBuysWhale)) {
      const b = trackedBuysWhale[m];
      for (const [w, rec] of b) { if (!rec || rec.ts < wCut) b.delete(w); }
      if (b.size === 0) delete trackedBuysWhale[m];
    }
    // Size cap: retention is now 48h (the #1 signal's max token age), so this
    // store holds far more than the old 60-min version. If it still exceeds the
    // cap after time pruning, drop the tokens whose MOST RECENT buy is oldest.
    // Deliberately NOT the wholesale `= {}` clear used for the other stores —
    // wiping this one would silently mute the #1 and whale signals.
    const tbKeys = Object.keys(trackedBuysWhale);
    if (tbKeys.length > TRACKED_BUY_MAX_TOKENS) {
      const newest = (k) => { let mx = 0; for (const [, r] of trackedBuysWhale[k]) if (r && r.ts > mx) mx = r.ts; return mx; };
      const ordered = tbKeys.map(k => [k, newest(k)]).sort((x, y) => x[1] - y[1]);
      const drop = ordered.length - TRACKED_BUY_MAX_TOKENS;
      for (let i = 0; i < drop; i++) delete trackedBuysWhale[ordered[i][0]];
      log(`[CLEANUP] trackedBuysWhale trimmed ${drop} oldest tokens (cap ${TRACKED_BUY_MAX_TOKENS})`);
    }
  }
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
log(`═══ SOL BLUECHIP TRENDING BOT — VERSION 2026-08-28a ═══`);
log(`[START] wallets: ${WALLETS.length} watched = ${_needLegacy ? LEGACY_ADDR_SET.size : 0} legacy (bluechip/cluster/#1/whale)${_needLegacy ? '' : ' — SKIPPED, those signals are off'} + ${ENABLE_FOMO ? FOMO_ADDR_SET.size : 0} FOMO traders`);
log(`[START] FOMO signal: ${ENABLE_FOMO ? 'ON' : 'OFF'} | >=${FOMO_MIN_WALLETS} tracked wallets entering within ${Math.round(FOMO_MAX_MINT_AGE/3600)}h of mint | min buy ${FOMO_MIN_BUY_USD > 0 ? '$' + FOMO_MIN_BUY_USD : 'any'} | >=${FOMO_MIN_CALLS} telegram channel(s)${TG_MATCH_TICKERS ? ' (contract or $ticker)' : ' (contract only)'} | chat ${FOMO_SIGNAL_CHAT}`);
log(`[START] ${WALLETS.length} wallets | SOLE SIGNAL: tracked buy + top-${TREND_TOP_N} trending (any interval) + age < ${TREND_MAX_TOKEN_AGE/3600}h + bluechip > ${(TREND_MIN_BLUECHIP*100).toFixed(0)}%`);
log(`[START] Signal chat: ${TREND_SIGNAL_CHAT} | Trending refresh: every ${TREND_POLL_SECS}s across [${TREND_INTERVALS.join(', ')}]`);
log(`[START] WSS chain: ${WSS_ENDPOINTS.map(e => e.name).join(' -> ')}`);
log(`[START] HTTP RPC chain: ${HTTP_RPC_NAMES.join(' -> ')} | buy-detection=${BUY_MODE}` + (BUY_MODE === 'POLL' ? `  <-- POLL makes ~${WALLETS.length} RPC calls every ${BUY_POLL_SECS}s against the FIRST endpoint` : ''));
log(`[START] Signals: bluechip=${ENABLE_TREND ? 'ON' : 'OFF'}, #1-everywhere=${ENABLE_TOP1 ? 'ON' : 'OFF'}, cluster(5w/$500/60m)=${ENABLE_CLUSTER ? 'ON' : 'OFF'}, whale-holder(>5k/60m)=${ENABLE_CLUSTER ? 'ON' : 'OFF'}, FOMO=${ENABLE_FOMO ? 'ON' : 'OFF'} | buy-detection=${BUY_MODE}`);
// Print the exact trending-pool query config at boot. Everything below is sent to
// /v1/market/rank, so this line IS what the bot is asking GMGN for — no need to
// read the source or guess which build is live.
setInterval(() => { recheckPendingSignals().catch(e => log(`[ERR] recheck loop: ${e.message}`)); }, TG_RECHECK_SECS * 1000);
setInterval(() => { if (ENABLE_FOMO) recheckPendingFomo().catch(e => log(`[ERR] FOMO recheck loop: ${e.message}`)); }, TG_RECHECK_SECS * 1000);
log(`[START] TG call gate: ${TG_GATE_ENABLED ? 'ON' : 'OFF'} | ${TG_FAIL_OPEN ? 'FAIL-OPEN (fires unchecked if feed down)' : 'FAIL-CLOSED (no call = no signal)'} | feed=${TG_CALLS_FILE} | re-check every ${TG_RECHECK_SECS}s | feed considered down after ${Math.round(TG_CALLS_MAX_STALE_SEC/60)}m without a heartbeat`);
log(`[START] Trending pool: order_by=volume desc | top${TREND_TOP_N} per interval | filters=[${TREND_FILTERS.join(', ') || 'none'}] | max_created=${TREND_MAX_CREATED || 'none'} | min_smart=${TREND_MIN_SMART} | min_kol=${TREND_MIN_KOL}`);
log(`[START] #1 signal: rank 1 on [${TOP1_INTERVALS.join(', ')}] + >=${TOP1_MIN_BIG_BUYS} wallets each >= $${TOP1_MIN_BIG_BUY_USD} within ${Math.round(TOP1_BUY_WINDOW_MS/60000)}m | whale: >${WHALE_MIN_HOLDERS} holders + smart>=${WHALE_MIN_SMART} + kol>=${WHALE_MIN_KOL} + tracked buy >= $${WHALE_MIN_BIG_BUY_USD} within ${Math.round(WHALE_BUY_WINDOW_MS/60000)}m`);

https.get('https://api.ipify.org?format=json', (res) => {
  let d = ''; res.on('data', c => d += c);
  res.on('end', () => { try { log(`[IP] ${JSON.parse(d).ip}`); } catch {} });
}).on('error', () => {});

// Prime the trending cache immediately, then refresh on a timer — but only if a
// signal still consumes it. The FOMO signal has no trending gate, so with the
// legacy signals off this poller would be pure GMGN quota burn.
if (ENABLE_TREND || ENABLE_TOP1 || ENABLE_CLUSTER) {
  refreshTrending().then(() => {
    log(`[TREND] initial load: ${trendingMap.size} tokens`);
  });
  setInterval(refreshTrending, TREND_POLL_SECS * 1000);
} else {
  log(`[TREND] poller OFF — no signal consumes trending (FOMO-only mode)`);
}

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

// ── WS -> POLL AUTOMATIC FALLBACK ────────────────────────────
// WebSocket logsSubscribe is the cheap path (one connection, ~free in credits)
// but it DIES when Helius credits run out — which is exactly how buy detection
// went silent for days. This watchdog starts the HTTP POLL backstop if the
// socket is unhealthy for WS_FALLBACK_SECS, and stops it again once the socket
// recovers, so the bot degrades to "costs credits" instead of "detects nothing".
const WS_FALLBACK_SECS = parseInt(process.env.WS_FALLBACK_SECS || '180', 10);
let fallbackPollTimer = null, fallbackPollActive = false, wsUnhealthySince = 0;
function startFallbackPoll(reason) {
  if (fallbackPollActive) return;
  fallbackPollActive = true;
  log(`[FALLBACK] WebSocket unhealthy (${reason}) — starting HTTP POLL backstop every ${BUY_POLL_SECS}s so buy detection does not go silent. NOTE: polling consumes RPC credits.`);
  pollWalletsForBuys().then(() => log(`[FALLBACK] poll cursors seeded`)).catch(() => {});
  fallbackPollTimer = setInterval(pollWalletsForBuys, BUY_POLL_SECS * 1000);
}
function stopFallbackPoll() {
  if (!fallbackPollActive) return;
  clearInterval(fallbackPollTimer);
  fallbackPollTimer = null; fallbackPollActive = false;
  log(`[FALLBACK] WebSocket healthy again — stopping HTTP POLL backstop (back to near-zero-credit buy detection)`);
}

// Start buy detection in the configured mode.
if (BUY_MODE === 'POLL') {
  log(`[START] Buy detection: HTTP POLL every ${BUY_POLL_SECS}s across ${WALLETS.length} wallets (free-tier safe, no WebSocket)`);
  // First cycle seeds cursors silently (no alerts for pre-existing history), then polls.
  pollWalletsForBuys().then(() => log(`[POLL] cursors seeded — now watching for new buys`));
  setInterval(pollWalletsForBuys, BUY_POLL_SECS * 1000);
} else {
  log(`[START] Buy detection: WebSocket (logsSubscribe) + auto POLL fallback after ${WS_FALLBACK_SECS}s unhealthy`);
// Feed-outage watchdog. Fail-closed means a dead tracker blocks every signal, so
// announce it rather than going quiet — silence is the failure mode to avoid.
setInterval(() => {
  try {
    const probe = tgCallCheck('So11111111111111111111111111111111111111112__probe');
    const now = Math.floor(Date.now() / 1000);
    if (probe.down) {
      if (!_tgDownSince) _tgDownSince = now;
      if (now - _tgDownNotifiedAt >= TG_DOWN_NOTIFY_SECS) {
        _tgDownNotifiedAt = now;
        const mins = Math.round((now - _tgDownSince) / 60);
        sendTelegram(TOP1_SIGNAL_CHAT,
          `\u26a0\ufe0f <b>SOL BOT — TELEGRAM CALL FEED DOWN</b>\n\n` +
          `${probe.detail}\n` +
          `Down for ~${mins}m.\n\n` +
          `<b>All signals are being SUPPRESSED</b> (fail-closed): nothing fires without a call.\n` +
          `Pending signals are still held and will fire if the feed returns while they qualify.\n\n` +
          `Fix: check profit-tracker is running and writing ${TG_CALLS_FILE}.`);
        log(`[TG] FEED DOWN ${mins}m — all signals suppressed (fail-closed)`);
      }
    } else if (_tgDownSince) {
      const mins = Math.round((now - _tgDownSince) / 60);
      sendTelegram(TOP1_SIGNAL_CHAT, `\u2705 <b>SOL BOT — call feed RECOVERED</b> after ~${mins}m. Signals re-enabled.`);
      log(`[TG] feed recovered after ${mins}m`);
      _tgDownSince = 0; _tgDownNotifiedAt = 0;
    }
  } catch (e) { log(`[TG] watchdog: ${e.message}`); }
}, 60_000);
  connect();
  // Health = socket open AND at least half the wallets actually subscribed.
  // Half, not all, because a partial subscribe still detects most buys and we do
  // not want to flap onto the paid path over one or two failed subscriptions.
  setInterval(() => {
    const active = Object.keys(subIdToWallet).length;
    const healthy = ws && ws.readyState === WebSocket.OPEN && active >= Math.ceil(WALLETS.length / 2);
    if (healthy) { wsUnhealthySince = 0; stopFallbackPoll(); return; }
    if (!wsUnhealthySince) wsUnhealthySince = Date.now();
    if (Date.now() - wsUnhealthySince > WS_FALLBACK_SECS * 1000) {
      startFallbackPoll(`${active}/${WALLETS.length} subscriptions active`);
    }
  }, 30000);
}

// Self-ping (Render only — harmless on a VPS where RENDER_EXTERNAL_URL is unset)
if (RENDER_URL) {
  setInterval(() => {
    const mod = RENDER_URL.startsWith('https') ? https : http;
    mod.get(RENDER_URL + '/', res => log(`[PING] ${res.statusCode}`))
      .on('error', e => log(`[PING] ${e.message}`));
  }, 10 * 60_000);
}
