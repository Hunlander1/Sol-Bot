
Sol Bot Reference Document
Last Updated: June 2, 2026
PASTE THIS AT THE START OF EVERY NEW CONVERSATION
⚠️ CRITICAL RULE
NEVER modify working code. Only change what is explicitly asked for.
Working features: slow tracker rolling window, fetchSameNameCount, handleWalletBuy, WebSocket, signal builders.
Bot

File: sol-combined-bot.js / deployed on Render as index.js in Hunlander1/sol-bot (main branch)
Node.js single file, ws dependency only

Signal Types
Slow signal → CHAT_ID_SLOW (-1003888330833): 3 wallets, 5-min window, token under 15 min, same-name ≥10 AND dev ATH ≥$1M or dev is tracked wallet. Currently running 24/7 (time constraint removed for testing — restore 11am-6pm ET when done).
Fast migration → CHAT_ID_FAST (-5081620734): 2 wallets within 30s of mint, token hits $38k MC. Currently running 24/7 (restore 11am-6pm ET when done).
Removed Features

❌ Sell signal
❌ Whale bot
❌ Accumulation signal
❌ 7-wallet fast signal
❌ Notable holders

Wallets Removed (flooding)

FaBGrHWj... — Dale Dev
6ujZxnph... — BadBunny Dev
7moqFjvm... — Smart 15

Environment Variables (Render)

TELEGRAM_TOKEN, CHAT_ID_SLOW, CHAT_ID_FAST
GMGN_API_KEY, HELIUS_API_KEY, ALCHEMY_API_KEY
RENDER_EXTERNAL_URL
SHYFT_API_KEY — throttled until June 10 reset, keep it in for when it resets

RPC Fallback Order
Helius → Shyft → Alchemy → Public Solana RPC
Key Architecture

WebSocket logsSubscribe per wallet via Helius
processLogNotification → getTransaction (tries all RPCs) → extractMint → handleWalletBuy
devWalletCache — 10 min cache prevents dev buys counting
firedAlerts — persisted to /tmp/fired_alerts.json
pendingSigs — debounces duplicate WebSocket notifications
/logs endpoint — hit https://[render-url]/logs for last 500 lines
Self-ping every 10 min

EVM Bot

File: evm-combined-bot.js, deployed on Railway
Fast migration → CHAT_ID_COORD, 4-wallet coord → CHAT_ID_COORD, whale → CHAT_ID_WHALE
Chains: ETH, BSC, Base, 61 wallets

Wallet Counts

Sol bot: ~86 wallets
EVM bot: 61 wallets
