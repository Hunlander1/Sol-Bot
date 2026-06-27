// ============================================================
//  SOLANA COMBINED BOT
//  ----------------------------------------------------------
//  >>> VERSION: 2026-06-24g  (Buy Tracker: kline 30s price MC primary; SLOW_MIN_WALLETS=4) <<<
//  If the right panel shows this header with this date,
//  it is the correct/latest file to deploy.
//  ----------------------------------------------------------
//  Two active signals, both running in one process:
//
//  1. SLOW SIGNAL  (Telegram group CHAT_ID_SLOW)
//     Fires when 2 tracked wallets buy the same token within
//     15 min, token under 15 min old. Passes the OR filter:
//     same-name >=10  OR  dev ATH >=$1M  OR  dev is a tracked wallet.
//
//  2. MIGRATION SIGNAL  (Telegram group CHAT_ID_FAST)
//     Fires when 2 tracked wallets buy + the token hits its MC
//     threshold within 30s of mint ($38k pump.fun / $375k Bags).
//
//  NOTE: This build includes a TEMPORARY [VOL DEBUG] block in
//  buildSlowSignal to identify GMGN's volume field. Remove once
//  the real volume source is wired in.
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

// ── WALLET BUY TRACKER ─────────────────────────────────────────
// Sends every FIRST buy (per token) from Theo and Cented to their own
// dedicated Telegram groups. Independent of slow/migration filters.
const CHAT_ID_THEO   = process.env.CHAT_ID_THEO   || '-5353363552';
const CHAT_ID_CENTED = process.env.CHAT_ID_CENTED || '-5305037806';
const TRACK_BUY_WALLETS = {
  "Bi4rd5FH5bYEN8scZ7wevxNZyNmKHdaBcvewdPFxYdLt": { name: "Theo",     chatId: CHAT_ID_THEO },
  "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o": { name: "Cented 7", chatId: CHAT_ID_CENTED },
};

const SOL_MINT = 'So11111111111111111111111111111111111111112';

// ── FAST BOT CONFIG ───────────────────────────────────────────
const FAST_WINDOW_SECS    = 30; // matches FAST_MAX_TOKEN_AGE
const FAST_MAX_TOKEN_AGE  = 30;
const FAST_MIN_WALLETS    = 5;

// ── FAST MIGRATION CONFIG ────────────────────────────────────
const FAST_MIG_MAX_AGE    = 30;  // token must hit MC threshold within 30s of mint
const FAST_MIG_MIN_WALLETS = 2;  // 2 tracked wallets (excluding dev)
const FAST_MIG_MIN_MC      = 38_000; // pump.fun migration ~$38k market cap threshold
const FAST_MIG_MIN_MC_BAGS = 375_000; // Bags tokens (mint ends 'bags') migrate at ~$375k

// ── SLOW BOT CONFIG ───────────────────────────────────────────
const SLOW_WINDOW_SECS    = 900;
const SLOW_MAX_TOKEN_AGE  = 900;
const SLOW_MIN_WALLETS    = 5;
const SLOW_SAME_NAME_THRESHOLD = 10;
const SLOW_DEV_ATH_THRESHOLD   = 1_000_000;



// ── RPC ───────────────────────────────────────────────────────
const HTTP_RPCS = [
  HELIUS_API_KEY  ? `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : null,
  SHYFT_API_KEY   ? `https://rpc.shyft.to?api_key=${SHYFT_API_KEY}` : null,
  ALCHEMY_API_KEY ? `https://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_API_KEY}` : null,
  'https://api.mainnet-beta.solana.com',
].filter(Boolean);
const WSS_PRIMARY  = HELIUS_API_KEY ? `wss://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}` : SHYFT_API_KEY ? `wss://rpc.shyft.to?api_key=${SHYFT_API_KEY}` : 'wss://api.mainnet-beta.solana.com';
const WSS_FALLBACK = 'wss://api.mainnet-beta.solana.com';
const HTTP_RPC     = HTTP_RPCS[0];

// ── FIRED ALERTS ──────────────────────────────────────────────
const FIRED_FILE       = '/tmp/sol_combined_fired.json';

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
  "GpTXmkdvrTajqkzX1fBmC4BUjSboF9dHgfnqPqj8WAc4","2ezv4U5HmPpkt2xLsKnw1FyyGmjFBeW7c166p99Hw2xB",
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
  "whamNNP9tHoxLg92yHvJPdYhghEoCg1qYTsh5a2oLbx","HdKJM6Lvfp9aV9tvEMC8AD4GnsbFgMUkHLoK923Sn1ET",
  "5FqUo9aBjsp7QeeyN6Vi2ZmF2fjS4H5EU7wnAQwPy17z","7hHmfYYR7L8LsCKk5akjtvVu1BbJRgHGJ2n6s7gbeKG4",
  "CjtqWn4toBbJ1feRZBDhz3TwBjbZm5RpES8rvKWTuNtk","FAX4qRQdiSj2iWDYvkJ21VieVCXGREtwMhEyAHSJ1aqp",
  "9VXuNqqqzniYYW3fRDeaCtUUtqWsEeWWn5umh3aF9h17","DAEdBmTPEKM6xkwfzC3d411QUe6coKpkND6UURa4CvHC",
  "iPUp3qkm39ycMGbywWFMUyvaDhiiPGXeWXaDtmHNe6C","CfkaAru9ArJ2tAStYHvbAyRBJL3EhDzsWYV2KYg9shxB",
  "EeLjBXRELqrcWAXbnj8T4jQPS9Qh7UGWiKxovsJ36pZY","H5Wh4EDvWQT4mShH746V5VDqxHQkaQZyPWfuhy1PRVBg",
  "GH9yk8vgFvHnAD8JZqXxr3hBN1Lr1mJ9NPzrP5mVqiJe","7hkd2kdx4bMyuUDgktZvykDh69r8YkkrX4kf1sW2C8T6",
  "8ghYW6ftL5kUemfsoA9X37rz3ZnvyMSZRAx1kt1CxpoS","GKaJNFDp2W5uCYfNKnTPN63tFXKgXgaDSfnTVfksBeq1",
  "DaKpjVJFxq3y4iZcEu12wzpXGCNBkQE587VNACUj15rT","C4ARzqpvZ4gR3ta89H5Yz7UyPTpRm22BL5U91e5dHTSf",
  "BSFxyBwsHQsDXULygBpsTu6iUmfHUbCr6j4geZSN6YJG","9Zu8AigeXgFAajBTni2VWw6Wmz7XxDqHmY5nQwdCWAyY",
  "9dkeTBYaHJzxVgVZqympcHmPeQvHtQv1sArZiZuwmhgp","AQdBYZNy3BZ1vouGUjA1w9Ay7aq7kH5UQSuh4LQWKotY",
  "HTM87R4mgjDdiF6Yfn8duK9vbDmZxiPCTRbGvm7eCAJY","8i5U2uNBEuTc4zskYP14zbebDg2RSwrrG8REhEnJb97K",
  "7E9jfxCczubz4FXkkVKzUMHXGwzJxyppC4m7y3ew8ATg","8v6ztxZwhPBNmA6aGrBzzrt6UBf2fZZfsWqZ9Lt47Kpv",
  "6nU2L7MQVUWjtdKHVpuZA9aind73nd3rXC4YFo8KQCy4","5zCkbcD74hFPeBHwYdwJLJAoLVgHX45AFeR7RzC8vFiD",
  "8HeDT75s5g4CtCimH5B5nySqCiQhtWii8UnZhxBtFo38","A8Z1ejQGk45EJibBPJviWnM3UvwKSuYun53nSCkWKM52",
  "D9gQ6RhKEpnobPBUdWY5bPQt2p3zGk3iVz6ChpUi2ArA","BZC7VEj5Y9Ege3cTRGBZW2zW7pjw3hpiSkcAoYKysvue",
  "FgifQEkRkSSXZjf2cJ4c55BhVts2yrNKzmzBLLyicg8b","EFaQQTGywnD4CjQQvTugUiyVT4LV9G6MsWqiub8X6unN",
  "HUgpmqL6r4Z4iEZiVuNZ6J6QnAsSZpsL8giVyVtz3QhT","FaBGrHWjcJ8vKnbgUtsdpZjvF7YAAajtQTWmmEHiKtQr",
  "HYWo71Wk9PNDe5sBaRKazPnVyGnQDiwgXCFKvgAQ1ENp","bwamJzztZsepfkteWRChggmXuiiCQvpLqPietdNfSXa",
  "7moqFjvm2MwAiMtCZoqYoTAPzRBxxMRT2ddyHThQuWjr",
  "DjM7Tu7whh6P3pGVBfDzwXAx2zaw51GJWrJE3PwtuN7s",
  "AvcWA3ngM55sSpjh1FZthmqA7V6BHo4f555a8w3Wv3ij",
  "J7nJ35d8EGU3fHCVCUun56C1MKakdoEQ38CFLHAhWDwP",
  "6ujZxnphRxTqveaQtLAQHFoWz16xhLWZbTijcgZN4fRp",
  "nazikTJezTC3W2fxXE3wzs495PYzXMiq5o7co6YYACA",
  "BtMBMPkoNbnLF9Xn552guQq528KKXcsNBNNBre3oaQtr",
  "EYfdt8cNFyyTEJKp18dcoVbgUHDnM1SK3bT2uKj9XXHc",
  "EgQX9R3Qph1dPHE1Ysou1auSYqRGomCNmLDC28Yg77aq",
  "2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f", // Cupsey
  "CtPxvpWo1pk7HtL6KwpCLMMdsXHC6fdqAN1bPiracaQq", // STINKDEX Dev
  "Bi4rd5FH5bYEN8scZ7wevxNZyNmKHdaBcvewdPFxYdLt", // Theo
  "6TbDFs2dkHETrRWVbheiC11bwg7EWLDgszsCADF1ML1b", // Notable 1
  "3dhwViJnxKhRJcJJznrVt6oYkuD1bULvsUXscuxpNBDs", // Notable 2
  "5Pr7D2d5WUM7j8fMF36DuzVDDGEHLtYsF7a6ezyzFG19", // Notable 3
  "GdRSPexhxbQz5H2zFQrNN2BAZUqEjAULBigTPvQ6oDMP", // NNC Dev
];
const WALLET_SET = new Set(WALLETS);

// Wallet name lookup — all known names
const WALLET_NAMES = {
  // Previously named
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
  "J7nJ35d8EGU3fHCVCUun56C1MKakdoEQ38CFLHAhWDwP": "Together Dev",
  "6ujZxnphRxTqveaQtLAQHFoWz16xhLWZbTijcgZN4fRp": "BadBunny Dev",
  "nazikTJezTC3W2fxXE3wzs495PYzXMiq5o7co6YYACA": "YZY Dev",
  "BtMBMPkoNbnLF9Xn552guQq528KKXcsNBNNBre3oaQtr": "Letterbomb(horse)",
  "EYfdt8cNFyyTEJKp18dcoVbgUHDnM1SK3bT2uKj9XXHc": "Penguin Dev",
  "EgQX9R3Qph1dPHE1Ysou1auSYqRGomCNmLDC28Yg77aq": "Smart 8",
  "2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f": "Cupsey",
  "CtPxvpWo1pk7HtL6KwpCLMMdsXHC6fdqAN1bPiracaQq": "STINKDEX Dev",
  // Newly matched from document
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
  "FgifQEkRkSSXZjf2cJ4c55BhVts2yrNKzmzBLLyicg8b": "Elephant Dev",
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
  "9dkeTBYaHJzxVgVZqympcHmPeQvHtQv1sArZiZuwmhgp": "Chud Dev",
  "9Zu8AigeXgFAajBTni2VWw6Wmz7XxDqHmY5nQwdCWAyY": "Moss Dev",
  "BSFxyBwsHQsDXULygBpsTu6iUmfHUbCr6j4geZSN6YJG": "Ziggy Dev",
  "C4ARzqpvZ4gR3ta89H5Yz7UyPTpRm22BL5U91e5dHTSf": "Ikun Dev",
  "DaKpjVJFxq3y4iZcEu12wzpXGCNBkQE587VNACUj15rT": "Xmas Dev",
  "GKaJNFDp2W5uCYfNKnTPN63tFXKgXgaDSfnTVfksBeq1": "Cartel Dev",
  "8ghYW6ftL5kUemfsoA9X37rz3ZnvyMSZRAx1kt1CxpoS": "Milady Ai Dev",
  "7hkd2kdx4bMyuUDgktZvykDh69r8YkkrX4kf1sW2C8T6": "Lamb Dev",
  "GH9yk8vgFvHnAD8JZqXxr3hBN1Lr1mJ9NPzrP5mVqiJe": "Eagy",
  "4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk": "Jijo",
  "8deJ9xeUvXSJwicYptA9mHsU2rN2pDx37KWzkDkEXhU6": "Cooker",
  "H5Wh4EDvWQT4mShH746V5VDqxHQkaQZyPWfuhy1PRVBg": "Bonkyo Dev",
  "EeLjBXRELqrcWAXbnj8T4jQPS9Qh7UGWiKxovsJ36pZY": "LLM Dev",
  "CfkaAru9ArJ2tAStYHvbAyRBJL3EhDzsWYV2KYg9shxB": "67 Dev",
  "bwamJzztZsepfkteWRChggmXuiiCQvpLqPietdNfSXa": "Copper Inu Dev",
  "DYAn4XpAkN5mhiXkRB7dGq4Jadnx6XYgu8L5b3WGhbrt": "Doc",
  // Names added from reconciliation
  "6EDaVsS6enYgJ81tmhEkiKFcb4HuzPUVFZeom6PHUqN3": "Cowboy",
  "FxN3VZ4BosL5urG2yoeQ156JSdmavm9K5fdLxjkPmaMR": "Track 15",
  "G7NvZKjoVqBDWciSYtWWgUPB7DA1iJavdvH5jty2FAmM": "America Dev",
  "BCagckXeMChUKrHEd6fKFA1uiWDtcmCXMsqaheLiUPJd": "DV",
  "4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9": "Decu",
  "2T5NgDDidkvhJQg8AHDi74uCFwgp25pYFMRZXBaCUNBH": "idontpaytaxes",
  "515vh1DrPuwMATt9Zoq9kP4sJL9fyojA1dHJu4DQpNRp": "crypto",
  "GpTXmkdvrTajqkzX1fBmC4BUjSboF9dHgfnqPqj8WAc4": "Track 5",
  "2ezv4U5HmPpkt2xLsKnw1FyyGmjFBeW7c166p99Hw2xB": "Track 7",
  "EaVboaPxFCYanjoNWdkxTbPvt57nhXGu5i6m9m6ZS2kK": "Danny",
  "FAicXNV5FVqtfbpn4Zccs71XcfGeyxBSGbqLDyDJZjke": "Radiance",
  "BAr5csYtpWoNpwhUjixX7ZPHXkUciFZzjBp9uNxZXJPh": "Jack Duval",
  "B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC": "Track 35",
  "8HcYptCBAaPFWkmupiSAmysZ6Z8jB7N1c4YhVjhX7zbg": "Smart 1",
  "FFEjC9MHhpQViBPrD2iU6LmV2hEigyhLJaL7MZUZzyD4": "Smart 2",
  "FTaSBuVj6w2S7XUa8fw19xrLy57DDr6kZDL6sxDXtvTP": "Smart 5",
  "G6fUXjMKPJzCY1rveAE6Qm7wy5U3vZgKDJmN1VPAdiZC": "Clukz",
  // New wallet added
  "Bi4rd5FH5bYEN8scZ7wevxNZyNmKHdaBcvewdPFxYdLt": "Theo",
  // More names added from reconciliation
  "Ar2Y6o1QmrRAskjii1cRfijeKugHH13ycxW5cd7rro1x": "Track 12",
  "5aLY85pyxiuX3fd4RgM3Yc1e3MAL6b7UgaZz6MS3JUfG": "Track 9",
  "7BNaxx6KdUYrjACNQZ9He26NBFoFxujQMAfNLnArLGH5": "Track 13",
  "BCnqsPEtA1TkgednYEebRpkmwFRJDCjMQcKZMMtEdArc": "Kreo",
  "5ZuV8eqkvzYFVEKbLvGBdexL2tFv7E5BCd2HZpjqbdg": "Doji",
  "FM1YCKED2KaqB8Uat8aB1nsffR1vezr7s6FAEieXJgke": "Pom Dev",
  "AV7PjXHL5JXZ1YoYRoN9Dsstg1x2UciBupMCXcJP8gUz": "Butthole Dev",
  "Dzp1SrZ474xwGp6ZEP6cNKo39u9zeXe1YAuTkyZyv3t4": "Distorted Dev",
  "whamNNP9tHoxLg92yHvJPdYhghEoCg1qYTsh5a2oLbx": "Ziggy Dev",
  "HdKJM6Lvfp9aV9tvEMC8AD4GnsbFgMUkHLoK923Sn1ET": "Chaos",
  "5FqUo9aBjsp7QeeyN6Vi2ZmF2fjS4H5EU7wnAQwPy17z": "Aloka Dev",
  "7hHmfYYR7L8LsCKk5akjtvVu1BbJRgHGJ2n6s7gbeKG4": "Goldcoin",
  "CjtqWn4toBbJ1feRZBDhz3TwBjbZm5RpES8rvKWTuNtk": "Vibecodoor",
  "FAX4qRQdiSj2iWDYvkJ21VieVCXGREtwMhEyAHSJ1aqp": "Petah Dev",
  "9VXuNqqqzniYYW3fRDeaCtUUtqWsEeWWn5umh3aF9h17": "Cancer Dev",
  "iPUp3qkm39ycMGbywWFMUyvaDhiiPGXeWXaDtmHNe6C": "Runner Dev",
  // New Notable wallets
  "6TbDFs2dkHETrRWVbheiC11bwg7EWLDgszsCADF1ML1b": "Notable 1",
  "3dhwViJnxKhRJcJJznrVt6oYkuD1bULvsUXscuxpNBDs": "Notable 2",
  "5Pr7D2d5WUM7j8fMF36DuzVDDGEHLtYsF7a6ezyzFG19": "Notable 3",
  "GdRSPexhxbQz5H2zFQrNN2BAZUqEjAULBigTPvQ6oDMP": "NNC Dev",
};

function walletName(addr) {
  return WALLET_NAMES[addr] ?? addr.substring(0, 8) + '...';
}



// ── STATE — SHARED ────────────────────────────────────────────
let firedAlerts    = loadSet(FIRED_FILE);
let firingNow      = new Set(); // race condition guard
let tokenInfoCache = {};
let tokenInfoInflight = {};
let creationCache  = {};
let skipCacheFast  = {};
let skipCacheSlow  = {};
let devWalletCache = {};

// ── STATE — FAST BOT ──────────────────────────────────────────
let fastAlerts  = {};

// ── STATE — FAST MIGRATION BOT ────────────────────────────────
let migAlerts = {};
let migFired  = loadSet('/tmp/sol_mig_fired.json');

// ── STATE — SLOW BOT ──────────────────────────────────────────
let slowAlerts  = {};


let pendingSigs    = new Set();
let seenPairs      = new Set(); // "wallet:mint" pairs already processed once — only the first buy matters
let walletEventTimes = {}; // wallet -> array of recent event timestamps (ms), for flood throttle

// ── WS STATE ──────────────────────────────────────────────────
let ws             = null;
let wsReady        = false;
let reconnectDelay = 5000;
let usingFallback  = false;
let subIdToWallet  = {};
let reqIdToWallet  = {};
let lastMessageAt  = Date.now();

// ── HELPERS ───────────────────────────────────────────────────
// ── LOG FILE ──────────────────────────────────────────────────
const LOG_FILE = '/tmp/sol_bot.log';
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
  const eastern = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const val = eastern.getHours() * 60 + eastern.getMinutes();
  return val >= 660 && val < 1080;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function fmtUsd(n) {
  if (!n || isNaN(n)) return 'N/A';
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `$${Math.round(n).toLocaleString()}`;
  return `$${n.toFixed(2)}`;
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
  // Some endpoints return data directly without code wrapper
  if (parsed && !parsed.code && !parsed.error) return parsed;
  return null;
}

async function fetchTokenInfo(mint) {
  return await gmgnGet('/v1/token/info', { chain: 'sol', address: mint });
}

// Pulls MC from GMGN's OFFICIAL market rank route (/v1/market/rank), which
// returns market_cap directly (curve-aware, no calculation). Brand-new tokens
// may not yet appear in the rank list — returns null if not found.
// Logs the raw shape so we can confirm auth works and the token is present.
async function fetchOfficialMarketCap(mint) {
  try {
    // Query the 1m trending rank, large limit, then find this token by address.
    const data = await gmgnGet('/v1/market/rank', {
      chain: 'sol', interval: '1m', orderby: 'volume', direction: 'desc', limit: '100',
    });
    const rank = data?.rank ?? data?.list ?? (Array.isArray(data) ? data : null);
    if (!Array.isArray(rank)) {
      log(`[OFFICIAL MC] ${mint.substring(0,8)} — rank route returned no array (auth/route issue?) raw keys=${data ? Object.keys(data).join(',') : 'null'}`);
      return null;
    }
    const hit = rank.find(t => (t.address ?? t.token_address) === mint);
    if (!hit) {
      log(`[OFFICIAL MC] ${mint.substring(0,8)} — not in trending rank (${rank.length} items), no MC`);
      return null;
    }
    const mc = parseFloat(hit.market_cap ?? hit.usd_market_cap ?? 0);
    log(`[OFFICIAL MC] ${mint.substring(0,8)} — found in rank, market_cap=${mc}`);
    return (mc > 0) ? mc : null;
  } catch (e) {
    log(`[ERR] fetchOfficialMarketCap: ${e.message}`);
    return null;
  }
}

// Pulls the latest USD price from GMGN's OFFICIAL kline route (/v1/market/token_kline)
// and computes MC = price × supply. This is GMGN's own curve-aware price, works on
// ANY token (not just trending), and is the most reliable source for fresh tokens.
// Uses 30s candles (finest resolution) so even very new tokens have a price.
// Returns { mc, price } or { mc:0 } if no candle/price yet.
async function fetchKlineMC(mint, supply) {
  try {
    const now = Math.floor(Date.now() / 1000);
    const from = now - 600; // last 10 minutes of 30s candles
    const data = await gmgnGet('/v1/market/token_kline', {
      chain: 'sol', address: mint, resolution: '30s',
      from: String(from), to: String(now),
    });
    const list = data?.list ?? (Array.isArray(data) ? data : null);
    if (!Array.isArray(list) || list.length === 0) {
      log(`[KLINE MC] ${mint.substring(0,8)} — no candles returned (auth/route issue or too fresh) raw keys=${data ? Object.keys(data).join(',') : 'null'}`);
      return { mc: 0, price: 0 };
    }
    // candles are chronological (oldest first) — take the most recent close price
    const last = list[list.length - 1];
    const price = parseFloat(last.close ?? last.c ?? 0);
    if (!(price > 0)) {
      log(`[KLINE MC] ${mint.substring(0,8)} — latest candle has no usable close price`);
      return { mc: 0, price: 0 };
    }
    const mc = (supply > 0) ? price * supply : 0;
    log(`[KLINE MC] ${mint.substring(0,8)} — close=${price} supply=${supply} => mc=${Math.round(mc)} (${list.length} candles)`);
    return { mc: mc > 0 ? mc : 0, price };
  } catch (e) {
    log(`[ERR] fetchKlineMC: ${e.message}`);
    return { mc: 0, price: 0 };
  }
}

async function fetchFreshWallets(mint) {
  const data = await gmgnGet('/v1/token/security', { chain: 'sol', address: mint });
  if (!data) return null;
  return data.fresh_holder_count ?? data.fresh_wallet_count ?? data.fresh_holders ?? null;
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

// ── DEXSCREENER ───────────────────────────────────────────────
async function dexFetch(url) {
  const headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
  };
  for (let attempt = 0; attempt < 4; attempt++) {
    const result = await new Promise((resolve) => {
      const req = https.get(url, { headers }, (res) => {
        if ([301,302,307,308].includes(res.statusCode) && res.headers.location) {
          res.resume();
          const rr = https.get(res.headers.location, { headers }, (res2) => {
            let d = ''; res2.on('data', c => d += c);
            res2.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve(null); } });
          });
          rr.on('error', () => resolve(null));
          rr.setTimeout(15000, () => { rr.destroy(); resolve(null); });
          return;
        }
        if (res.statusCode === 429) { res.resume(); resolve('429'); return; }
        if (res.statusCode !== 200) { res.resume(); resolve(null); return; }
        let d = ''; res.on('data', c => d += c);
        res.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve(null); } });
      });
      req.on('error', () => resolve(null));
      req.setTimeout(15000, () => { req.destroy(); resolve(null); });
    });
    if (result === '429') { await sleep((attempt+1)*5000); continue; }
    if (result) return result;
    if (attempt < 3) await sleep(2000);
  }
  return null;
}

async function fetchSameNameCount(mint, symbol) {
  const nowSecs = Math.floor(Date.now() / 1000);
  const cutoff = 5 * 3600;

  function countMatches(pairs, sym, excludeMint) {
    const uniqueMints = new Set();
    for (const pair of pairs) {
      if ((pair.chainId ?? pair.chain_id) !== 'solana') continue;
      if (pair.baseToken?.symbol?.toUpperCase() !== sym.toUpperCase()) continue;
      const addr = pair.baseToken?.address;
      if (!addr || addr === excludeMint) continue;
      const createdAt = pair.pairCreatedAt ?? pair.pair_created_at;
      if (!createdAt) continue;
      const ageSecs = nowSecs - Math.floor(createdAt / 1000);
      if (ageSecs >= 0 && ageSecs <= cutoff) uniqueMints.add(addr); // dedupe: one token can have several pairs
    }
    return uniqueMints.size;
  }

  // Same-name = how many tokens with this exact symbol launched in the last 5h.
  // This is a NAME search, independent of the current token — search DexScreener by symbol.
  if (!symbol || symbol === 'UNKNOWN') {
    log(`[Dex] No symbol for ${mint.substring(0, 8)} — can't do same-name search, returning null`);
    return null;
  }

  await new Promise(r => setTimeout(r, 1500)); // small breather after GMGN calls
  log(`[Dex] Searching same-name for ${symbol}...`);
  const r = await dexFetch(`https://api.dexscreener.com/latest/dex/search?q=${encodeURIComponent(symbol)}`);
  if (r) {
    const pairs = r.pairs ?? r.data ?? [];
    const count = countMatches(pairs, symbol, mint);
    log(`[Dex] Same-name search: ${symbol} — ${count} same-name tokens in last 5h`);
    return count;
  }

  log(`[Dex] Same-name search failed for ${symbol} — returning null`);
  return null;
}

// Fallback MC source: read marketCap (or fdv) from DexScreener's Solana pair
async function fetchDexMarketCap(mint) {
  const r = await dexFetch(`https://api.dexscreener.com/latest/dex/tokens/${mint}`);
  if (!r) return 0;
  const pairs = r.pairs ?? r.data ?? [];
  const solPairs = pairs.filter(p => (p.chainId ?? p.chain_id) === 'solana');
  let best = 0;
  for (const p of solPairs) {
    const mc = parseFloat(p.marketCap ?? p.fdv ?? 0);
    if (!isNaN(mc) && mc > best) best = mc;
  }
  return best;
}

// 5-minute volume from DexScreener. DISPLAY-ONLY — this never gates or delays
// a signal. It is called only AFTER a signal has already been cleared to fire,
// and any failure/timeout returns null so the signal still sends with "N/A".
async function fetchDexVolume5m(mint) {
  const r = await dexFetch(`https://api.dexscreener.com/latest/dex/tokens/${mint}`);
  if (!r) return null;
  const pairs = r.pairs ?? r.data ?? [];
  const solPairs = pairs.filter(p => (p.chainId ?? p.chain_id) === 'solana');
  let best = null;
  for (const p of solPairs) {
    const v = parseFloat(p.volume?.m5 ?? 0);
    if (!isNaN(v) && (best === null || v > best)) best = v;
  }
  return best;
}

// ── TOKEN AGE ─────────────────────────────────────────────────
async function getTokenAge(mint, maxAge, skipCache) {
  const now = Math.floor(Date.now() / 1000);
  if (skipCache[mint]) return -1;
  if (creationCache[mint]) {
    const age = now - creationCache[mint];
    if (age > maxAge) { skipCache[mint] = true; return -1; }
    return age;
  }
  const info = await getCachedTokenInfo(mint);
  if (!info) return null;
  const createdAt = info.creation_timestamp;
  if (!createdAt) return null;
  creationCache[mint] = createdAt;
  const age = now - createdAt;
  if (age > maxAge) { skipCache[mint] = true; return -1; }
  return age;
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

// ── NOTABLE HOLDERS (RPC-based) ───────────────────────────────
const NOTABLE_THRESHOLD = 50_000;
let solPriceCache = { price: null, ts: 0 };

async function getSolPrice() {
  const now = Math.floor(Date.now() / 1000);
  if (solPriceCache.price && now - solPriceCache.ts < 300) return solPriceCache.price;
  const info = await getCachedTokenInfo(SOL_MINT);
  const price = parseFloat(info?.price ?? 0);
  if (price > 0) solPriceCache = { price, ts: now };
  return solPriceCache.price ?? 150;
}

async function rpcPost(body) {
  for (const rpc of HTTP_RPCS) {
    const r = await httpsPost(rpc, body);
    if (r?.result) return r;
  }
  return null;
}

async function fetchNotableHolders(mint, tokenInfo) {
  try {
    const result = await rpcPost({
      jsonrpc: '2.0', id: 1, method: 'getTokenLargestAccounts',
      params: [mint, { commitment: 'confirmed' }]
    });
    const accounts = result?.result?.value ?? [];
    if (!accounts.length) return [];
    const solPrice = await getSolPrice();
    const tokenPrice = parseFloat(tokenInfo?.price ?? 0);
    const totalSupply = parseFloat(tokenInfo?.circulating_supply ?? tokenInfo?.total_supply ?? 0);
    const notable = []; const seen = new Set();
    for (const account of accounts.slice(0, 20)) {
      await sleep(200);
      const ownerRes = await rpcPost({
        jsonrpc: '2.0', id: 1, method: 'getAccountInfo',
        params: [account.address, { encoding: 'jsonParsed', commitment: 'confirmed' }]
      });
      const owner = ownerRes?.result?.value?.data?.parsed?.info?.owner ?? null;
      if (!owner || seen.has(owner) || WALLET_SET.has(owner)) continue;
      seen.add(owner);
      const tokenAmt = parseFloat(account.uiAmount ?? 0);
      const tokenVal = tokenPrice > 0 ? tokenAmt * tokenPrice : 0;
      const solRes = await rpcPost({
        jsonrpc: '2.0', id: 1, method: 'getBalance',
        params: [owner, { commitment: 'confirmed' }]
      });
      await sleep(200);
      const solVal = ((solRes?.result?.value ?? 0) / 1e9) * solPrice;
      const total = tokenVal + solVal;
      if (total >= NOTABLE_THRESHOLD) {
        const pctStr = totalSupply > 0 ? ` (${((tokenAmt/totalSupply)*100).toFixed(1)}%)` : '';
        const valStr = total >= 1_000_000 ? `$${(total/1_000_000).toFixed(1)}M` : `$${Math.round(total/1000)}k`;
        notable.push({ addr: owner, valStr, pctStr });
      }
    }
    return notable;
  } catch(e) { log(`[ERR] fetchNotableHolders: ${e.message}`); return []; }
}

// ── MINT EXTRACTION ───────────────────────────────────────────
// Returns the mint the TRACKED WALLET actually received in this tx (post balance > pre balance).
// Returns null for sells, transfers out, or tokens that merely passed through other accounts —
// this is what prevents false "wallet bought X" attributions.
function extractMint(tx, trackedWallet) {
  const meta = tx?.meta; const msg = tx?.transaction?.message;
  if (!meta || !msg) return null;
  const postBals = meta.postTokenBalances ?? [];
  const preBals  = meta.preTokenBalances ?? [];

  // pre-balance amounts for the tracked wallet, keyed by mint
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
    if (delta > 0 && delta > bestDelta) { bestDelta = delta; bestMint = b.mint; } // genuine increase = a buy/receive
  }
  return bestMint;
}

// ── BUY TRACKER (Theo / Cented) ───────────────────────────────
// Reads how many tokens the tracked wallet received in this tx for a given mint.
// Read-only helper for the buy tracker; does NOT affect signal logic.
function extractBuyAmount(tx, trackedWallet, mint) {
  const meta = tx?.meta;
  if (!meta) return 0;
  const postBals = meta.postTokenBalances ?? [];
  const preBals  = meta.preTokenBalances ?? [];
  let pre = 0, post = 0;
  for (const b of preBals)  { if (b.owner === trackedWallet && b.mint === mint) pre  = parseFloat(b.uiTokenAmount?.uiAmount ?? 0) || 0; }
  for (const b of postBals) { if (b.owner === trackedWallet && b.mint === mint) post = parseFloat(b.uiTokenAmount?.uiAmount ?? 0) || 0; }
  const delta = post - pre;
  return delta > 0 ? delta : 0;
}

function fmtTokenAmount(n) {
  if (!n || isNaN(n)) return 'N/A';
  if (n >= 1_000_000_000) return `${(n/1_000_000_000).toFixed(2)}B`;
  if (n >= 1_000_000)     return `${(n/1_000_000).toFixed(2)}M`;
  if (n >= 1_000)         return `${(n/1_000).toFixed(1)}K`;
  return `${Math.round(n).toLocaleString()}`;
}

// Net SOL the wallet put into the swap, read from the transaction.
// Uses native SOL balance delta + wSOL token balance delta.
// NOTE: this includes gas/priority/tip fees, so it can slightly
// overstate spend; used only as a fallback when reserve pricing fails.
function extractSolSpent(tx, trackedWallet) {
  const meta = tx?.meta;
  const msg  = tx?.transaction?.message;
  if (!meta || !msg) return 0;

  const keys = msg.accountKeys ?? [];
  let idx = -1;
  for (let i = 0; i < keys.length; i++) {
    const k = typeof keys[i] === 'string' ? keys[i] : keys[i]?.pubkey;
    if (k === trackedWallet) { idx = i; break; }
  }

  let nativeSpent = 0;
  if (idx >= 0 && Array.isArray(meta.preBalances) && Array.isArray(meta.postBalances)) {
    const pre  = meta.preBalances[idx]  ?? 0;
    const post = meta.postBalances[idx] ?? 0;
    nativeSpent = (pre - post) / 1e9;
  }

  let preW = 0, postW = 0;
  for (const b of (meta.preTokenBalances ?? []))  { if (b.owner === trackedWallet && b.mint === SOL_MINT) preW  = parseFloat(b.uiTokenAmount?.uiAmount ?? 0) || 0; }
  for (const b of (meta.postTokenBalances ?? [])) { if (b.owner === trackedWallet && b.mint === SOL_MINT) postW = parseFloat(b.uiTokenAmount?.uiAmount ?? 0) || 0; }
  const wsolSpent = preW - postW;

  const total = nativeSpent + (wsolSpent > 0 ? wsolSpent : 0);
  return total > 0 ? total : 0;
}

// Computes MC from GMGN's pool reserves, which GMGN already identifies and
// USD-prices correctly across all exchange types (pump, pump_amm, meteora).
// price/token = quote_reserve_value(USD) / base_reserve(tokens in pool)
// MC = price/token * circulating_supply.
// Works even when GMGN's headline market_cap field is missing or wrong.
// Returns { price, mc, symbol }; mc=0 if reserves/supply unavailable.
async function computePoolMC(mint, infoMaybe) {
  try {
    const info = infoMaybe || await fetchTokenInfo(mint);
    if (!info) return { price: 0, mc: 0, symbol: 'UNKNOWN' };
    const symbol = info.symbol ?? 'UNKNOWN';
    const pool = info.pool ?? {};
    const quoteValUsd = parseFloat(pool.quote_reserve_value ?? 0); // SOL side, USD
    const baseReserve = parseFloat(pool.base_reserve ?? 0);        // tokens in pool
    const supply = parseFloat(info.circulating_supply ?? info.total_supply ?? 0);

    if (quoteValUsd > 0 && baseReserve > 0 && supply > 0) {
      const pricePerToken = quoteValUsd / baseReserve;
      const mc = pricePerToken * supply;
      log(`[POOL MC] ${mint.substring(0,8)} quoteVal=${quoteValUsd} baseReserve=${baseReserve} supply=${supply} => price=${pricePerToken} mc=${Math.round(mc)}`);
      return { price: pricePerToken, mc: mc > 0 ? mc : 0, symbol };
    }
    log(`[POOL MC] ${mint.substring(0,8)} insufficient pool data — quoteVal=${quoteValUsd} baseReserve=${baseReserve} supply=${supply}`);
    return { price: 0, mc: 0, symbol };
  } catch (e) {
    log(`[ERR] computePoolMC: ${e.message}`);
    return { price: 0, mc: 0, symbol: 'UNKNOWN' };
  }
}

// Resolves market cap for a mint: GMGN tokenInfo → price×supply → DexScreener.
// Returns { mc, symbol, tokenPrice } with mc=0 if nothing available this attempt.
async function resolveBuyTrackerMC(tokenMint) {
  let symbol = 'UNKNOWN', mc = 0, tokenPrice = 0;
  // fresh fetch each attempt (not cached) so a late-populating MC can be picked up
  const info = await fetchTokenInfo(tokenMint);
  if (info) {
    tokenInfoCache[tokenMint] = info;
    symbol = info.symbol ?? 'UNKNOWN';
    tokenPrice = parseFloat(info.price ?? 0);
    mc = parseFloat(info.market_cap ?? info.usd_market_cap ?? 0);
    if ((isNaN(mc) || mc === 0) && tokenPrice > 0) {
      const supply = parseFloat(info.circulating_supply ?? info.total_supply ?? 0);
      if (supply > 0) mc = tokenPrice * supply;
    }
  }
  if (isNaN(mc) || mc === 0) {
    const dexMC = await fetchDexMarketCap(tokenMint);
    if (dexMC > 0) mc = dexMC;
  }
  if (isNaN(mc)) mc = 0;
  return { mc, symbol, tokenPrice };
}

// Sends a first-buy alert for Theo / Cented to their dedicated group.
// MC is the priority: retry every 3s for up to 20s to get it. If MC never
// resolves, the alert is still sent with "N/A" so a buy is never missed.
// Independent of slow/migration signals; never touches signal logic.
async function sendBuyTrackerAlert(trackedWallet, tokenMint, buyAmount, tx) {
  try {
    const cfg = TRACK_BUY_WALLETS[trackedWallet];
    if (!cfg) return;

    // Single tokenInfo fetch.
    let info = null;
    try { info = await fetchTokenInfo(tokenMint); }
    catch (e) { log(`[ERR] buyTracker fetchTokenInfo: ${e.message}`); }

    let symbol = info?.symbol ?? 'UNKNOWN';
    const tokenPrice = parseFloat(info?.price ?? 0);

    // ── MC SOURCE PRIORITY ──
    // 0) OFFICIAL kline price × supply (curve-aware, works on any token) — primary
    // 1) OFFICIAL market rank market_cap (only if token is trending)
    // 2) GMGN headline market_cap field
    // 3) GMGN price × supply
    // 4) pool-reserve calc (fallback — can be unreliable)
    // 5) DexScreener
    let mc = 0;
    let mcSource = 'none';
    const supplyForMC = parseFloat(info?.circulating_supply ?? info?.total_supply ?? 0);
    // 0) kline price × supply (best for fresh tokens)
    try {
      const k = await fetchKlineMC(tokenMint, supplyForMC);
      if (k.mc > 0) { mc = k.mc; mcSource = 'kline-30s'; }
    } catch (e) { log(`[ERR] fetchKlineMC call: ${e.message}`); }
    // 1) official rank route
    if (mc <= 0) {
      try {
        const off = await fetchOfficialMarketCap(tokenMint);
        if (off && off > 0) { mc = off; mcSource = 'official-rank'; }
      } catch (e) { log(`[ERR] fetchOfficialMarketCap call: ${e.message}`); }
    }
    // 2) / 3) GMGN headline, then price×supply
    if (mc <= 0 && info) {
      let g = parseFloat(info.market_cap ?? info.usd_market_cap ?? 0);
      if (!isNaN(g) && g > 0) { mc = g; mcSource = 'gmgn-headline'; }
      if (mc <= 0 && tokenPrice > 0 && supplyForMC > 0) { mc = tokenPrice * supplyForMC; mcSource = 'gmgn-price×supply'; }
    }
    // 4) pool-reserve calc only if GMGN gave nothing
    if (mc <= 0) {
      try {
        const pm = await computePoolMC(tokenMint, info);
        if (pm.mc > 0) { mc = pm.mc; mcSource = 'pool-calc'; }
        if (pm.symbol && pm.symbol !== 'UNKNOWN' && symbol === 'UNKNOWN') symbol = pm.symbol;
      } catch (e) { log(`[ERR] computePoolMC call: ${e.message}`); }
    }
    // 5) Last resort: DexScreener
    if (mc <= 0) {
      try { const dexMC = await fetchDexMarketCap(tokenMint); if (dexMC > 0) { mc = dexMC; mcSource = 'dexscreener'; } }
      catch (e) { log(`[ERR] buyTracker dexMC: ${e.message}`); }
    }

    const mcStr = (mc > 0) ? fmtUsd(mc) : 'N/A';

    // USD value of the buy, when we have a price
    let usdStr = '';
    if (tokenPrice > 0 && buyAmount > 0) {
      const usd = tokenPrice * buyAmount;
      usdStr = ` (~${fmtUsd(usd)})`;
    }

    const amtStr = buyAmount > 0 ? fmtTokenAmount(buyAmount) : 'N/A';
    const buyTime = new Date().toLocaleTimeString('en-US', { timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });

    sendTelegram(cfg.chatId,
      `🟢 <b>${cfg.name} BUY</b>\n\n` +
      `Token: #${symbol}\n` +
      `Contract: <code>${tokenMint}</code>\n` +
      `Amount: ${amtStr}${usdStr}\n` +
      `Market Cap: ${mcStr}\n` +
      `Time: ${buyTime}\n\n` +
      `🔗 <a href="https://gmgn.ai/sol/token/${tokenMint}">View on GMGN</a>`
    );
    log(`[BUY TRACKER] ${cfg.name} bought #${symbol} @ MC ${mcStr} (src=${mcSource}) — sent to ${cfg.chatId}`);
  } catch(e) { log(`[ERR] sendBuyTrackerAlert: ${e.message}`); }
}


// ── FAST BOT SIGNAL ───────────────────────────────────────────
async function buildMigrationSignal(tokenMint, walletCount, elapsed, tokenInfo, coordWallets, resolvedMC) {
  try {
    const now = Math.floor(Date.now()/1000);
    let symbol = 'UNKNOWN', mintTimeStr = 'N/A', ageStr = 'N/A';
    let liquidityStr = 'N/A', marketCapStr = 'N/A';
    let devWallet = null, devAth = 'N/A';

    if (tokenInfo) {
      symbol = tokenInfo.symbol ?? 'UNKNOWN';
      const ca = tokenInfo.creation_timestamp;
      if (ca) {
        mintTimeStr = new Date(ca*1000).toLocaleTimeString('en-US', { timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
        const s = now - ca; ageStr = s < 60 ? `${s}s` : `${Math.floor(s/60)}m ${s%60}s`;
      }
      const liq = parseFloat(tokenInfo.liquidity);
      if (!isNaN(liq)) liquidityStr = `$${liq.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
      let mc = parseFloat(tokenInfo.market_cap ?? tokenInfo.usd_market_cap);
      if (isNaN(mc) || mc === 0) { const p = parseFloat(tokenInfo.price); const s = parseFloat(tokenInfo.circulating_supply ?? tokenInfo.total_supply); if (!isNaN(p) && !isNaN(s) && p > 0 && s > 0) mc = p*s; }
      if ((isNaN(mc) || mc === 0) && resolvedMC > 0) mc = resolvedMC; // fall back to the MC that cleared the threshold (e.g. DexScreener)
      if (!isNaN(mc) && mc > 0) marketCapStr = `$${mc.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
      const ca2 = tokenInfo.dev?.creator_address; if (ca2) devWallet = ca2;
      const athInfo = tokenInfo.dev?.ath_token_info;
      if (athInfo?.ath_mc) { const p = parseFloat(athInfo.ath_mc); if (!isNaN(p)) { devAth = p >= 1_000_000 ? `$${(p/1_000_000).toFixed(1)}M${athInfo.symbol?' #'+athInfo.symbol:''}` : `$${p.toLocaleString('en-US',{maximumFractionDigits:0})}${athInfo.symbol?' #'+athInfo.symbol:''}`; } }
    }

    // If GMGN had no usable token info at all, still show the MC that cleared the threshold
    if (!tokenInfo && resolvedMC > 0) marketCapStr = `$${resolvedMC.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;

    // Display-only 5m volume (DexScreener). Runs after the signal is already cleared to fire.
    const vol5m = await fetchDexVolume5m(tokenMint).catch(() => null);
    const vol5mStr = (vol5m === null) ? 'N/A' : fmtUsd(vol5m);

    const signalTime = new Date().toLocaleTimeString('en-US', { timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });

    sendTelegram(CHAT_ID_FAST,
      `⚡ <b>Fast Signal — $38k in 30s (${walletCount}w)</b>\n\n` +
      `Token: #${symbol}\n` +
      `Contract: <code>${tokenMint}</code>\n` +
      `Mint Time: ${mintTimeStr}\n` +
      `Token Age at $40k: ${ageStr}\n` +
      `Liquidity: ${liquidityStr}\n` +
      `Market Cap: ${marketCapStr}\n` +
      `Vol (5m): ${vol5mStr}\n` +
      `Wallets: ${walletCount} bought within ${elapsed}s of mint\n` +
      `Buyers: ${coordWallets ? [...coordWallets].map(a => walletName(a)).join(', ') : 'N/A'}\n\n` +
      `Dev Wallet: ${devWallet ? `<code>${devWallet}</code>` : 'N/A'}\n` +
      `Dev ATH: ${devAth}\n\n` +
      `Signal Time: ${signalTime}\n\n` +
      `<a href="https://gmgn.ai/sol/token/${tokenMint}">GMGN</a>`
    );
    log(`[MIG] Signal sent for #${symbol} — ${walletCount} wallets, $40k+ in ${elapsed}s`);
  } catch(e) { log(`[ERR] buildMigrationSignal: ${e.message}`); }
}

// ── SLOW BOT SIGNAL FILTER ────────────────────────────────────
function slowShouldFire(symbol, sameNameCount, devWallet, devAthMc) {
  const devIsTracked = devWallet && devWallet !== 'N/A' && devWallet !== 'unknown' && WALLET_SET.has(devWallet);
  const devAthPasses = devWallet && devWallet !== 'N/A' && devAthMc !== null && devAthMc >= SLOW_DEV_ATH_THRESHOLD;
  const sameNamePasses = sameNameCount !== null && sameNameCount >= SLOW_SAME_NAME_THRESHOLD;
  if (sameNamePasses) { log(`[SLOW FILTER] ✅ same-name ${sameNameCount}`); return true; }
  if (devAthPasses) { log(`[SLOW FILTER] ✅ dev ATH ${fmtUsd(devAthMc)}`); return true; }
  if (devIsTracked) { log(`[SLOW FILTER] ✅ dev is tracked wallet`); return true; }
  log(`[SLOW FILTER] ❌ SUPPRESSED #${symbol} — same-name: ${sameNameCount??'?'}, devATH: ${fmtUsd(devAthMc)}`);
  return false;
}

async function buildSlowSignal(tokenMint, walletCount, elapsed, tokenInfo, coordWallets) {
  try {
    const t0 = Date.now();
    log(`[TIMING] buildSlowSignal ENTER ${tokenMint.substring(0,8)} | tokenInfo=${tokenInfo ? 'present' : 'NULL'} | devATH_raw=${tokenInfo?.dev?.ath_token_info?.ath_mc ?? 'none'}`);
    const now = Math.floor(Date.now()/1000);
    let symbol = 'UNKNOWN', mintTimeStr = 'N/A', ageStr = 'N/A';
    let liquidityStr = 'N/A', marketCapStr = 'N/A';
    let devWallet = 'N/A', devAth = 'N/A', devAthMc = null;
    let freshWalletsFromInfo = null;

    if (tokenInfo) {
      // ── TEMP VOL DEBUG ── GMGN keeps volume nested inside `stat` (and maybe `pool`),
      // and fresh-wallet data inside `wallet_tags_stat`. Dump all three so we can read
      // the exact field names for BOTH volume and fresh wallets. Remove after wiring.
      try {
        log(`[VOL DEBUG] ${tokenMint.substring(0,8)} stat = ${JSON.stringify(tokenInfo.stat)}`);
        log(`[VOL DEBUG] ${tokenMint.substring(0,8)} pool = ${JSON.stringify(tokenInfo.pool)}`);
        log(`[VOL DEBUG] ${tokenMint.substring(0,8)} wallet_tags_stat = ${JSON.stringify(tokenInfo.wallet_tags_stat)}`);
      } catch(e) { log(`[VOL DEBUG] dump failed: ${e.message}`); }

      symbol = tokenInfo.symbol ?? 'UNKNOWN';
      const ca = tokenInfo.creation_timestamp;
      if (ca) {
        mintTimeStr = new Date(ca*1000).toLocaleTimeString('en-US', { timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });
        const s = now - ca; ageStr = s < 60 ? `${s}s` : `${Math.floor(s/60)}m ${s%60}s`;
      }
      const liq = parseFloat(tokenInfo.liquidity);
      if (!isNaN(liq)) liquidityStr = `$${liq.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
      let mc = parseFloat(tokenInfo.market_cap ?? tokenInfo.usd_market_cap);
      if (isNaN(mc) || mc === 0) { const p = parseFloat(tokenInfo.price); const s = parseFloat(tokenInfo.circulating_supply ?? tokenInfo.total_supply); if (!isNaN(p) && !isNaN(s) && p > 0 && s > 0) mc = p*s; }
      if (!isNaN(mc) && mc > 0) marketCapStr = `$${mc.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
      const ca2 = tokenInfo.dev?.creator_address; if (ca2) devWallet = ca2;
      const athInfo = tokenInfo.dev?.ath_token_info;
      if (athInfo?.ath_mc) { const p = parseFloat(athInfo.ath_mc); if (!isNaN(p)) { devAthMc = p; devAth = p >= 1_000_000 ? `$${(p/1_000_000).toFixed(1)}M${athInfo.symbol?' #'+athInfo.symbol:''}` : `$${p.toLocaleString('en-US',{maximumFractionDigits:0})}${athInfo.symbol?' #'+athInfo.symbol:''}`; } }
      const fw = tokenInfo.wallet_tags_stat?.fresh_wallets; if (fw != null) freshWalletsFromInfo = fw;
    }

    const devAthPassesAlready = devAthMc !== null && devAthMc >= SLOW_DEV_ATH_THRESHOLD;
    const devIsTrackedAlready = devWallet && devWallet !== 'N/A' && devWallet !== 'unknown' && WALLET_SET.has(devWallet);
    log(`[TIMING] dev decision ${tokenMint.substring(0,8)} | devAthMc=${devAthMc ?? 'null'} | devAthPasses=${devAthPassesAlready} | devTracked=${devIsTrackedAlready} | will ${(!devAthPassesAlready && !devIsTrackedAlready) ? 'FETCH same-name (slow path)' : 'SKIP same-name (fast path)'}`);

    let sameNameCount = null;
    if (!devAthPassesAlready && !devIsTrackedAlready) {
      log(`[SLOW] Fetching same-name count for #${symbol} (${tokenMint.substring(0,8)})`);
      // Timeout guard: a hung/rate-limited DexScreener call must not freeze the signal.
      // On timeout, treat same-name as null — the OR filter can still fire on the dev condition.
      sameNameCount = await Promise.race([
        fetchSameNameCount(tokenMint, symbol),
        new Promise(resolve => setTimeout(() => resolve(null), 30000)),
      ]);
      log(`[SLOW] Same-name result: ${sameNameCount ?? 'null'} for #${symbol}`);
    } else {
      log(`[SLOW FILTER] ✅ Dev passes immediately — skipping DexScreener`);
    }

    if (!slowShouldFire(symbol, sameNameCount, devWallet, devAthMc)) return;

    const freshWallets = freshWalletsFromInfo ?? await fetchFreshWallets(tokenMint);
    // Display-only 5m volume (DexScreener). Runs only after the signal has already
    // passed slowShouldFire above, so it cannot affect or delay the fire decision.
    const vol5m = await fetchDexVolume5m(tokenMint).catch(() => null);
    const vol5mStr = (vol5m === null) ? 'N/A' : fmtUsd(vol5m);
    const notableHolders = await fetchNotableHolders(tokenMint, tokenInfo);
    let notableLine = '';
    if (notableHolders.length > 0) {
      notableLine = `\n\n💰 <b>Notable Holders (>$50k)</b>\n` +
        notableHolders.map(h => `  • <code>${h.addr}</code> — ${h.valStr}${h.pctStr}`).join('\n');
    }

    const signalTime = new Date().toLocaleTimeString('en-US', { timeZone: 'America/Toronto', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true });

    sendTelegram(CHAT_ID_SLOW,
      `🚨 <b>3-Wallet Signal</b>\n\n` +
      `Token: #${symbol}\n` +
      `Contract: <code>${tokenMint}</code>\n` +
      `Mint Time: ${mintTimeStr}\n` +
      `Token Age: ${ageStr}\n` +
      `Liquidity: ${liquidityStr}\n` +
      `Market Cap: ${marketCapStr}\n` +
      `Same-Name Count (5h): ${sameNameCount ?? '?'}\n` +
      `Fresh Wallets: ${freshWallets ?? 'N/A'}\n` +
      `Vol (5m): ${vol5mStr}\n` +
      `Wallets Coordinated: ${walletCount} within ${elapsed}s\n` +
      `Wallets: ${[...coordWallets].map(a => walletName(a)).join(', ')}\n\n` +
      `Dev Wallet: ${devWallet !== 'N/A' ? `<code>${devWallet}</code>` : 'N/A'}\n` +
      `Dev ATH: ${devAth}` +
      notableLine +
      `\n\nSignal Time: ${signalTime}\n\n` +
      `<a href="https://gmgn.ai/sol/token/${tokenMint}">GMGN</a>`
    );
    log(`[SLOW] Signal sent for #${symbol}`);
    log(`[TIMING] buildSlowSignal TOTAL ${tokenMint.substring(0,8)} took ${Date.now()-t0}ms`);
  } catch(e) { log(`[ERR] buildSlowSignal: ${e.message}`); }
}


// ── COORDINATION LOGIC ────────────────────────────────────────
const processing = new Set(); // synchronous guard against duplicate concurrent signals
const migResolving = new Set(); // guard — one migration MC-resolver per token at a time

// Resolve migration MC out-of-band so it never blocks the buy handler.
// GMGN cached → GMGN fresh re-fetch → DexScreener. Fires if any clears the threshold.
async function resolveMigration(tokenMint, now) {
  try {
    if (migFired.has(tokenMint)) return;
    const tokenInfo = await getCachedTokenInfo(tokenMint);
    let tokenMC = parseFloat(tokenInfo?.market_cap ?? tokenInfo?.usd_market_cap ?? 0);
    if ((isNaN(tokenMC) || tokenMC === 0) && tokenInfo?.price && tokenInfo?.circulating_supply) {
      tokenMC = parseFloat(tokenInfo.price) * parseFloat(tokenInfo.circulating_supply);
    }
    // Retry: cached info on a brand-new token often has no MC yet. Re-fetch fresh once.
    let retryInfo = tokenInfo;
    if (isNaN(tokenMC) || tokenMC === 0) {
      log(`[MIG] ${tokenMint.substring(0,8)} MC unknown — retrying fresh fetch in 3s`);
      await sleep(3000);
      retryInfo = await fetchTokenInfo(tokenMint);
      if (retryInfo) {
        tokenInfoCache[tokenMint] = retryInfo;
        tokenMC = parseFloat(retryInfo.market_cap ?? retryInfo.usd_market_cap ?? 0);
        if ((isNaN(tokenMC) || tokenMC === 0) && retryInfo.price && retryInfo.circulating_supply) {
          tokenMC = parseFloat(retryInfo.price) * parseFloat(retryInfo.circulating_supply);
        }
      }
    }
    // Fallback: GMGN still has no MC — try DexScreener
    if (isNaN(tokenMC) || tokenMC === 0) {
      log(`[MIG] ${tokenMint.substring(0,8)} MC still unknown — trying DexScreener`);
      const dexMC = await fetchDexMarketCap(tokenMint);
      if (dexMC > 0) { tokenMC = dexMC; log(`[MIG] ${tokenMint.substring(0,8)} DexScreener MC $${Math.round(dexMC).toLocaleString()}`); }
    }
    if (migFired.has(tokenMint)) return;
    // Bags tokens migrate at a higher MC (~$375k) than pump.fun (~$38k). Pick threshold by mint suffix.
    const migThreshold = tokenMint.toLowerCase().endsWith('bags') ? FAST_MIG_MIN_MC_BAGS : FAST_MIG_MIN_MC;
    if (tokenMC >= migThreshold) {
      const entry = migAlerts[tokenMint];
      const coordWallets = new Set(entry ? entry.wallets : []);
      const elapsed = now - (entry ? entry.firstSeenAt : now);
      migFired.add(tokenMint); saveSet('/tmp/sol_mig_fired.json', migFired);
      delete migAlerts[tokenMint];
      await buildMigrationSignal(tokenMint, coordWallets.size, elapsed, retryInfo, coordWallets, tokenMC);
    } else {
      log(`[MIG] ${tokenMint.substring(0,8)} MC ${tokenMC > 0 ? '$'+Math.round(tokenMC).toLocaleString() : 'unknown'} — below $${migThreshold.toLocaleString()} threshold`);
    }
  } finally {
    migResolving.delete(tokenMint);
  }
}

async function handleWalletBuy(trackedWallet, tokenMint) {
  if (firedAlerts.has(tokenMint)) return;

  if (!devWalletCache[tokenMint]) {
    const devInfo = await getCachedTokenInfo(tokenMint);
    devWalletCache[tokenMint] = devInfo?.dev?.creator_address ?? 'unknown';
    setTimeout(() => delete devWalletCache[tokenMint], 600000);
    // We already have token info here — cache the mint time so the migration
    // check below works on the FIRST buy (it needs creationCache populated).
    if (devInfo?.creation_timestamp && !creationCache[tokenMint]) {
      creationCache[tokenMint] = devInfo.creation_timestamp;
    }
  }
  if (devWalletCache[tokenMint] !== 'unknown' && trackedWallet === devWalletCache[tokenMint]) {
    log(`[SKIP] ${trackedWallet.substring(0,8)} is dev`); return;
  }

  const now = Math.floor(Date.now()/1000);


  // ── FAST MIGRATION BOT ──────────────────────────────────
  if (!migFired.has(tokenMint)) {
    const migAge = creationCache[tokenMint] ? now - creationCache[tokenMint] : null;
    if (migAge !== null && migAge <= FAST_MIG_MAX_AGE) {
      if (!migAlerts[tokenMint]) {
        migAlerts[tokenMint] = { wallets: new Set(), firstSeenAt: creationCache[tokenMint] ?? now };
      }
      migAlerts[tokenMint].wallets.add(trackedWallet);
      const mc = migAlerts[tokenMint].wallets.size;
      log(`[MIG] ${mc}/${FAST_MIG_MIN_WALLETS} for ${tokenMint.substring(0,8)} within ${migAge}s`);
      if (mc >= FAST_MIG_MIN_WALLETS && !migResolving.has(tokenMint)) {
        migResolving.add(tokenMint); // guard — only one resolver per token, no await before this
        resolveMigration(tokenMint, now).catch(e => {
          log(`[ERR] resolveMigration: ${e.message}`);
          migResolving.delete(tokenMint);
        });
      }
    }
  }

  // ── FAST BOT ────────────────────────────────────────────
  // (removed — only migration + slow signals are active)

  // ── SLOW BOT ────────────────────────────────────────────
  const slowAge = await getTokenAge(tokenMint, SLOW_MAX_TOKEN_AGE, skipCacheSlow);
  if (slowAge === -1) { log(`[SLOW SKIP] ${tokenMint.substring(0,8)} too old`); }
  else {
    // Allow unknown age through — same-name count and dev ATH filters will catch bad tokens
    if (slowAge === null) { log(`[SLOW] ${tokenMint.substring(0,8)} age unknown — allowing (filtered by same-name/dev ATH)`); }
    if (!slowAlerts[tokenMint]) {
      slowAlerts[tokenMint] = { wallets: new Set(), firstSeenAt: now };
    }
    const se = slowAlerts[tokenMint];
    if (now - se.firstSeenAt > SLOW_WINDOW_SECS) {
      log(`[SLOW RESET] ${tokenMint.substring(0,8)}`);
      slowAlerts[tokenMint] = { wallets: new Set(), firstSeenAt: now };
    }
    se.wallets.add(trackedWallet);
    if (se.wallets.size >= 2) {
      const names = [...se.wallets].map(w => walletName(w)).join(', ');
      log(`[SLOW] ${se.wallets.size}/${SLOW_MIN_WALLETS} for ${tokenMint} within ${now-se.firstSeenAt}s — wallets: ${names}`);
    }
    if (se.wallets.size >= SLOW_MIN_WALLETS) {
      if (firedAlerts.has(tokenMint) || processing.has(tokenMint)) return;
      processing.add(tokenMint); // synchronous — no await between check and add
      firedAlerts.add(tokenMint); saveSet(FIRED_FILE, firedAlerts);
      delete slowAlerts[tokenMint];
      const elapsed = now - se.firstSeenAt;
      const coordWallets = new Set(se.wallets);
      const tiA = Date.now();
      log(`[TIMING] 2/2 hit ${tokenMint.substring(0,8)} — fetching tokenInfo before signal`);
      const tokenInfo = await getCachedTokenInfo(tokenMint);
      log(`[TIMING] tokenInfo fetch took ${Date.now()-tiA}ms for ${tokenMint.substring(0,8)}`);
      await buildSlowSignal(tokenMint, se.wallets.size, elapsed, tokenInfo, coordWallets);
      processing.delete(tokenMint);
    }
  }

}


// ── LOG NOTIFICATION PROCESSING ──────────────────────────────
async function processLogNotification(params) {
  const value = params?.result?.value;
  const subId = params?.subscription;
  if (!value || (value.err !== null && value.err !== undefined)) return;

  const signature     = value.signature;
  const trackedWallet = subIdToWallet[subId];
  if (!trackedWallet) return;

  // #3: skip everything (including the tx fetch) outside active trading hours
  if (!isActiveHours()) return;

  if (pendingSigs.has(signature)) { return; }
  pendingSigs.add(signature);
  setTimeout(() => pendingSigs.delete(signature), 30000);

  // #1/#2: per-wallet flood throttle. A wallet firing absurdly fast (e.g. a dev spamming its
  // own token) only ever counts once toward a coordination signal (Sets dedupe by wallet),
  // so dropping its excess events here is safe and avoids hundreds of wasted getTransaction calls.
  const nowMs = Date.now();
  const times = (walletEventTimes[trackedWallet] ?? []).filter(t => nowMs - t < 10000);
  times.push(nowMs);
  walletEventTimes[trackedWallet] = times;
  if (times.length > 15) return; // >15 events in 10s from one wallet — flood/dump, drop silently (a real coordinated buy never bursts this fast on one wallet)

  log(`[LOG HIT] wallet ${trackedWallet.substring(0,8)} | sig ${signature.substring(0,12)}...`);

  let tx = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    tx = await getTransaction(signature);
    if (tx) break;
    await sleep(2000);
  }
  if (!tx) return;

  const mint = extractMint(tx, trackedWallet);
  if (!mint) return;

  // Only the first buy of a token by a given wallet matters (coordination counts distinct
  // wallets). Drop every repeat of this wallet+token; record on first sight.
  const pairKey = `${trackedWallet}:${mint}`;
  if (seenPairs.has(pairKey)) return;
  seenPairs.add(pairKey);

  log(`[MINT] ${trackedWallet.substring(0,8)} bought ${mint.substring(0,8)}`);

  // ── BUY TRACKER (Theo / Cented) ──
  // Parallel branch: on a first buy by a tracked-buy wallet, send to its group.
  // Fire-and-forget so it never delays or affects the signal path below.
  if (TRACK_BUY_WALLETS[trackedWallet]) {
    const buyAmount = extractBuyAmount(tx, trackedWallet, mint);
    sendBuyTrackerAlert(trackedWallet, mint, buyAmount, tx).catch(e => log(`[ERR] buyTracker: ${e.message}`));
  }

  await handleWalletBuy(trackedWallet, mint);
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
    usingFallback = !usingFallback;
    reconnectDelay = 5000;
    connect();
  }
}, 60000);

function connect() {
  const url = usingFallback ? WSS_FALLBACK : WSS_PRIMARY;
  log(`[WS] Connecting to ${usingFallback ? 'FALLBACK' : 'PRIMARY'}...`);
  ws = new WebSocket(url, { handshakeTimeout: 30000 });
  subIdToWallet = {}; reqIdToWallet = {}; wsReady = false;

  ws.on('open', async () => {
    log(`[WS] Connected — subscribing to ${WALLETS.length} wallets...`);
    wsReady = true; reconnectDelay = 5000; lastMessageAt = Date.now();
    // Send subscriptions PACED, not all at once. A burst of 100+ logsSubscribe
    // messages the instant the socket opens trips public-RPC rate limits (close 1013).
    // ~40ms apart spreads 109 subs over ~4-5s, which the server accepts.
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
    if (reconnectDelay >= 30000 && !usingFallback) { usingFallback = true; reconnectDelay = 5000; }
    setTimeout(() => connect(), reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 2, 60000);
  });
}

// ── CLEANUP ───────────────────────────────────────────────────
setInterval(() => {
  const now = Math.floor(Date.now()/1000);
  for (const mint of Object.keys(migAlerts)) { if (now - migAlerts[mint].firstSeenAt > FAST_MIG_MAX_AGE * 2) delete migAlerts[mint]; }
  for (const mint of Object.keys(slowAlerts)) { if (now - slowAlerts[mint].firstSeenAt > SLOW_WINDOW_SECS) delete slowAlerts[mint]; }
  if (seenPairs.size > 20000) { seenPairs.clear(); log(`[CLEANUP] seenPairs cleared`); }
  const cutMs = Date.now() - 10000;
  for (const w of Object.keys(walletEventTimes)) {
    walletEventTimes[w] = walletEventTimes[w].filter(t => t > cutMs);
    if (walletEventTimes[w].length === 0) delete walletEventTimes[w];
  }
}, 60000);

// ── HEALTH CHECK ──────────────────────────────────────────────
http.createServer((req, res) => {
  if (req.url === '/logs') {
    // Show last 500 log lines — hit /logs in your browser anytime
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end(logBuffer.join('\n'));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end(
    `SOLANA COMBINED BOT — LIVE\n` +
    `WS: ${wsReady ? 'connected' : 'reconnecting'}\n` +
    `Subscriptions: ${Object.keys(subIdToWallet).length}/${WALLETS.length}\n` +
    `Migration alerts: ${Object.keys(migAlerts).length} | Migration fired: ${migFired.size}\n` +
    `Slow alerts: ${Object.keys(slowAlerts).length}\n` +
    `Fired (coord): ${firedAlerts.size}\n` +
    `\nHit /logs to see last 500 log lines\n`
  );
}).listen(process.env.PORT || 3000, () => log(`[HTTP] Health server on port ${process.env.PORT || 3000}`));

// ── START ─────────────────────────────────────────────────────
log(`[START] Solana combined bot | ${WALLETS.length} wallets | Fast + Slow + Migration`);
log(`[START] WSS: ${WSS_PRIMARY.replace(/api_key=[^&]+/, 'api_key=***')}`);

https.get('https://api.ipify.org?format=json', (res) => {
  let d = ''; res.on('data', c => d += c);
  res.on('end', () => { try { log(`[IP] ${JSON.parse(d).ip}`); } catch {} });
}).on('error', () => {});

// Connect WebSocket
connect();

// Self-ping
if (RENDER_URL) {
  setInterval(() => {
    const mod = RENDER_URL.startsWith('https') ? https : http;
    mod.get(RENDER_URL + '/', res => log(`[PING] ${res.statusCode}`))
      .on('error', e => log(`[PING] ${e.message}`));
  }, 10 * 60_000);
}
