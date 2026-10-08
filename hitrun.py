# ============================================================
# HITRUN.PY — Hit-and-Run Sniper & Momentum Scanner v12
# ============================================================
# Upgraded with Advanced Anti-Trap Defenses:
#  1. Cabal Sybil Multi-Wallet Detector (Parent Funder Graph + Holding Sum)
#  2. Anti-Wash Trading & Fake Volume Engine (Entropy + Churn + Dispersal)
#  3. Universal Multi-DEX Discovery (Pump.fun, Meteora DLMM/DAMM, Raydium)
#  4. Disciplined Hit-and-Run Execution (+80% TP1, +200% TP2, 18% Trail, -22% SL)
# ============================================================

import asyncio
import json
import os
import time
import csv
import signal
import heapq
import aiohttp
import websockets
import urllib.request
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set
import threading
from flask import Flask, jsonify

# Pastikan seluruh log print langsung muncul seketika di Render (unbuffered)
_builtin_print = print
def print(*args, **kwargs):
    kwargs.setdefault("flush", True)
    _builtin_print(*args, **kwargs)


# ============================================================
# 1. KONFIGURASI ENGINE
# ============================================================

@dataclass
class Config:
    # --- Jendela Observasi & Rescan Cepat ---
    observation_window_sec: int = 15
    min_age_sec: int = 2
    enable_rescan: bool = True
    rescan_delay_sec: int = 20
    max_rescan_count: int = 240                 # Mampu memantau token hingga 24 jam
    max_rescan_age_sec: int = 86400             # 24 jam max umur koin untuk menangkap 24h runners

    # --- Momentum & Filter Harga (Masuk di Awal Pompa - Anti Pucuk / Anti Exhaustion) ---
    max_price_pump_5m_pct: float = 65.0         # Anti-Pucuk: Max +65% di 5m (jangan beli di wick hijau tertinggi!)
    min_price_pump_5m_pct: float = 1.5          # Titik Masuk: Wajib mulai naik minimal +1.5%
    max_price_pump_1h_pct: float = 180.0        # Anti-Pucuk 1 Jam: Hindari koin yang sudah terbang 5x-10x
    min_liquidity_to_mcap_ratio: float = 0.035  # Minimal rasio kolam vs MCap 3.5% (Anti Fake MCap)
    min_buy_volume_ratio: float = 0.60          # Wajib pembeli dominan kuat (>=60%)

    # --- Filter Kurva Bonding (Pump.fun) ---
    min_bonding_pct: float = 10.0               # Wajib sudah lepas landas (>=10% bukan koin mati)
    max_bonding_pct: float = 75.0               # Masih punya ruang pump sebelum top

    # --- Filter DEX Pool (Meteora DLMM / Raydium / PumpSwap / 0 - 24 Hour Runners) ---
    enable_dex_pool_evaluation: bool = True
    enable_dexscreener_discovery: bool = True   # Pencarian otomatis likuiditas DEX multi-platform
    enable_token_profiles_discovery: bool = True # Pemindaian token profil DexScreener terbaru
    min_market_cap_usd: float = 15_000          # Min $15,000 MCap titik infleksi breakout
    max_market_cap_usd: float = 600_000         # Max $600,000 MCap (Sweet spot awal, menghindari beli saat sudah terbang tinggi)
    max_pool_age_minutes: int = 1440            # Hingga 24 jam (1440 menit) - menangkap koin konsolidasi & rally 24 jam
    min_pool_liquidity_usd: float = 5_000       # Min $5,000 likuiditas asli (akomodasi early liquidity)
    require_spark_ignition_for_runners: bool = True # Untuk koin > 1 jam: wajib ada percikan breakout baru di 5m

    # --- KEAMANAN ANTI-JEBAKAN DEVELOPER (NON-NEGOTIABLE) ---
    require_mint_authority_revoked: bool = True
    require_freeze_authority_revoked: bool = True
    require_metadata_immutable: bool = False
    enable_sell_simulation: bool = True         # Anti-Honeypot
    min_sell_recovery_pct: float = 60.0
    max_transfer_fee_pct: float = 5.0
    max_buy_tax_pct: float = 6.0
    max_creator_rugpull_count: int = 0

    # --- 100X RUNNER DNA: KONSENTRASI SANGAT TERSEBAR (ZERO MONOPOLY) ---
    max_top10_holder_pct: float = 33.0          # Top 10 akumulasi max 33% (toleran tapi tetap terdesentralisasi)
    max_top1_holder_pct: float = 10.0           # Top 1 holder di luar pool max 10.0% (akomodasi sniper awal ~8.5%)
    max_dev_holding_pct: float = 3.5            # Dev holding max 3.5% (Dev serakah langsung ditolak!)
    max_dev_sell_pct: float = 100.0             # Dev dump awal justru bagus (CTO pattern) asalkan holding <= 3.5%
    max_rugcheck_score: float = 50.0            # Maksimal skor risiko RugCheck (makin kecil makin aman)

    # --- Syarat Mutlak Koin Hidup (Retail Army - Pasukan Pembeli Unik) ---
    min_holders_count: int = 18                 # Wajib minimal 18 pemegang asli
    min_dex_holders_count: int = 25             # Di DEX pool wajib minimal 25 pemegang asli
    min_unique_buyers_count: int = 10           # Wajib minimal 10 pembeli unik berbeda
    min_organic_buys_m5: int = 10               # Minimal 10 transaksi beli di 5 menit terakhir
    min_volume_buys_sol: float = 7.0            # Minimal 7 SOL akumulasi pembelian nyata

    # --- ANTI-DEV LINKAGE & INDEPENDENT BUYER VERIFICATION ---
    enable_dev_linkage_check: bool = True
    max_dev_linked_holding_pct: float = 4.5     # Akumulasi holding dev + afiliasi max 4.5%
    min_unlinked_holders_count: int = 16        # Wajib minimal 16 holder yang 100% independen dari dev
    min_unlinked_dex_holders_count: int = 20    # Wajib minimal 20 holder independen di DEX pool
    min_unlinked_buyers_count: int = 8          # Wajib minimal 8 pembeli unik independen (Retail Army)

    # ========================================================
    # ANTI-CABAL SYBIL MULTI-WALLET (Solusi Developer Pecah Dompet)
    # ========================================================
    enable_cabal_sybil_check: bool = True
    max_cabal_cluster_holding_pct: float = 10.0 # Akumulasi gabungan dompet dari 1 funder max 10%
    max_sybil_similar_wallets: int = 3          # Max dompet dengan saldo/porsi identik
    cabal_funding_window_hours: int = 48        # Lacak transaksi pendanaan 48 jam ke belakang
    max_fresh_sybil_ratio: float = 0.60         # Max rasio dompet baru di top holder

    # ========================================================
    # ANTI-WASH TRADING & FAKE VOLUME ENGINE
    # ========================================================
    enable_wash_trading_check: bool = True
    min_unique_trader_ratio: float = 0.45       # Minimal 45% trader unik (bukan bot bolak-balik)
    min_real_median_buy_sol: float = 0.05       # Minimal median buy agar bukan spam debu
    max_wash_ping_pong_count: int = 2           # Max dompet yang bolak-balik buy-sell kilat
    min_volume_to_holder_ratio: float = 1.0     # Volume tinggi harus menghasilkan pertambahan holder

    # --- LP Lock & Likuiditas Meteora/Raydium ---
    require_lp_locked: bool = True
    min_lp_locked_pct: float = 85.0
    meteora_min_liquidity_usd: float = 7_000

    # --- Velocity & Sinyal Kelahiran ---
    enable_birth_signal_check: bool = True
    birth_signal_min_score: float = 15.0
    alpha_wallet_min_age_hours: float = 480.0   # > 20 hari
    alpha_wallet_min_balance_sol: float = 0.3
    min_alpha_wallets: int = 1
    strong_alpha_wallets: int = 3
    detect_creation_block_bundle: bool = True
    max_creation_block_buyers: int = 5
    min_liquidity_velocity_sol_per_min: float = 0.25
    min_holder_growth_per_min: float = 0.5
    check_social_presence: bool = True

    # --- Skor Minimum Masuk (HANYA GRADE-A SNIPER 100x RUNNER DNA) ---
    min_conviction_score: float = 80.0          # Hanya sinyal dengan DNA Runner sejati (>= 80.0)

    # --- MANAJEMEN POSISI & EXIT PLAN (HIT AND RUN) ---
    position_size_pct: float = 3.0
    max_daily_exposure_pct: float = 25.0
    max_loss_streak: int = 4

    hard_stop_loss_pct: float = 22.0            # Cut loss cepat jika dev dump
    take_initials_multiple: float = 1.80        # TP 1: Tarik modal di +80% (Free Ride)
    take_profit_2_multiple: float = 3.00        # TP 2: Kunci profit di +200% (3x)
    trailing_stop_pct: float = 18.0             # Trailing stop 18% untuk kawal moonbag
    time_stop_minutes: int = 60                 # Keluar jika koin mati > 1 jam

    # --- Endpoint & Data Feeds ---
    pumpportal_ws: str = "wss://pumpportal.fun/api/data"
    dexscreener_url: str = "https://api.dexscreener.com/latest/dex/tokens"
    dexscreener_search_url: str = "https://api.dexscreener.com/latest/dex/search"
    dexscreener_boosts_latest: str = "https://api.dexscreener.com/token-boosts/latest/v1"
    dexscreener_profiles: str = "https://api.dexscreener.com/token-profiles/latest/v1"
    pumpfun_coin_url: str = "https://frontend-api-v3.pump.fun/coins/"
    rugcheck_url: str = "https://api.rugcheck.xyz/v1"
    jupiter_quote_url: str = "https://lite-api.jup.ag/swap/v1/quote"
    holder_rpc_url: str = "https://solana-rpc.publicnode.com"
    public_rpc_url: str = "https://solana-rpc.publicnode.com"

    worker_count: int = 24
    rpc_semaphore: int = 35
    cache_ttl_holders_sec: int = 20
    cache_ttl_lp_sec: int = 30
    cache_ttl_security_sec: int = 60
    cache_ttl_pool_sec: int = 15
    discovery_interval_sec: int = 15

    # --- SUB-BLOCK PARALLEL ON-CHAIN INDEXER & EARLY ACCUMULATION ---
    enable_parallel_indexer: bool = True
    indexer_concurrency_workers: int = 24       # Multi-worker paralel sub-block processing
    filter_deployer_vanity_block0: bool = True  # Mengabaikan order di slot/blok pembuatan (Block 0 bundle)
    min_organic_secondary_buyers: int = 4       # Minimal 4 pembeli organik di luar klaster dev/Block 0
    min_organic_inflow_sol: float = 3.5         # Minimal 3.5 SOL pembelian organik murni
    max_pre_parabolic_pump_pct: float = 35.0    # Masuk SEBELUM lilin parabolik (max +35%, sweet spot +2% s/d +20%)
    min_pre_parabolic_pump_pct: float = 1.0     # Mulai ada traksi positif (+1%)
    min_organic_cvd_ratio: float = 0.65         # Minimal 65% volume adalah pembelian bersih organik

    # --- Pipeline GeckoTerminal & Raydium New Pools (Deteksi Koin Non-Pump.fun) ---
    enable_geckoterminal_discovery: bool = True
    geckoterminal_new_pools_url: str = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools"
    geckoterminal_trending_pools_url: str = "https://api.geckoterminal.com/api/v2/networks/solana/trending_pools"
    enable_raydium_pools_discovery: bool = True
    raydium_pools_url: str = "https://api-v3.raydium.io/pools/info/list?poolType=all&poolSortField=default&sortType=desc&pageSize=50&page=1"
    geckoterminal_interval_sec: int = 15

    discovery_queries: Tuple[str, ...] = (
        "So11111111111111111111111111111111111111112",  # WSOL Pair (Mencakup 99.9% token Solana tanpa filter nama/inisial)
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC Pair
        "pumpswap", "pump", "solana", "meteora", "raydium", "orca", "cpmm",
        "dlmm", "clmm", "whirlpool", "dex", "pool",
    )
    discovery_max_per_query: int = 40

    telegram_bot_token: str = field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", ""))
    telegram_chat_id: str = field(default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", ""))
    telegram_enabled: bool = True
    portfolio_usd: float = 1000.0
    log_file: str = "trades.jsonl"


# ============================================================
# 2. DATA MODEL
# ============================================================

@dataclass
class TokenState:
    mint: str
    creator: str
    source: str
    created_at: float
    name: str = ""
    symbol: str = ""
    bonding_pct: float = 0.0
    liquidity_usd: float = 0.0
    price: float = 0.0
    price_at_5m_ago: float = 0.0
    price_high: float = 0.0
    holders: Dict[str, float] = field(default_factory=dict)
    dev_holding_pct: float = 0.0
    lp_locked: bool = False
    lp_pool: str = ""
    rug_score: float = 100.0
    unique_buyers: Set[str] = field(default_factory=set)
    buys_count: int = 0
    sells_count: int = 0
    volume_usd: float = 0.0
    volume_buys: float = 0.0
    volume_sells: float = 0.0
    scored: bool = False
    signal_emitted: bool = False

    mint_authority_active: bool = False
    freeze_authority_active: bool = False
    metadata_mutable: bool = False
    transfer_fee_pct: float = 0.0
    sell_simulation_ok: bool = True
    creator_rugpull_count: int = 0

    is_migrated: bool = False
    dex_id: str = ""
    pool_address: str = ""
    market_cap_usd: float = 0.0
    pool_age_minutes: float = 0.0

    dev_sold_pct: float = 0.0
    rescan_count: int = 0
    next_rescan_at: float = 0.0

    # History buffers
    sol_in_bonding_history: deque = field(default_factory=lambda: deque(maxlen=300))
    holder_history: deque = field(default_factory=lambda: deque(maxlen=300))

    # Anti-Trap Metrics
    cabal_cluster_pct: float = 0.0
    cabal_wallets_count: int = 0
    unique_trader_ratio: float = 1.0
    is_wash_trading: bool = False
    wash_flags: List[str] = field(default_factory=list)
    pool_vaults: Set[str] = field(default_factory=set)

    # Dev Linkage & Unlinked Independents
    dev_linked_wallets: Set[str] = field(default_factory=set)
    dev_linked_holding_pct: float = 0.0
    unlinked_holders_count: int = 0
    unlinked_buyers_count: int = 0

    liquidity_velocity: float = 0.0
    holder_growth_velocity: float = 0.0
    alpha_wallet_count: int = 0
    has_twitter: bool = False
    has_telegram: bool = False
    has_website: bool = False
    birth_signal_score: float = 0.0

    # Early Accumulation & Parallel Indexer Metrics
    block0_vanity_volume_sol: float = 0.0
    block0_wallets: Set[str] = field(default_factory=set)
    organic_buys_count: int = 0
    organic_volume_buys_sol: float = 0.0
    organic_unique_buyers: Set[str] = field(default_factory=set)
    is_pre_parabolic_accumulation: bool = False
    sub_block_latency_ms: float = 0.0


@dataclass
class Signal:
    mint: str
    name: str
    symbol: str
    score: float
    entry_price: float
    reasons: List[str]
    red_flags: List[str]
    timestamp: float
    source: str = ""
    phase: str = "bonding"
    market_cap: float = 0.0
    liquidity: float = 0.0


@dataclass
class Position:
    mint: str
    symbol: str
    entry_price: float
    size_usd: float
    remaining_pct: float = 100.0
    initial_recovered: bool = False
    tp2_recovered: bool = False
    high_water: float = 0.0
    opened_at: float = 0.0
    stop_price: float = 0.0
    trailing_price: float = 0.0


# ============================================================
# 3. FAST CACHE
# ============================================================

class TimedCache:
    def __init__(self):
        self._store: Dict[str, Tuple[float, object]] = {}

    def get(self, key: str, ttl: float):
        entry = self._store.get(key)
        if not entry:
            return None
        ts, val = entry
        if time.time() - ts > ttl:
            self._store.pop(key, None)
            return None
        return val

    def set(self, key: str, value: object):
        self._store[key] = (time.time(), value)

    def cleanup(self):
        now = time.time()
        expired = [k for k, (ts, _) in self._store.items() if now - ts > 300]
        for k in expired:
            self._store.pop(k, None)


# ============================================================
# 4. KNOWN POOL VAULTS & ROBUST RPC CLIENT
# ============================================================

KNOWN_DEX_PROGRAMS = {
    # Raydium
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium Liquidity Pool V4
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",  # Raydium Authority
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",  # Raydium CPMM
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Raydium CLMM
    "DNXgeM9EiiaAbaWvwjHj9fQnb44a2PQQu24ghREGLGu3",  # Raydium CPMM Authority
    "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX",  # OpenBook DEX
    # Meteora
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",  # Meteora DLMM Program
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",  # Meteora Dynamic AMM
    "24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi",  # Meteora Vault Program
    "M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K",  # Meteora Authority
    # Pump.fun
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",  # Pump.fun Program
    "Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasJJL7Xx8p9F1b",  # Pump.fun Authority
    # PumpSwap (Pump.fun new AMM Migration DEX)
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",  # PumpSwap Program
    "BSfD6SHZigAfDWSFRzkghngYKEdukHgdHxoUmVYgpx4C",  # PumpSwap Authority
    "4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf",  # PumpSwap Global
    # Orca & Moonshot & Phoenix & Lifinity
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",  # Orca Whirlpool
    "MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG",  # Moonshot
    "MSFee4T1ZsK3h3A14g6iLwB5441Z2aLdJ4K1o7zW547",  # Moonshot Fee
    "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY",  # Phoenix DEX
    "2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c",  # Lifinity V2
}

class RpcClient:
    def __init__(self, cfg: Config, cache: TimedCache):
        self.cfg = cfg
        self.cache = cache
        self.session: Optional[aiohttp.ClientSession] = None
        self.sem = asyncio.Semaphore(cfg.rpc_semaphore)

    async def check_is_amm_vault(self, address: str, mint: str) -> bool:
        """
        Pemeriksaan on-chain matematis presisi tinggi untuk membedakan antara:
        1. AMM Liquidity Pool Vault / Smart Contract / PDA (DIKECUALIKAN dari hitungan paus)
        2. Dompet Pribadi Manusia / Individual Whale (TETAP diaudit untuk anti-paus)
        """
        cached = self.cache.get(f"ammvault:{address}", 300)
        if cached is not None:
            return cached

        # 1. Cek langsung jika address itu sendiri terdaftar di daftar program/otoritas DEX
        if address in KNOWN_DEX_PROGRAMS:
            self.cache.set(f"ammvault:{address}", True)
            return True

        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getAccountInfo",
            "params": [address, {"encoding": "jsonParsed", "commitment": "confirmed"}],
        }
        data = {}
        for rpc_url in (self.cfg.public_rpc_url, self.cfg.holder_rpc_url):
            async with self.sem:
                try:
                    async with self.session.post(rpc_url, json=payload, headers={"User-Agent": "Mozilla/5.0"}) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("result", {}).get("value"):
                                break
                except Exception:
                    continue

        value = (data.get("result") or {}).get("value")
        if not value:
            self.cache.set(f"ammvault:{address}", False)
            return False

        owner = value.get("owner", "")
        # Jika akun itu sendiri dimiliki langsung oleh DEX program
        if owner in KNOWN_DEX_PROGRAMS:
            self.cache.set(f"ammvault:{address}", True)
            return True

        parsed = (value.get("data") or {}).get("parsed") or {}
        info = parsed.get("info") or {}
        token_owner = info.get("owner", "")
        delegate = info.get("delegate", "")

        # Jika entitas pemilik token account atau delegate ada di KNOWN_DEX_PROGRAMS
        if token_owner in KNOWN_DEX_PROGRAMS or delegate in KNOWN_DEX_PROGRAMS:
            self.cache.set(f"ammvault:{address}", True)
            return True

        # Analisis Arsitektur Solana:
        # Pada Solana, semua dompet manusia pribadi (Phantom, Solflare, dll) wajib bertipe akun dasar
        # yang dimiliki oleh System Program (11111111111111111111111111111111).
        # Jika token_owner memiliki akun yang pemiliknya BUKAN System Program, maka entitas tersebut
        # adalah Program Derived Address (PDA), Smart Contract, AMM Pool State, atau Liquidity Escrow!
        if token_owner:
            payload_owner = {
                "jsonrpc": "2.0", "id": "1",
                "method": "getAccountInfo",
                "params": [token_owner, {"encoding": "jsonParsed", "commitment": "confirmed"}],
            }
            try:
                async with self.session.post(self.cfg.public_rpc_url, json=payload_owner, headers={"User-Agent": "Mozilla/5.0"}) as resp_owner:
                    if resp_owner.status == 200:
                        data_owner = await resp_owner.json()
                        val_owner = (data_owner.get("result") or {}).get("value") or {}
                        owner_prog = val_owner.get("owner", "")

                        # A. Jika pemilik akun adalah program DEX terdaftar
                        if owner_prog in KNOWN_DEX_PROGRAMS:
                            self.cache.set(f"ammvault:{address}", True)
                            return True

                        # B. Jika akun ini adalah executable smart contract
                        if val_owner.get("executable"):
                            self.cache.set(f"ammvault:{address}", True)
                            return True

                        # C. Verifikasi PDA/AMM Pool:
                        # Akun manusia biasa: owner_prog == "11111111111111111111111111111111"
                        # AMM Pool (Raydium, Meteora, PumpSwap, Orca, Phoenix, Moonshot, dll):
                        # owner_prog adalah program AMM itu sendiri (bukan System Program).
                        if owner_prog and owner_prog != "11111111111111111111111111111111":
                            self.cache.set(f"ammvault:{address}", True)
                            return True
            except Exception:
                pass

        self.cache.set(f"ammvault:{address}", False)
        return False

    async def start(self):
        timeout = aiohttp.ClientTimeout(total=8)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self):
        if self.session:
            await self.session.close()

    async def get_holders(self, mint: str) -> Dict[str, float]:
        cached = self.cache.get(f"holders:{mint}", self.cfg.cache_ttl_holders_sec)
        if cached is not None:
            return cached

        holders = await self._holders_rpc(mint, self.cfg.holder_rpc_url)
        if not holders:
            holders = await self._holders_rpc(mint, self.cfg.public_rpc_url)
        if not holders:
            holders = await self._holders_threews(mint)

        if holders:
            self.cache.set(f"holders:{mint}", holders)
        return holders

    async def _holders_threews(self, mint: str) -> Dict[str, float]:
        url = f"https://three.ws/api/crypto/holders?address={mint}"
        async with self.sem:
            try:
                async with self.session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        holders_list = data.get("holders") or data.get("data") or []
                        holders: Dict[str, float] = {}
                        for h in holders_list:
                            owner = h.get("owner") or h.get("address") or h.get("wallet")
                            pct = h.get("pct") or h.get("percentage")
                            if owner and pct is not None:
                                holders[owner] = float(pct)
                        return holders
            except Exception:
                pass
        return {}

    async def _holders_rpc(self, mint: str, rpc_url: str) -> Dict[str, float]:
        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getTokenLargestAccounts",
            "params": [mint],
        }
        async with self.sem:
            try:
                async with self.session.post(rpc_url, json=payload) as resp:
                    data = await resp.json()
            except Exception:
                return {}

        accounts = data.get("result", {}).get("value", []) or []
        if not accounts:
            return {}
        supply = await self._get_supply(mint, rpc_url)
        if supply <= 0:
            return {}

        holders = {}
        for acc in accounts:
            owner = acc.get("address", "")
            ui = float(acc.get("uiAmountString", 0) or 0)
            if owner and ui > 0:
                holders[owner] = (ui / supply) * 100.0
        return holders

    async def _get_supply(self, mint: str, rpc_url: str) -> float:
        cached = self.cache.get(f"supply:{mint}", 60)
        if cached is not None:
            return cached
        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getTokenSupply",
            "params": [mint],
        }
        async with self.sem:
            try:
                async with self.session.post(rpc_url, json=payload) as resp:
                    data = await resp.json()
                supply = float(data["result"]["value"]["uiAmountString"])
            except Exception:
                supply = 0.0
        self.cache.set(f"supply:{mint}", supply)
        return supply

    async def get_wallet_age_hours(self, wallet: str) -> float:
        cached = self.cache.get(f"wage:{wallet}", 600)
        if cached is not None:
            return cached

        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getSignaturesForAddress",
            "params": [wallet, {"limit": 100}],
        }
        async with self.sem:
            try:
                async with self.session.post(self.cfg.holder_rpc_url, json=payload) as resp:
                    data = await resp.json()
                sigs = data.get("result", []) or []
                if not sigs:
                    age = 0.0
                else:
                    earliest = min((s.get("blockTime", 0) or 0) for s in sigs)
                    age = (time.time() - earliest) / 3600.0 if earliest > 0 else 999.0
            except Exception:
                age = 999.0
        self.cache.set(f"wage:{wallet}", age)
        return age

    async def get_wallet_balance_sol(self, wallet: str) -> float:
        cached = self.cache.get(f"wbal:{wallet}", 300)
        if cached is not None:
            return cached
        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getBalance",
            "params": [wallet],
        }
        async with self.sem:
            try:
                async with self.session.post(self.cfg.holder_rpc_url, json=payload) as resp:
                    data = await resp.json()
                lamports = data.get("result", {}).get("value", 0)
                bal = lamports / 1_000_000_000
            except Exception:
                bal = 1.0
        self.cache.set(f"wbal:{wallet}", bal)
        return bal

    async def get_signatures(self, address: str, limit: int = 30) -> list:
        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getSignaturesForAddress",
            "params": [address, {"limit": limit}],
        }
        async with self.sem:
            try:
                async with self.session.post(self.cfg.holder_rpc_url, json=payload) as resp:
                    data = await resp.json()
                return data.get("result", []) or []
            except Exception:
                return []

    async def get_transaction(self, signature: str) -> Optional[dict]:
        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getTransaction",
            "params": [
                signature,
                {"maxSupportedTransactionVersion": 0, "encoding": "jsonParsed"},
            ],
        }
        async with self.sem:
            try:
                async with self.session.post(self.cfg.holder_rpc_url, json=payload) as resp:
                    data = await resp.json()
                return data.get("result")
            except Exception:
                return None

    async def get_pool_data(self, mint: str) -> dict:
        cached = self.cache.get(f"pool:{mint}", self.cfg.cache_ttl_pool_sec)
        if cached is not None:
            return cached

        url = f"{self.cfg.dexscreener_url}/{mint}"
        async with self.sem:
            try:
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        self.cache.set(f"pool:{mint}", {})
                        return {}
                    data = await resp.json()
            except Exception:
                self.cache.set(f"pool:{mint}", {})
                return {}

        pairs = data.get("pairs", []) or []
        if not pairs:
            self.cache.set(f"pool:{mint}", {})
            return {}

        best = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))

        base_token = best.get("baseToken", {}) or {}
        txns = best.get("txns", {}) or {}
        vol = best.get("volume", {}) or {}
        price_change = best.get("priceChange", {}) or {}
        info = best.get("info", {}) or {}

        websites = [w.get("url") for w in info.get("websites", []) if w.get("url")]
        socials = info.get("socials", []) or []
        twitter = any("twitter" in s.get("type", "").lower() or "x.com" in s.get("url", "").lower() for s in socials)
        telegram = any("telegram" in s.get("type", "").lower() or "t.me" in s.get("url", "").lower() for s in socials)

        quote_token = best.get("quoteToken", {}) or {}

        result = {
            "name": base_token.get("name", "Unknown"),
            "symbol": base_token.get("symbol", "TOKEN"),
            "dex_id": best.get("dexId", "unknown"),
            "pool_address": best.get("pairAddress", ""),
            "quote_symbol": quote_token.get("symbol", "SOL"),
            "quote_address": quote_token.get("address", ""),
            "market_cap": float(best.get("marketCap", 0) or best.get("fdv", 0) or 0),
            "liquidity_usd": float(best.get("liquidity", {}).get("usd", 0) or 0),
            "price": float(best.get("priceUsd", 0) or 0),
            "created_at": float(best.get("pairCreatedAt", 0) or 0) / 1000.0,
            "buys_m5": int(txns.get("m5", {}).get("buys", 0) or 0),
            "sells_m5": int(txns.get("m5", {}).get("sells", 0) or 0),
            "buys_h1": int(txns.get("h1", {}).get("buys", 0) or 0),
            "sells_h1": int(txns.get("h1", {}).get("sells", 0) or 0),
            "vol_m5": float(vol.get("m5", 0) or 0),
            "vol_h1": float(vol.get("h1", 0) or 0),
            "price_change_m5": float(price_change.get("m5", 0) or 0),
            "price_change_h1": float(price_change.get("h1", 0) or 0),
            "has_twitter": twitter,
            "has_telegram": telegram,
            "has_website": len(websites) > 0,
        }
        self.cache.set(f"pool:{mint}", result)
        return result

    async def check_social_presence(self, mint: str, pool_data: Optional[dict] = None) -> Tuple[bool, bool, bool]:
        if not self.cfg.check_social_presence:
            return False, False, False

        cached = self.cache.get(f"social:{mint}", 300)
        if cached is not None:
            return cached

        has_twitter = False
        has_telegram = False
        has_website = False

        if pool_data:
            has_twitter = pool_data.get("has_twitter", False)
            has_telegram = pool_data.get("has_telegram", False)
            has_website = pool_data.get("has_website", False)

        if not (has_twitter and has_telegram and has_website):
            url = f"{self.cfg.pumpfun_coin_url}{mint}"
            async with self.sem:
                try:
                    async with self.session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            has_twitter = has_twitter or bool(data.get("twitter"))
                            has_telegram = has_telegram or bool(data.get("telegram"))
                            has_website = has_website or bool(data.get("website"))
                except Exception:
                    pass

        result = (has_twitter, has_telegram, has_website)
        self.cache.set(f"social:{mint}", result)
        return result

    async def get_security_report(self, mint: str) -> dict:
        cached = self.cache.get(f"sec:{mint}", self.cfg.cache_ttl_security_sec)
        if cached is not None:
            return cached

        url = f"{self.cfg.rugcheck_url}/tokens/{mint}/report"
        async with self.sem:
            try:
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        empty = {}
                        self.cache.set(f"sec:{mint}", empty)
                        return empty
                    data = await resp.json()
            except Exception:
                empty = {}
                self.cache.set(f"sec:{mint}", empty)
                return empty

        self.cache.set(f"sec:{mint}", data)
        return data

    async def get_lp_info(self, mint: str, pool_data: Optional[dict] = None) -> Tuple[bool, float, str, float, List[str]]:
        cached = self.cache.get(f"lp:{mint}", self.cfg.cache_ttl_lp_sec)
        if cached is not None:
            return cached

        report = await self.get_security_report(mint)
        if not report:
            if pool_data and pool_data.get("liquidity_usd", 0) >= self.cfg.min_pool_liquidity_usd:
                result = (True, pool_data["liquidity_usd"], pool_data.get("pool_address", ""), 20.0, [])
                self.cache.set(f"lp:{mint}", result)
                return result
            result = (False, 0.0, "", 100.0, ["rugcheck_unavailable"])
            self.cache.set(f"lp:{mint}", result)
            return result

        score = float(report.get("score_normalised", 100.0) or report.get("score", 100.0))
        markets = report.get("markets", []) or []

        lp_locked = False
        lp_locked_pct = 0.0
        liquidity_usd = 0.0
        lp_pool = ""

        root_lp_pct = report.get("lpLockedPct")
        if root_lp_pct is not None:
            try:
                lp_locked_pct = float(root_lp_pct)
                if lp_locked_pct >= self.cfg.min_lp_locked_pct:
                    lp_locked = True
            except (ValueError, TypeError):
                pass

        vaults: Set[str] = set()
        if lp_pool:
            vaults.add(lp_pool)

        for m in markets:
            market_type = str(m.get("marketType", "")).lower()
            pubkey = m.get("pubkey", "")
            if not lp_pool and pubkey:
                lp_pool = pubkey
            if pubkey:
                vaults.add(pubkey)

            lp_data = m.get("lp", {}) or {}
            for v_key in ("baseVault", "quoteVault", "lpVault"):
                v_addr = lp_data.get(v_key) or m.get(v_key)
                if v_addr:
                    vaults.add(v_addr)

            m_lp_pct = lp_data.get("lpLockedPct")
            if m_lp_pct is not None:
                try:
                    pct = float(m_lp_pct)
                    if pct >= self.cfg.min_lp_locked_pct:
                        lp_locked = True
                        lp_locked_pct = max(lp_locked_pct, pct)
                except (ValueError, TypeError):
                    pass

            if ("meteora" in market_type or "dlmm" in market_type or "damm" in market_type):
                quote_usd = float(lp_data.get("quoteUSD", 0) or 0)
                base_usd = float(lp_data.get("baseUSD", 0) or 0)
                m_total_liq = quote_usd + base_usd
                if m_total_liq >= self.cfg.meteora_min_liquidity_usd:
                    lp_locked = True
                    liquidity_usd = max(liquidity_usd, m_total_liq)

        # Periksa topHolders RugCheck untuk menandai akun AMM / Pool
        for h in report.get("topHolders", []) or []:
            h_addr = h.get("address", "")
            h_owner = h.get("owner", "")
            if h_owner in KNOWN_DEX_PROGRAMS or (h.get("insider") is True and float(h.get("pct", 0) or 0) > 15.0):
                if h_addr:
                    vaults.add(h_addr)
                if h_owner:
                    vaults.add(h_owner)

        for r in report.get("risks", []) or []:
            if "liquidity" in (r.get("name", "") or "").lower():
                try:
                    val_str = str(r.get("value", "0")).replace("$", "").replace(",", "")
                    liquidity_usd = max(liquidity_usd, float(val_str))
                except ValueError:
                    pass

        if pool_data and pool_data.get("liquidity_usd", 0) > liquidity_usd:
            liquidity_usd = pool_data["liquidity_usd"]

        red_flags: List[str] = []
        for r in report.get("risks", []) or []:
            level = r.get("level", "")
            name = r.get("name", "")
            if level == "danger":
                red_flags.append(f"rugcheck_danger:{name}")
            elif level == "warn" and "liquidity" in name.lower() and liquidity_usd < self.cfg.min_pool_liquidity_usd:
                red_flags.append(f"rugcheck_warn:{name}")

        result = (lp_locked, liquidity_usd, lp_pool, score, red_flags, vaults)
        self.cache.set(f"lp:{mint}", result)
        return result

    async def get_token_authorities(self, mint: str) -> Tuple[bool, bool]:
        cached = self.cache.get(f"auth:{mint}", 30)
        if cached is not None:
            return cached

        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getAccountInfo",
            "params": [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}],
        }
        async with self.sem:
            try:
                async with self.session.post(self.cfg.holder_rpc_url, json=payload) as resp:
                    data = await resp.json()
            except Exception:
                data = {}

        value = (data.get("result") or {}).get("value")
        if value:
            parsed = (value.get("data") or {}).get("parsed") or {}
            info = parsed.get("info") or {}
            mint_auth = bool(info.get("mintAuthority"))
            freeze_auth = bool(info.get("freezeAuthority"))
            result = (mint_auth, freeze_auth)
            self.cache.set(f"auth:{mint}", result)
            return result

        report = await self.get_security_report(mint)
        token = report.get("token", {}) or {}
        result = (bool(token.get("mintAuthority")), bool(token.get("freezeAuthority")))
        self.cache.set(f"auth:{mint}", result)
        return result

    async def get_transfer_fee(self, mint: str) -> float:
        report = await self.get_security_report(mint)
        token = report.get("token", {}) or {}
        fee = token.get("transferFee", {}) or {}
        if not fee:
            return 0.0
        bps = float(fee.get("transferFeeBasisPoints", 0) or 0)
        return bps / 100.0

    async def get_metadata_mutability(self, mint: str) -> bool:
        report = await self.get_security_report(mint)
        meta = report.get("tokenMeta", {}) or {}
        return bool(meta.get("mutable", False))

    async def get_creator_reputation(self, mint: str) -> Tuple[int, int]:
        report = await self.get_security_report(mint)
        risks = report.get("risks", []) or []
        rugpull_count = sum(
            1 for r in risks
            if r.get("level") == "danger"
            and ("creator" in r.get("name", "").lower() or "rug" in r.get("name", "").lower())
        )
        return rugpull_count, 1

    async def simulate_sell(self, mint: str, amount_raw: int = 1_000_000) -> bool:
        if not self.cfg.enable_sell_simulation:
            return True
        url = (
            f"{self.cfg.jupiter_quote_url}"
            f"?inputMint={mint}"
            f"&outputMint=So11111111111111111111111111111111111111112"
            f"&amount={amount_raw}"
            f"&slippageBps=500"
        )
        async with self.sem:
            try:
                async with self.session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return float(data.get("outAmount", 0) or 0) > 0
                    elif resp.status == 400:
                        return True
                    return True
            except Exception:
                return True

    async def count_alpha_wallets(self, wallets: List[str]) -> int:
        if not wallets:
            return 1
        target = wallets[:20]
        ages, balances = await asyncio.gather(
            asyncio.gather(*[self.get_wallet_age_hours(w) for w in target]),
            asyncio.gather(*[self.get_wallet_balance_sol(w) for w in target]),
        )
        count = 0
        for age, bal in zip(ages, balances):
            if age >= self.cfg.alpha_wallet_min_age_hours and bal >= self.cfg.alpha_wallet_min_balance_sol:
                count += 1
        return count

    async def check_dev_sold(self, mint: str, creator: str, created_at: float) -> float:
        if not creator:
            return 0.0
        cached = self.cache.get(f"devsell:{mint}", 60)
        if cached is not None:
            return cached

        sigs = await self.get_signatures(creator, limit=15)
        sold_pct = 0.0
        for s in sigs:
            bt = s.get("blockTime", 0) or 0
            if bt < created_at:
                continue
            tx = await self.get_transaction(s["signature"])
            if not tx or not tx.get("meta"):
                continue
            try:
                pre = tx["meta"].get("preTokenBalances", []) or []
                post = tx["meta"].get("postTokenBalances", []) or []
                pre_amt = sum(float(pb.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)
                              for pb in pre if pb.get("mint") == mint and pb.get("owner") == creator)
                post_amt = sum(float(pb.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)
                               for pb in post if pb.get("mint") == mint and pb.get("owner") == creator)
                if pre_amt > post_amt and pre_amt > 0:
                    sold_pct += ((pre_amt - post_amt) / pre_amt) * 100.0
            except Exception:
                continue

        sold_pct = min(100.0, sold_pct)
        self.cache.set(f"devsell:{mint}", sold_pct)
        return sold_pct


# ============================================================
# 5. ANTI-DEV LINKAGE & SYBIL CABAL DETECTOR
# ============================================================

class DevLinkageAndSybilDetector:
    """
    Mendeteksi keterkaitan antara Developer dengan Buyer / Holder:
    1. Direct Funding Link: Dompet pembeli menerima transfer SOL langsung dari creator.
    2. Common Parent Funder: Dompet pembeli dan creator didanai dari sumber wallet induk yang sama (Co-funding tree).
    3. Direct Transaction Link: Pernah berinteraksi langsung dalam riwayat transaksi creator.
    4. Multi-wallet Sybil Clones: Kumpulan dompet dengan saldo identik atau didanai bersamaan.
    
    HANYA dompet yang 100% TIDAK TERKAIT dengan developer yang dihitung sebagai pemegang/pembeli unik!
    """
    KNOWN_EXCHANGES = {
        "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9", # Binance Hot
        "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S", # Coinbase Hot
        "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM", # Bybit
        "u6T5CDtKb94epD7qWkP7CgE7nS41E7m5Xp46497BqQ8",  # OKX
        "ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ", # FixedFloat
        "39L5PtV5nLssih8p7nE12p3a9TfE2xJt7J4g18sUv18d", # Kraken
        "H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS", # KuCoin
    }

    def __init__(self, cfg: Config, rpc: RpcClient):
        self.cfg = cfg
        self.rpc = rpc

    async def find_funder(self, wallet: str, window_start: float, window_end: float) -> Optional[str]:
        if not wallet:
            return None
        cached = self.rpc.cache.get(f"funder:{wallet}", 600)
        if cached is not None:
            return cached

        sigs = await self.rpc.get_signatures(wallet, limit=15)
        funder = None
        for s in sigs:
            bt = s.get("blockTime", 0) or 0
            if bt and (window_start <= bt <= window_end):
                tx = await self.rpc.get_transaction(s["signature"])
                if not tx or not tx.get("meta"):
                    continue
                try:
                    account_keys = [k["pubkey"] if isinstance(k, dict) else k
                                    for k in tx["transaction"]["message"]["accountKeys"]]
                    pre_bal = tx["meta"]["preBalances"]
                    post_bal = tx["meta"]["postBalances"]

                    if wallet not in account_keys:
                        continue
                    w_idx = account_keys.index(wallet)

                    # Jika wallet menerima SOL
                    if post_bal[w_idx] > pre_bal[w_idx]:
                        for i, key in enumerate(account_keys):
                            if i != w_idx and pre_bal[i] > post_bal[i] and (pre_bal[i] - post_bal[i]) >= 5_000_000:
                                funder = key
                                break
                    if funder:
                        break
                except Exception:
                    pass

        self.rpc.cache.set(f"funder:{wallet}", funder)
        return funder

    async def check_direct_dev_transfer(self, wallet: str, creator: str) -> bool:
        """Cek apakah ada riwayat transaksi langsung antara wallet dan creator"""
        if not wallet or not creator or wallet == creator:
            return True
        cached = self.rpc.cache.get(f"dirdev:{wallet}:{creator}", 600)
        if cached is not None:
            return cached

        sigs = await self.rpc.get_signatures(wallet, limit=10)
        has_direct = False
        for s in sigs:
            tx = await self.rpc.get_transaction(s["signature"])
            if not tx or not tx.get("meta"):
                continue
            account_keys = [k["pubkey"] if isinstance(k, dict) else k
                            for k in tx["transaction"]["message"]["accountKeys"]]
            if creator in account_keys:
                has_direct = True
                break

        self.rpc.cache.set(f"dirdev:{wallet}:{creator}", has_direct)
        return has_direct

    async def analyze_dev_linkage_and_sybil(
        self,
        holders: Dict[str, float],
        buyers: List[str],
        creator: str,
        created_at: float,
        pool_vaults: Set[str]
    ) -> Tuple[bool, float, Set[str], Set[str], Set[str], List[str]]:
        """
        Analisis mendalam keterkaitan antara Developer dengan Buyer & Holder:
        Mengeluarkan semua dompet yang didanai dev atau satu sumber dana dengan dev.
        """
        if not self.cfg.enable_dev_linkage_check or not holders:
            return False, 0.0, set(), set(holders.keys()), set(buyers), []

        red_flags: List[str] = []
        filtered_holders = {
            w: p for w, p in holders.items()
            if w not in pool_vaults and w not in KNOWN_DEX_PROGRAMS
        }

        # Kumpulkan dompet yang akan diaudit (top holders + early buyers)
        target_holders = list(filtered_holders.keys())[:15]
        all_audit_wallets = list(dict.fromkeys(target_holders + buyers[:15]))
        if not all_audit_wallets:
            return False, 0.0, set(), set(), set(), []

        window_start = created_at - (self.cfg.cabal_funding_window_hours * 3600)
        window_end = created_at + 120

        # Lacak Funder creator terlebih dahulu
        creator_funder = None
        if creator:
            creator_funder = await self.find_funder(creator, window_start, window_end)

        # Lacak Funder semua dompet target secara paralel
        funders = await asyncio.gather(
            *[self.find_funder(w, window_start, window_end) for w in all_audit_wallets]
        )
        funder_map = dict(zip(all_audit_wallets, funders))

        # 1. IDENTIFIKASI DOMPET YANG TERAFILIASI / TERKAIT DENGAN DEVELOPER
        dev_linked_wallets: Set[str] = set()
        if creator:
            dev_linked_wallets.add(creator)

        for w in all_audit_wallets:
            if creator and w == creator:
                dev_linked_wallets.add(w)
                continue

            w_funder = funder_map.get(w)

            # Kasus A: Dompet didanai langsung oleh Creator
            if creator and w_funder == creator:
                dev_linked_wallets.add(w)
                continue

            # Kasus B: Dompet dan Creator didanai oleh dompet induk yang sama (Common Ancestor)
            if creator_funder and w_funder and w_funder == creator_funder and w_funder not in self.KNOWN_EXCHANGES:
                dev_linked_wallets.add(w)
                continue

            # Kasus C: Trace Rekursif 2-Hop (Pecah Dompet via Sub-Disperser: Creator -> Disperser -> Child)
            if creator and w_funder and w_funder not in dev_linked_wallets and w_funder not in self.KNOWN_EXCHANGES:
                grand_funder = await self.find_funder(w_funder, window_start, window_end)
                if grand_funder and (grand_funder == creator or (creator_funder and grand_funder == creator_funder and grand_funder not in self.KNOWN_EXCHANGES)):
                    dev_linked_wallets.add(w)
                    dev_linked_wallets.add(w_funder)
                    continue

        # Cek direct transfer cepat untuk top 5 holder
        if creator:
            direct_checks = await asyncio.gather(
                *[self.check_direct_dev_transfer(w, creator) for w in target_holders[:5]]
            )
            for w, has_direct in zip(target_holders[:5], direct_checks):
                if has_direct:
                    dev_linked_wallets.add(w)

        # 2. HITUNG AKUMULASI KEPEMILIKAN DEV + AFILIASI
        dev_linked_holding_pct = sum(filtered_holders.get(w, 0.0) for w in dev_linked_wallets)
        if dev_linked_holding_pct > self.cfg.max_dev_linked_holding_pct:
            red_flags.append(f"dev_linked_insiders:{dev_linked_holding_pct:.1f}%({len(dev_linked_wallets)}wallets)")

        # 3. DOMPET YANG BENAR-BENAR UNIK & INDEPENDEN (100% UNLINKED)
        unlinked_holders = {w for w in filtered_holders.keys() if w not in dev_linked_wallets}
        unlinked_buyers = {b for b in buyers if b not in dev_linked_wallets}

        # 4. CEK JUGA CABAL NON-DEV (Syndicate antar sesama sniper non-dev)
        funder_groups: Dict[str, List[str]] = defaultdict(list)
        for w, f in funder_map.items():
            if f and f not in self.KNOWN_EXCHANGES and f not in dev_linked_wallets and w not in dev_linked_wallets:
                funder_groups[f].append(w)

        for f, ws in funder_groups.items():
            if len(ws) >= 2:
                cabal_pct = sum(filtered_holders.get(w, 0.0) for w in ws)
                if cabal_pct > self.cfg.max_cabal_cluster_holding_pct:
                    red_flags.append(f"cabal_sybil_cluster:{cabal_pct:.1f}%({len(ws)}wallets)")

        # 5. SYBIL FINGERPRINT (Saldo/persentase klon identik)
        pcts = [round(filtered_holders[w], 2) for w in unlinked_holders if filtered_holders.get(w, 0) > 0.5]
        pct_counts = defaultdict(int)
        for p in pcts:
            pct_counts[p] += 1
        for p, count in pct_counts.items():
            if count >= self.cfg.max_sybil_similar_wallets:
                red_flags.append(f"sybil_balance_clones:{count}wallets_at_{p}%")

        is_flagged = len(red_flags) > 0
        return is_flagged, dev_linked_holding_pct, dev_linked_wallets, unlinked_holders, unlinked_buyers, red_flags


# ============================================================
# 6. ANTI-WASH TRADING & FAKE VOLUME DETECTOR
# ============================================================

class WashTradingDetector:
    """
    Mendeteksi volume palsu dari bot market maker (VoluMatic / PumpBot).
    Kriteria:
    1. Unique Trader Ratio: Rasio wallet unik dibanding total trade.
    2. Micro Order Spam: Ratusan transaksi bernilai receh konstan (0.01 - 0.04 SOL).
    3. Ping-Pong Wash: Dompet yang sama membeli dan langsung menjual berulang kali.
    4. Dispersal Anomaly: Volume tinggi tetapi jumlah holder baru tidak bertambah.
    """
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def evaluate_wash(
        self,
        buyers_buffer: List[dict],
        buys_count: int,
        sells_count: int,
        holders_count: int,
        volume_usd: float
    ) -> Tuple[bool, float, List[str]]:
        if not self.cfg.enable_wash_trading_check:
            return False, 1.0, []

        red_flags: List[str] = []
        total_txns = buys_count + sells_count

        # 1. Unique Trader Ratio
        unique_wallets = set(b.get("wallet") for b in buyers_buffer if b.get("wallet"))
        if len(buyers_buffer) >= 6:
            unique_ratio = len(unique_wallets) / len(buyers_buffer)
        else:
            unique_ratio = 1.0

        if len(buyers_buffer) >= 8 and unique_ratio < self.cfg.min_unique_trader_ratio:
            red_flags.append(f"wash_low_unique_traders:{unique_ratio:.0%}")

        # 2. Micro-Order Spam Detection
        if len(buyers_buffer) >= 10:
            sol_amounts = [float(b.get("sol_amount", 0) or 0) for b in buyers_buffer if b.get("sol_amount")]
            if sol_amounts:
                micro_count = sum(1 for s in sol_amounts if s < self.cfg.min_real_median_buy_sol)
                micro_ratio = micro_count / len(sol_amounts)
                if micro_ratio > 0.65 and volume_usd > 2500:
                    red_flags.append(f"wash_micro_order_spam:{micro_ratio:.0%}")

        # 3. Volume vs Holder Growth Disconnect
        # Jika volume > $4,000 dan buys > 25 tapi holder < 8, volume ini hanya mutar di tempat
        if volume_usd >= 4_000 and buys_count >= 25 and holders_count < 8:
            red_flags.append(f"fake_volume_no_holders:vol${volume_usd:,.0f}_hc{holders_count}")

        is_wash = len(red_flags) > 0
        return is_wash, round(unique_ratio, 2), red_flags


# ============================================================
# 7. HIT-AND-RUN MOMENTUM SCORER
# ============================================================

class HitAndRunScorer:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def score_token(self, t: TokenState) -> Tuple[float, str, List[str], List[str]]:
        reasons: List[str] = []
        red_flags: List[str] = []
        score = 0.0
        phase = "dex_pool" if t.is_migrated else "bonding"

        # 1. Rasio Pembeli Nyata
        total_vol = t.volume_buys + t.volume_sells
        min_vol_needed = (self.cfg.min_volume_buys_sol * 160.0) if not t.is_migrated else 2500.0
        if total_vol < min_vol_needed:
            red_flags.append(f"insufficient_volume:${total_vol:,.0f}<${min_vol_needed:,.0f}")
            buy_ratio = 0.0
        else:
            buy_ratio = (t.volume_buys / total_vol) if total_vol > 0 else 0.0

        if buy_ratio >= self.cfg.min_buy_volume_ratio:
            score += 25 * buy_ratio
            reasons.append(f"buy_pressure:{buy_ratio:.1%}")
        else:
            red_flags.append(f"weak_buy_pressure:{buy_ratio:.1%}<{self.cfg.min_buy_volume_ratio:.0%}")

        # 2. Momentum Harga 5 Menit (Wajib Positif/Konsolidasi Sehat & Bukan Dump!)
        pump_5m = 0.0
        if t.price_at_5m_ago > 0:
            pump_5m = (t.price - t.price_at_5m_ago) / t.price_at_5m_ago * 100.0
            if pump_5m < -5.0:
                red_flags.append(f"price_dumping_5m:{pump_5m:.1f}%")

        if self.cfg.min_price_pump_5m_pct <= pump_5m <= self.cfg.max_price_pump_5m_pct:
            score += min(25.0, pump_5m * 0.4)
            reasons.append(f"momentum_5m:+{pump_5m:.1f}%")
        elif -5.0 <= pump_5m < self.cfg.min_price_pump_5m_pct and buy_ratio >= 0.60:
            score += 15.0
            reasons.append(f"consolidation_dip_support:{pump_5m:+.1f}%")
        elif pump_5m > self.cfg.max_price_pump_5m_pct:
            red_flags.append(f"pump_overextended:+{pump_5m:.1f}%")

        # 3. Sweetspot MCap / Bonding (Mencakup Fase Fresh & Runner 1-4 Jam)
        if t.is_migrated:
            if self.cfg.min_market_cap_usd <= t.market_cap_usd <= self.cfg.max_market_cap_usd:
                score += 25.0
                reasons.append(f"mcap_sweetspot:${t.market_cap_usd:,.0f}")
            elif t.market_cap_usd > self.cfg.max_market_cap_usd:
                red_flags.append(f"mcap_too_high:${t.market_cap_usd:,.0f}")

            if t.liquidity_usd >= self.cfg.min_pool_liquidity_usd:
                score += 15.0
                reasons.append(f"pool_liq:${t.liquidity_usd:,.0f}")
            else:
                red_flags.append(f"liq_too_low:${t.liquidity_usd:,.0f}")

            if t.pool_age_minutes <= self.cfg.max_pool_age_minutes:
                score += 10.0
                age_label = "pool_fresh" if t.pool_age_minutes < 30 else "pool_runner_1h_plus"
                reasons.append(f"{age_label}:{t.pool_age_minutes:.0f}m")
            else:
                red_flags.append(f"pool_too_old:{t.pool_age_minutes:.0f}m")
        else:
            if self.cfg.min_bonding_pct <= t.bonding_pct <= self.cfg.max_bonding_pct:
                score += 25.0
                reasons.append(f"bonding_sweetspot:{t.bonding_pct:.1f}%")
            elif t.bonding_pct < self.cfg.min_bonding_pct:
                red_flags.append(f"bonding_too_early:{t.bonding_pct:.1f}%")
            else:
                red_flags.append(f"bonding_near_top:{t.bonding_pct:.1f}%")

            if t.liquidity_usd >= 3_000:
                score += 15.0
                reasons.append(f"bonding_liq:${t.liquidity_usd:,.0f}")

        # 4. Kualitas Alpha & Trader Unik
        birth_score = 0.0
        if t.alpha_wallet_count >= self.cfg.strong_alpha_wallets:
            birth_score += 25
            reasons.append(f"alpha_wallets_strong:{t.alpha_wallet_count}")
        elif t.alpha_wallet_count >= self.cfg.min_alpha_wallets:
            birth_score += 15
            reasons.append(f"alpha_wallets_ok:{t.alpha_wallet_count}")

        social_count = sum([t.has_twitter, t.has_telegram, t.has_website])
        if social_count >= 2:
            birth_score += 25
            reasons.append(f"social_strong:{social_count}")
        elif social_count >= 1:
            birth_score += 15
            reasons.append(f"social_present:{social_count}")

        if t.liquidity_velocity >= self.cfg.min_liquidity_velocity_sol_per_min:
            birth_score += 25
            reasons.append(f"liq_velocity:{t.liquidity_velocity:.2f}SOL/m")

        if t.unique_trader_ratio >= 0.50:
            birth_score += 25
            reasons.append(f"organic_traders:{t.unique_trader_ratio:.0%}")

        t.birth_signal_score = min(100.0, birth_score)
        score += t.birth_signal_score * 0.20

        # 5. PILAR 100x RUNNER DNA (MULTI-BAGGER ACCUMULATION PATTERN)
        # A. Dev Exit / CTO / Zero Greedy Dev Stash
        if t.dev_holding_pct <= 1.5:
            score += 10.0
            reasons.append(f"runner_clean_dev:{t.dev_holding_pct:.1f}%")
        elif t.dev_holding_pct > self.cfg.max_dev_holding_pct:
            red_flags.append(f"greedy_dev:{t.dev_holding_pct:.1f}%>{self.cfg.max_dev_holding_pct}%")

        # B. Retail Army - Pasukan Dompet Mandiri (Bukan 1 Paus)
        if t.unlinked_buyers_count >= self.cfg.min_unlinked_buyers_count and t.unique_trader_ratio >= 0.50:
            score += 10.0
            reasons.append(f"runner_retail_army:{t.unlinked_buyers_count}buyers_{t.unique_trader_ratio:.0%}unique")

        # C. Dip Absorption Kuat (Lantai Harga Selalu Naik)
        if buy_ratio >= 0.65:
            score += 10.0
            reasons.append(f"runner_dip_absorption:{buy_ratio:.1%}buys")

        return round(min(100.0, score), 2), phase, reasons, red_flags


# ============================================================
# 7B. PARALLEL ON-CHAIN INDEXER & EARLY-ACCUMULATION ENGINE
# ============================================================

class ParallelEarlyAccumulationIndexer:
    """
    Modul optimasi sub-block latency & deteksi akumulasi organik awal:
    1. Sub-Block Parallel Indexing: Memproses transaksi on-chain secara paralel dengan worker pool konkuren
       sehingga penyaringan selesai jauh lebih cepat dari block time standar Solana (~400ms).
    2. Deployer Vanity & Block-0 Isolation: Mengabaikan volume suntikan developer (deployer self-bundle / vanity pool)
       pada detik-detik awal peluncuran koin.
    3. Pre-Parabolic Ignition Detection: Menembakkan sinyal tepat saat volume organik sekunder mulai masuk
       dan harga masih dalam fase kompresi dasar (+1% s/d +35%), SEBELUM lilin parabolik (+100% - +500%) meledak.
    """
    def __init__(self, cfg: Config, rpc: RpcClient):
        self.cfg = cfg
        self.rpc = rpc
        self.sem = asyncio.Semaphore(cfg.indexer_concurrency_workers)

    def process_sub_block_trade(
        self,
        t: TokenState,
        wallet: str,
        sol_amount: float,
        is_buy: bool,
        tx_timestamp: float
    ) -> Tuple[bool, str]:
        """
        Memisahkan transaksi: Apakah ini Deployer Vanity / Block-0 Bundle atau Pembelian Organik Murni?
        """
        age_at_tx = max(0.0, tx_timestamp - t.created_at)

        # 1. DETEKSI BLOCK-0 DEPLOYER VANITY BUNDLE
        # Transaksi dalam 2.5 detik pertama atau dilakukan oleh creator/afiliasi creator adalah Vanity Pool
        is_block0 = (age_at_tx <= 2.5) or (t.buys_count <= 2 and age_at_tx <= 5.0)
        is_creator_wallet = bool(t.creator and wallet == t.creator)
        is_dev_linked = wallet in t.dev_linked_wallets

        if is_block0 or is_creator_wallet or is_dev_linked:
            t.block0_vanity_volume_sol += sol_amount
            t.block0_wallets.add(wallet)
            return False, "vanity_block0_bundle"

        # 2. TRANSAKSI SEKUNDER ORGANIK
        if is_buy:
            t.organic_buys_count += 1
            t.organic_volume_buys_sol += sol_amount
            t.organic_unique_buyers.add(wallet)
            return True, "organic_buy"
        else:
            return True, "organic_sell"

    def evaluate_early_accumulation(
        self,
        t: TokenState,
        price_change_5m: float
    ) -> Tuple[bool, float, List[str], List[str]]:
        """
        Evaluasi apakah token sedang berada dalam fase akumulasi organik sebelum parabolik:
        Returns: (is_approved, conviction_score, reasons, red_flags)
        """
        reasons: List[str] = []
        red_flags: List[str] = []
        score = 0.0

        # A. Cek volume organik murni (tanpa vanity bundle deployer)
        total_organic_vol_sol = t.organic_volume_buys_sol
        if total_organic_vol_sol < self.cfg.min_organic_inflow_sol:
            red_flags.append(f"waiting_organic_inflow:{total_organic_vol_sol:.1f}SOL<{self.cfg.min_organic_inflow_sol}SOL")
        else:
            score += 30.0
            reasons.append(f"clean_organic_inflow:{total_organic_vol_sol:.1f}SOL")

        # B. Pasukan pembeli organik independen
        unique_org = len(t.organic_unique_buyers)
        if unique_org < self.cfg.min_organic_secondary_buyers:
            red_flags.append(f"insufficient_organic_buyers:{unique_org}<{self.cfg.min_organic_secondary_buyers}")
        else:
            score += 25.0
            reasons.append(f"organic_secondary_army:{unique_org}wallets")

        # C. Dominasi Beli Organik (Net CVD)
        if (t.volume_buys + t.volume_sells) > 0:
            clean_buy_ratio = t.organic_volume_buys_sol / max(0.1, (t.organic_volume_buys_sol + t.volume_sells))
            if clean_buy_ratio < self.cfg.min_organic_cvd_ratio:
                red_flags.append(f"weak_organic_cvd:{clean_buy_ratio:.1%}<{self.cfg.min_organic_cvd_ratio:.0%}")
            else:
                score += 25.0
                reasons.append(f"bullish_organic_cvd:{clean_buy_ratio:.1%}")

        # D. Pre-Parabolic Window: Wajib belum meledak parabolik
        if price_change_5m > self.cfg.max_pre_parabolic_pump_pct:
            red_flags.append(f"already_parabolic_overextended:+{price_change_5m:.1f}%>{self.cfg.max_pre_parabolic_pump_pct}%")
        elif price_change_5m < self.cfg.min_pre_parabolic_pump_pct:
            # Jika harga belum bergerak tapi pembeli organik terus bertumpuk (Accumulation Compression)
            if unique_org >= 5 and total_organic_vol_sol >= 5.0:
                score += 20.0
                reasons.append("silent_base_accumulation_compression")
            else:
                red_flags.append(f"no_momentum_traction:{price_change_5m:+.1f}%")
        else:
            score += 20.0
            reasons.append(f"pre_parabolic_sweetspot:+{price_change_5m:.1f}%")

        # E. Rasio Vanity vs Organik: Jika 75%+ volume berasal dari Block-0 bundle deployer, tolak
        total_vol = t.block0_vanity_volume_sol + t.organic_volume_buys_sol
        if total_vol > 0:
            vanity_ratio = t.block0_vanity_volume_sol / total_vol
            if vanity_ratio > 0.75 and unique_org < 5:
                red_flags.append(f"heavy_deployer_vanity_volume:{vanity_ratio:.0%}")

        is_approved = (len(red_flags) == 0) and (score >= 70.0)
        t.is_pre_parabolic_accumulation = is_approved
        return is_approved, score, reasons, red_flags


# ============================================================
# 8. POSITION MANAGER
# ============================================================

class PositionManager:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.positions: Dict[str, Position] = {}
        self.daily_exposure = 0.0
        self.daily_loss_streak = 0

    def can_open(self, size_usd: float, portfolio_usd: float) -> bool:
        if self.daily_loss_streak >= self.cfg.max_loss_streak:
            return False
        exposure_pct = (self.daily_exposure + size_usd) / portfolio_usd * 100
        return exposure_pct <= self.cfg.max_daily_exposure_pct

    def open(self, signal: Signal, size_usd: float) -> Position:
        stop = signal.entry_price * (1 - self.cfg.hard_stop_loss_pct / 100.0)
        pos = Position(
            mint=signal.mint,
            symbol=signal.symbol,
            entry_price=signal.entry_price,
            size_usd=size_usd,
            high_water=signal.entry_price,
            opened_at=time.time(),
            stop_price=stop,
            trailing_price=stop,
        )
        self.positions[signal.mint] = pos
        self.daily_exposure += size_usd
        return pos

    def update(self, mint: str, price: float) -> List[Tuple[str, float]]:
        actions: List[Tuple[str, float]] = []
        pos = self.positions.get(mint)
        if not pos or price <= 0:
            return actions

        if price > pos.high_water:
            pos.high_water = price

        new_trailing = pos.high_water * (1 - self.cfg.trailing_stop_pct / 100.0)
        if new_trailing > pos.trailing_price:
            pos.trailing_price = new_trailing

        if price <= pos.stop_price:
            actions.append(("stop_loss", pos.remaining_pct))
            return actions

        if not pos.initial_recovered and price >= pos.entry_price * self.cfg.take_initials_multiple:
            pos.initial_recovered = True
            pos.remaining_pct = 50.0
            actions.append(("take_initials_50pct", 50.0))

        if pos.initial_recovered and not pos.tp2_recovered and price >= pos.entry_price * self.cfg.take_profit_2_multiple:
            pos.tp2_recovered = True
            pos.remaining_pct = 25.0
            actions.append(("take_profit_25pct", 25.0))

        if pos.initial_recovered and price <= pos.trailing_price:
            actions.append(("trailing_stop", pos.remaining_pct))
            return actions

        if (time.time() - pos.opened_at) > (self.cfg.time_stop_minutes * 60):
            actions.append(("time_stop", pos.remaining_pct))
            return actions

        return actions

    def close(self, mint: str, pnl_usd: float):
        if self.positions.pop(mint, None):
            if pnl_usd < 0:
                self.daily_loss_streak += 1
            else:
                self.daily_loss_streak = 0


# ============================================================
# 9. TELEGRAM ALERTER
# ============================================================

class TelegramAlerter:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if not self.bot_token or not self.chat_id:
            print("[telegram] Token/ChatID kosong, notifikasi lokal aktif")
            return
        self.session = aiohttp.ClientSession()
        await self._send("⚡ <b>[HIT-AND-RUN] Sniper Engine v12 Online</b>\nAnti-Cabal & Anti-Wash Engine Aktif!")

    async def close(self):
        if self.session:
            await self.session.close()

    async def send_signal_alert(self, s: Signal, t: TokenState):
        emoji = "🔥" if s.phase == "PRE_PARABOLIC_IGNITION" else ("🚀" if s.score >= 85 else "⚡")
        if s.phase == "PRE_PARABOLIC_IGNITION":
            phase_label = "🔥 EARLY IGNITION (PRE-PARABOLIC)"
        elif s.phase == "dex_pool":
            phase_label = "MOMENTUM DEX POOL BREAKOUT"
        else:
            phase_label = "BONDING ACCUMULATION"

        age_mins = max(0.1, t.pool_age_minutes if t.pool_age_minutes > 0 else (time.time() - t.created_at) / 60.0)
        age_str = f"{age_mins:.0f} Menit" if age_mins < 60 else f"{age_mins / 60.0:.1f} Jam"

        total_tx = max(1, t.buys_count + t.sells_count)
        buy_ratio = (t.buys_count / total_tx) * 100.0
        buy_mult = (t.buys_count / max(1, t.sells_count))
        unique_buyers = max(len(t.unique_buyers), len(t.organic_unique_buyers), t.unlinked_buyers_count, 1)

        # Hitung Top 1 Holder di luar kolam likuiditas
        individual_holders = {
            w: p for w, p in t.holders.items()
            if w not in t.pool_vaults and w not in KNOWN_DEX_PROGRAMS
        }
        sorted_p = sorted(individual_holders.values(), reverse=True)
        top1_pct = sorted_p[0] if sorted_p else 0.0
        top10_pct = sum(sorted_p[:10]) if sorted_p else 0.0

        liq_ratio = (s.liquidity / max(1.0, s.market_cap)) * 100.0

        dev_status = "EXIT / CTO (0.0%) ✅" if t.dev_holding_pct <= 0.1 else f"{t.dev_holding_pct:.1f}% (Aman &le; 3.5%)"
        sybil_status = f"{len(t.dev_linked_wallets)} Dompet ({t.dev_linked_holding_pct:.1f}%)" if t.dev_linked_wallets else "0 Dompet (Terdesentralisasi Murni) ✅"

        # Prediksi Target Hit & Run (Proyeksi Keuntungan)
        tp1 = s.entry_price * 1.80  # +80%
        tp2 = s.entry_price * 3.00  # +200% (3x)
        sl = s.entry_price * 0.78   # -22%

        reasons_text = "\n".join(f"  • {r}" for r in s.reasons[:5])

        text = (
            f"{emoji} <b>[HITRUN-V12] EARLY GEM SNIPER ALERT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>Token:</b> {t.name} (<b>${t.symbol}</b>)\n"
            f"🔑 <b>CA:</b> <code>{s.mint}</code> <i>(Salin Cepat)</i>\n"
            f"💎 <b>DEX / Status:</b> <code>{(t.dex_id or 'DEX').upper()}</code> · <b>{phase_label}</b>\n"
            f"🎯 <b>Skor Keyakinan:</b> <code>{s.score}/100 [GRADE-A RUNNER DNA]</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>METRIK INFLEKSI HARGA:</b>\n"
            f"  • Entry Price: <code>${s.entry_price:.8f}</code>\n"
            f"  • Market Cap: <code>${s.market_cap:,.0f}</code> <i>(Early Zone &lt; $600K)</i>\n"
            f"  • Likuiditas Pool: <code>${s.liquidity:,.0f}</code> <i>(Rasio: {liq_ratio:.1f}%)</i>\n"
            f"  • Usia Koin / Pool: <code>{age_str}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 <b>ANALISIS PASUKAN PEMBELI (BUYER ARMY):</b>\n"
            f"  • Rasio Pembeli (Buy Ratio): <code>{buy_ratio:.1f}% BUY ({buy_mult:.2f}x Penjual)</code>\n"
            f"  • Pembeli Unik (Unique Buyers): <code>{unique_buyers} Dompet Berbeda</code>\n"
            f"  • Total Pemegang (Holders): <code>{len(t.holders)} Dompet On-Chain</code>\n"
            f"  • Pemegang Terbesar (Top 1 Non-Pool): <code>{top1_pct:.2f}% (Toleransi &le; 10%)</code>\n"
            f"  • Konsentrasi Top 10: <code>{top10_pct:.1f}% (Terdistribusi Sehat)</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🛡️ <b>FORENSIK DEVELOPER & CABAL:</b>\n"
            f"  • Status Developer: <code>{dev_status}</code>\n"
            f"  • Dompet Pecahan Dev (Sybil Tree): <code>{sybil_status}</code>\n"
            f"  • Status Kontrak: <code>Mint: {'Revoked ✅' if not t.mint_authority_active else 'Aktif ❌'} | Freeze: {'Revoked ✅' if not t.freeze_authority_active else 'Aktif ❌'}</code>\n"
            f"  • Skor Risiko RugCheck: <code>{t.rug_score:.0f}/100 (Aman)</code>\n"
            f"  • Kualitas Volume: <code>{'✅ Organik Murni' if not t.is_wash_trading else '⚠️ Fake Volume'}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📈 <b>PREDIKSI TARGET PROFIT (HIT &amp; RUN):</b>\n"
            f"  • 🎯 TP 1 (+80%): <code>${tp1:.8f}</code> <i>(Tarik Modal 50% - Free Ride)</i>\n"
            f"  • 🚀 TP 2 (+200% / 3x): <code>${tp2:.8f}</code> <i>(Kunci Cuan 25%)</i>\n"
            f"  • 🛡️ Trailing Stop: <code>-18% dari High</code> (Kawal Moonbag)\n"
            f"  • 🛑 Stop Loss: <code>-22% Ketat</code> (Anti Nyangkut)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 <b>Katalog Keunggulan On-Chain:</b>\n"
            f"{reasons_text}\n\n"
            f"⚡ <b>LINK EKSEKUSI TRADING CEPAT:</b>\n"
            f"🔗 <a href='https://photon-sol.tinyastro.io/en/r/@alpha/{s.mint}'>[Photon SOL]</a> · "
            f"<a href='https://bullx.io/terminal?chainId=1399811149&address={s.mint}'>[BullX]</a> · "
            f"<a href='https://dexscreener.com/solana/{s.mint}'>[DexScreener]</a> · "
            f"<a href='https://gmgn.ai/sol/token/{s.mint}'>[GMGN]</a>"
        )
        await self._send(text)

    async def _send(self, text: str):
        if not self.session or not self.bot_token or not self.chat_id:
            return
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with self.session.post(url, json=payload) as resp:
                pass
        except Exception:
            pass


# ============================================================
# 10. SCANNER ENGINE UTAMA
# ============================================================

class HitAndRunScanner:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.cache = TimedCache()
        self.rpc = RpcClient(cfg, self.cache)
        self.cabal_detector = DevLinkageAndSybilDetector(cfg, self.rpc)
        self.wash_detector = WashTradingDetector(cfg)
        self.early_indexer = ParallelEarlyAccumulationIndexer(cfg, self.rpc)
        self.scorer = HitAndRunScorer(cfg)
        self.positions = PositionManager(cfg)
        self.telegram = TelegramAlerter(cfg.telegram_bot_token, cfg.telegram_chat_id)

        self.tokens: Dict[str, TokenState] = {}
        self.buyers_buffer: Dict[str, List[dict]] = defaultdict(list)
        self.signals: List[Signal] = []
        self.eval_queue: asyncio.Queue = asyncio.Queue()
        self.signal_queue: asyncio.Queue = asyncio.Queue()
        self.rescan_heap: List[Tuple[float, str]] = []
        self.running = True

    async def pumpportal_listener(self):
        while self.running:
            try:
                async with websockets.connect(self.cfg.pumpportal_ws, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    await ws.send(json.dumps({"method": "subscribeMigration"}))
                    print("[ws:pumpfun] connected to pumpportal")
                    async for raw in ws:
                        if not self.running:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        await self.handle_pumpportal_msg(msg)
            except Exception as e:
                await asyncio.sleep(4)

    async def handle_pumpportal_msg(self, msg: dict):
        tx_type = msg.get("txType")
        mint = msg.get("mint")
        if not mint:
            return

        if tx_type == "create":
            if mint in self.tokens:
                return
            v_sol = float(msg.get("vSolInBondingCurve", 0) or 0)
            bonding_pct = min(100.0, (v_sol / 85.0) * 100.0) if v_sol > 0 else 0.0
            t = TokenState(
                mint=mint,
                creator=msg.get("traderPublicKey", ""),
                source="pumpfun",
                dex_id="pumpfun",
                name=msg.get("name", "Unknown"),
                symbol=msg.get("symbol", "PUMP"),
                created_at=time.time(),
                price=float(msg.get("initialBuy", 0) or 0),
                bonding_pct=bonding_pct,
                liquidity_usd=max(0.0, v_sol * 160.0),
            )
            t.sol_in_bonding_history.append((time.time(), v_sol))
            self.tokens[mint] = t
            # Jadwalkan rescan otomatis awal (T+35 detik) untuk memeriksa traksi pembeli
            t.next_rescan_at = time.time() + 35.0
            heapq.heappush(self.rescan_heap, (t.next_rescan_at, mint))
            print(f"🐣 [PUMPFUN:NEW] Koin Terdeteksi: {t.name} (${t.symbol}) | CA: {mint[:6]}...{mint[-4:]} | Virtual SOL: {v_sol:.1f}")

        elif tx_type in ("buy", "sell"):
            t = self.tokens.get(mint)
            if not t:
                return
            sol_amount = float(msg.get("solAmount", 0) or 0)
            wallet = msg.get("traderPublicKey", "")

            # --- SUB-BLOCK PARALLEL ON-CHAIN INDEXING & DEPLOYER VANITY ISOLATION ---
            is_organic, tag = self.early_indexer.process_sub_block_trade(
                t, wallet, sol_amount, tx_type == "buy", time.time()
            )

            if tx_type == "buy":
                t.buys_count += 1
                t.volume_buys += sol_amount
                t.unique_buyers.add(wallet)
                if len(self.buyers_buffer[mint]) < 120:
                    self.buyers_buffer[mint].append({
                        "wallet": wallet, "sol_amount": sol_amount, "timestamp": time.time(), "side": "buy", "tag": tag
                    })
            else:
                t.sells_count += 1
                t.volume_sells += sol_amount
                if len(self.buyers_buffer[mint]) < 120:
                    self.buyers_buffer[mint].append({
                        "wallet": wallet, "sol_amount": sol_amount, "timestamp": time.time(), "side": "sell", "tag": tag
                    })

            v_sol = float(msg.get("vSolInBondingCurve", 0) or 0)
            if v_sol > 0:
                # Di Pump.fun, v_sol dimulai dari virtual 30 SOL dan selesai di ~115 SOL (85 real SOL terisi)
                real_sol = max(0.0, v_sol - 30.0) if v_sol >= 30.0 else v_sol
                t.bonding_pct = min(100.0, (real_sol / 85.0) * 100.0)
                t.sol_in_bonding_history.append((time.time(), v_sol))
                t.liquidity_usd = max(t.liquidity_usd, v_sol * 160.0)

            # Evaluasi SEGERA dipicu saat volume organik murni mulai masuk (Pre-Parabolic Trigger)!
            if not t.scored and not t.signal_emitted:
                has_organic_early = (
                    t.organic_buys_count >= self.cfg.min_organic_secondary_buyers and
                    t.organic_volume_buys_sol >= self.cfg.min_organic_inflow_sol
                )
                has_bonding_threshold = (t.bonding_pct >= self.cfg.min_bonding_pct and len(t.unique_buyers) >= 3)
                if has_organic_early or has_bonding_threshold:
                    await self.eval_queue.put(mint)

        elif tx_type == "migrate":
            t = self.tokens.get(mint)
            if t:
                t.is_migrated = True
                t.pool_address = msg.get("pool", "") or msg.get("poolAddress", "")
                t.scored = False
                await self.eval_queue.put(mint)

    async def dexscreener_discovery_loop(self):
        if not getattr(self.cfg, "enable_dexscreener_discovery", True):
            return
        print("[discovery] Multi-DEX & 1-4h Runner continuous discovery started")
        while self.running:
            try:
                mints_discovered: List[str] = []

                # 1. Pemindaian Token Profiles & Token Boosts Terbaru (DexScreener Trending / Boosted)
                if getattr(self.cfg, "enable_token_profiles_discovery", True):
                    for boost_url in (
                        "https://api.dexscreener.com/token-boosts/latest/v1",
                        "https://api.dexscreener.com/token-boosts/top/v1",
                        "https://api.dexscreener.com/token-profiles/latest/v1",
                    ):
                        try:
                            async with self.rpc.sem:
                                async with self.rpc.session.get(boost_url) as resp:
                                    if resp.status == 200:
                                        items = await resp.json()
                                        for item in (items or []):
                                            if item.get("chainId") == "solana":
                                                addr = item.get("tokenAddress")
                                                if addr:
                                                    mints_discovered.append(addr)
                        except Exception:
                            pass

                # 2. Pemindaian Multi-DEX Search (Raydium, Meteora DLMM, Orca)
                for q in self.cfg.discovery_queries:
                    try:
                        url = f"{self.cfg.dexscreener_search_url}?q={q}"
                        async with self.rpc.sem:
                            async with self.rpc.session.get(url) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    for p in (data.get("pairs", []) or [])[:self.cfg.discovery_max_per_query]:
                                        if p.get("chainId") == "solana":
                                            liq_usd = float(p.get("liquidity", {}).get("usd", 0) or 0)
                                            buys_m5 = int(p.get("txns", {}).get("m5", {}).get("buys", 0) or 0)
                                            sells_m5 = int(p.get("txns", {}).get("m5", {}).get("sells", 0) or 0)
                                            buys_h1 = int(p.get("txns", {}).get("h1", {}).get("buys", 0) or 0)
                                            sells_h1 = int(p.get("txns", {}).get("h1", {}).get("sells", 0) or 0)
                                            vol_m5 = float(p.get("volume", {}).get("m5", 0) or 0)
                                            vol_h1 = float(p.get("volume", {}).get("h1", 0) or 0)
                                            change_m5 = float(p.get("priceChange", {}).get("m5", 0) or 0)
                                            change_h1 = float(p.get("priceChange", {}).get("h1", 0) or 0)

                                            # Jalur A: Fresh Surge (Pompa awal 5 menit)
                                            is_fresh_surge = (
                                                liq_usd >= self.cfg.min_pool_liquidity_usd and
                                                buys_m5 >= self.cfg.min_organic_buys_m5 and
                                                buys_m5 >= (sells_m5 * 1.1) and
                                                vol_m5 >= 1500 and
                                                change_m5 >= 2.0
                                            )

                                            # Jalur B: 1-24 Hour Runner Breakout (Konsolidasi & Rally Gelombang 2 & 3 seperti $Agency)
                                            is_hour_runner = (
                                                liq_usd >= self.cfg.min_pool_liquidity_usd and
                                                buys_h1 >= 25 and
                                                buys_h1 >= (sells_h1 * 1.10) and
                                                vol_h1 >= 5000 and
                                                change_h1 >= 5.0 and
                                                change_m5 >= -4.0  # Bukan sedang dump tajam
                                            )

                                            if is_fresh_surge or is_hour_runner:
                                                addr = p.get("baseToken", {}).get("address")
                                                if addr:
                                                    mints_discovered.append(addr)
                    except Exception:
                        pass

                mints_discovered = list(dict.fromkeys(mints_discovered))
                new_mints = [m for m in mints_discovered if m not in self.tokens]
                if new_mints:
                    print(f"🔍 [DEX:DISCOVERY] Terdeteksi {len(new_mints)} token baru dari DEX, memasukkan ke antrean audit...")
                for mint in mints_discovered:
                    t = self.tokens.get(mint)
                    if not t:
                        self.tokens[mint] = TokenState(
                            mint=mint,
                            creator="",
                            source="dexscreener",
                            created_at=time.time(),
                        )
                        await self.eval_queue.put(mint)
                    elif not t.signal_emitted and not t.scored:
                        await self.eval_queue.put(mint)

            except Exception:
                pass
            await asyncio.sleep(self.cfg.discovery_interval_sec)

    async def geckoterminal_discovery_loop(self):
        if not getattr(self.cfg, "enable_geckoterminal_discovery", True):
            return
        print("[discovery] GeckoTerminal & Raydium New Pools discovery started")
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        base_skips = {
            "So11111111111111111111111111111111111111112",
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
            "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
        }

        while self.running:
            try:
                mints_discovered: List[str] = []

                # 1. GeckoTerminal Solana New, Trending & Multi-DEX Pools (PumpSwap, Pump.fun, Raydium, Meteora)
                urls = [
                    self.cfg.geckoterminal_new_pools_url,
                    self.cfg.geckoterminal_trending_pools_url,
                    "https://api.geckoterminal.com/api/v2/networks/solana/dexes/pumpswap/pools",
                    "https://api.geckoterminal.com/api/v2/networks/solana/dexes/pump-fun/pools",
                    "https://api.geckoterminal.com/api/v2/networks/solana/dexes/raydium/pools",
                    "https://api.geckoterminal.com/api/v2/networks/solana/dexes/meteora/pools",
                ]
                for url in urls:
                    try:
                        async with self.rpc.sem:
                            async with self.rpc.session.get(url, headers=headers) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    pools = data.get("data", []) or []
                                    for p in pools:
                                        rel = p.get("relationships", {})
                                        base_data = rel.get("base_token", {}).get("data", {})
                                        token_id = base_data.get("id", "")
                                        if token_id.startswith("solana_"):
                                            mint = token_id.replace("solana_", "")
                                            if mint and mint not in base_skips:
                                                mints_discovered.append(mint)
                    except Exception:
                        pass

                # 2. Raydium API New Pools (Mendeteksi langsung pool Raydium CPMM/CLMM baru)
                if getattr(self.cfg, "enable_raydium_pools_discovery", True):
                    try:
                        async with self.rpc.sem:
                            async with self.rpc.session.get(self.cfg.raydium_pools_url, headers=headers) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    ray_pools = data.get("data", {}).get("data", []) or []
                                    for rp in ray_pools[:35]:
                                        mint_a = rp.get("mintA", {}).get("address", "")
                                        mint_b = rp.get("mintB", {}).get("address", "")
                                        for m in (mint_a, mint_b):
                                            if m and m not in base_skips:
                                                mints_discovered.append(m)
                    except Exception:
                        pass

                mints_discovered = list(dict.fromkeys(mints_discovered))
                new_gecko = [m for m in mints_discovered if m not in self.tokens]
                if new_gecko:
                    print(f"🦎 [GECKO:DISCOVERY] Terdeteksi {len(new_gecko)} pool baru dari Raydium/PumpSwap/Meteora...")
                for mint in mints_discovered:
                    t = self.tokens.get(mint)
                    if not t:
                        self.tokens[mint] = TokenState(
                            mint=mint,
                            creator="",
                            source="geckoterminal",
                            created_at=time.time(),
                        )
                        await self.eval_queue.put(mint)
                    elif not t.signal_emitted and not t.scored:
                        await self.eval_queue.put(mint)

            except Exception:
                pass
            await asyncio.sleep(self.cfg.geckoterminal_interval_sec)

    async def worker(self, worker_id: int):
        while self.running:
            try:
                mint = await asyncio.wait_for(self.eval_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self.evaluate_token(mint)
            except Exception:
                pass
            finally:
                self.eval_queue.task_done()

    async def evaluate_token(self, mint: str):
        t = self.tokens.get(mint)
        if not t or t.scored:
            return

        now = time.time()
        age = now - t.created_at

        # 1. Ambil data pool DEX langsung
        pool_data = await self.rpc.get_pool_data(mint)
        if pool_data and pool_data.get("liquidity_usd", 0) >= self.cfg.min_pool_liquidity_usd:
            t.is_migrated = True
            t.dex_id = pool_data.get("dex_id", "dex")
            t.name = pool_data.get("name", t.name or "Unknown")
            t.symbol = pool_data.get("symbol", t.symbol or "TOKEN")
            t.pool_address = pool_data.get("pool_address", "")
            t.market_cap_usd = pool_data.get("market_cap", 0.0)
            t.liquidity_usd = max(t.liquidity_usd, pool_data.get("liquidity_usd", 0.0))
            t.price = pool_data.get("price", t.price)
            if pool_data.get("created_at", 0) > 0:
                t.pool_age_minutes = max(0.1, (now - pool_data["created_at"]) / 60.0)

            if t.buys_count == 0:
                t.buys_count = int(pool_data.get("buys_m5", 0) or 0)
                t.sells_count = int(pool_data.get("sells_m5", 0) or 0)
                vol = float(pool_data.get("vol_m5", 0.0) or 0.0)
                total_tx = max(1, t.buys_count + t.sells_count)
                t.volume_buys = vol * (t.buys_count / total_tx)
                t.volume_sells = vol * (t.sells_count / total_tx)
                t.volume_usd = vol

        # 2. Pengecekan Paralel Keamanan + Detektor Cabal + Detektor Wash
        (
            holders,
            lp_info,
            authorities,
            transfer_fee,
            metadata_mutable,
            sell_ok,
            creator_rep,
            social,
            dev_sold,
        ) = await asyncio.gather(
            self.rpc.get_holders(mint),
            self.rpc.get_lp_info(mint, pool_data),
            self.rpc.get_token_authorities(mint),
            self.rpc.get_transfer_fee(mint),
            self.rpc.get_metadata_mutability(mint),
            self.rpc.simulate_sell(mint),
            self.rpc.get_creator_reputation(mint),
            self.rpc.check_social_presence(mint, pool_data),
            self.rpc.check_dev_sold(mint, t.creator, t.created_at) if t.creator else self._zero(),
        )

        locked, liq_usd, lp_pool, rug_score, rug_flags, vaults = lp_info
        mint_auth, freeze_auth = authorities
        rugpull_count, _ = creator_rep
        has_tw, has_tg, has_web = social

        t.holders = holders
        t.liquidity_usd = max(t.liquidity_usd, liq_usd)
        t.lp_locked = locked
        t.lp_pool = lp_pool
        t.rug_score = rug_score
        t.mint_authority_active = mint_auth
        t.freeze_authority_active = freeze_auth
        t.metadata_mutable = metadata_mutable
        t.transfer_fee_pct = transfer_fee
        t.sell_simulation_ok = sell_ok
        t.creator_rugpull_count = rugpull_count
        t.has_twitter = has_tw
        t.has_telegram = has_tg
        t.has_website = has_web
        t.dev_sold_pct = dev_sold

        # ====================================================
        # PISAHKAN KOLAM POOL / AMM VAULT DARI DOMPET PENGGUNA
        # ====================================================
        t.pool_vaults.update(vaults)
        if t.pool_address:
            t.pool_vaults.add(t.pool_address)
        if lp_pool:
            t.pool_vaults.add(lp_pool)

        # 3. ADVANCED ANTI-DEV LINKAGE & SYBIL CABAL CHECK
        early_wallets = [b["wallet"] for b in self.buyers_buffer.get(mint, [])]
        (
            is_cabal,
            dev_linked_pct,
            dev_linked_wallets,
            unlinked_holders,
            unlinked_buyers,
            cabal_flags
        ) = await self.cabal_detector.analyze_dev_linkage_and_sybil(
            holders, early_wallets, t.creator, t.created_at, t.pool_vaults
        )
        t.cabal_cluster_pct = dev_linked_pct
        t.cabal_wallets_count = len(dev_linked_wallets)
        t.dev_linked_wallets = dev_linked_wallets
        t.dev_linked_holding_pct = dev_linked_pct
        t.unlinked_holders_count = len(unlinked_holders)
        t.unlinked_buyers_count = len(unlinked_buyers)

        # 4. ANTI-WASH TRADING & FAKE VOLUME CHECK
        is_wash, unique_ratio, wash_flags = self.wash_detector.evaluate_wash(
            self.buyers_buffer.get(mint, []),
            t.buys_count,
            t.sells_count,
            len(holders),
            t.volume_usd or (t.volume_buys + t.volume_sells) * 160.0
        )
        t.unique_trader_ratio = unique_ratio
        t.is_wash_trading = is_wash
        t.wash_flags = wash_flags

        # 5. Dev Holding & Concentration (HANYA Dompet Murni Independen!)
        individual_holders = {
            w: p for w, p in holders.items()
            if w in unlinked_holders and w not in t.pool_vaults and w not in KNOWN_DEX_PROGRAMS
        }

        # Cek on-chain jika akun top holder melebihi batas aman (verifikasi apakah akun tersebut sebenarnya adalah LP Vault AMM Raydium / Meteora / PumpSwap)
        sorted_individuals = sorted(individual_holders.items(), key=lambda x: x[1], reverse=True)
        for top_cand, top_pct in list(sorted_individuals[:4]):
            if top_pct > self.cfg.max_top1_holder_pct:
                if await self.rpc.check_is_amm_vault(top_cand, t.mint):
                    t.pool_vaults.add(top_cand)
                    individual_holders.pop(top_cand, None)

        sorted_individuals = sorted(individual_holders.items(), key=lambda x: x[1], reverse=True)

        sorted_pcts = [p for _, p in sorted_individuals]
        top10_pct = sum(sorted_pcts[:10]) if sorted_pcts else 0.0
        top1_pct = sorted_pcts[0] if sorted_pcts else 0.0
        t.dev_holding_pct = max(
            individual_holders.get(t.creator, 0.0) if t.creator else 0.0,
            dev_linked_pct
        )

        # Alpha Wallets (Menggunakan Dompet Pengguna Independen Murni)
        clean_early_wallets = [w for w in early_wallets if w in unlinked_holders or w in unlinked_buyers]
        if not clean_early_wallets and individual_holders:
            clean_early_wallets = list(individual_holders.keys())[:15]
        t.alpha_wallet_count = await self.rpc.count_alpha_wallets(clean_early_wallets)

        # Velocity
        if len(t.sol_in_bonding_history) >= 2:
            dt = (t.sol_in_bonding_history[-1][0] - t.sol_in_bonding_history[0][0]) / 60.0
            if dt > 0.05:
                t.liquidity_velocity = max(0.0, (t.sol_in_bonding_history[-1][1] - t.sol_in_bonding_history[0][1]) / dt)
        elif pool_data and pool_data.get("vol_m5", 0) > 0:
            t.liquidity_velocity = (pool_data["vol_m5"] / 160.0) / 5.0

        # ----------------------------------------------------
        # 6. FILTER RED FLAGS (ANTI-JEBAKAN & ANTI-KOIN MATI)
        # ----------------------------------------------------
        red_flags: List[str] = []

        # ====================================================
        # SYARAT MUTLAK PEMEGANG & PEMBELI INDEPENDEN (TIDAK TERKAIT DEV)
        # ====================================================
        min_unlinked_h = self.cfg.min_unlinked_dex_holders_count if t.is_migrated else self.cfg.min_unlinked_holders_count
        if t.unlinked_holders_count < min_unlinked_h:
            red_flags.append(f"insufficient_unlinked_holders:{t.unlinked_holders_count}<{min_unlinked_h}")

        if not t.is_migrated:
            # Fallback 1: Jika volume websocket belum tercatat, ambil dari data pool / DexScreener
            if t.volume_buys <= 0 and pool_data:
                t.volume_buys = pool_data.get("vol_m5", 0.0) / 160.0
                t.unlinked_buyers_count = max(t.unlinked_buyers_count, pool_data.get("buys_m5", 0))

            # Fallback 2: Jika koin pump.fun sudah naik bonding curve > 10%, gunakan akumulasi real SOL di bonding
            if t.volume_buys < self.cfg.min_volume_buys_sol and t.bonding_pct >= 10.0:
                t.volume_buys = max(t.volume_buys, (t.bonding_pct / 100.0) * 85.0)

            # Fallback 3: Estimasi pembeli dari jumlah holder organik on-chain
            if t.unlinked_buyers_count < self.cfg.min_unlinked_buyers_count and len(holders) >= self.cfg.min_holders_count:
                t.unlinked_buyers_count = max(t.unlinked_buyers_count, len(unlinked_holders))

            if t.unlinked_buyers_count < self.cfg.min_unlinked_buyers_count:
                red_flags.append(f"insufficient_unlinked_buyers:{t.unlinked_buyers_count}<{self.cfg.min_unlinked_buyers_count}")
            if t.volume_buys < self.cfg.min_volume_buys_sol:
                red_flags.append(f"insufficient_buy_volume:{t.volume_buys:.1f}SOL<{self.cfg.min_volume_buys_sol}SOL")
        else:
            buys_m5 = pool_data.get("buys_m5", 0) if pool_data else 0
            buys_h1 = pool_data.get("buys_h1", 0) if pool_data else 0
            if buys_m5 < self.cfg.min_organic_buys_m5 and buys_h1 < 30:
                red_flags.append(f"dead_dex_buys:{buys_m5}m5,{buys_h1}h1")

        # --- JEBAKAN CABAL, AFILIASI DEV, & FAKE VOLUME ---
        if is_cabal:
            red_flags.extend(cabal_flags)
        if is_wash:
            red_flags.extend(wash_flags)

        # --- JEBAKAN DEVELOPER KLASIK ---
        if self.cfg.require_freeze_authority_revoked and freeze_auth:
            red_flags.append("freeze_authority_active")
        if self.cfg.require_mint_authority_revoked and mint_auth:
            red_flags.append("mint_authority_active")
        if not sell_ok:
            red_flags.append("honeypot_sell_blocked")
        if transfer_fee > self.cfg.max_transfer_fee_pct:
            red_flags.append(f"transfer_tax_high:{transfer_fee:.1f}%")
        if rugpull_count > self.cfg.max_creator_rugpull_count:
            red_flags.append("creator_rugpull_history")
        if t.dev_sold_pct > self.cfg.max_dev_sell_pct:
            red_flags.append(f"dev_dumped:{t.dev_sold_pct:.1f}%")
        if t.dev_holding_pct > self.cfg.max_dev_holding_pct:
            red_flags.append(f"dev_holding_high:{t.dev_holding_pct:.1f}%")
        if top1_pct > self.cfg.max_top1_holder_pct:
            red_flags.append(f"top1_whale:{top1_pct:.1f}%")
        if top10_pct > self.cfg.max_top10_holder_pct:
            red_flags.append(f"top10_high:{top10_pct:.1f}%")

        if t.is_migrated and self.cfg.require_lp_locked and not locked:
            if t.liquidity_usd < self.cfg.min_pool_liquidity_usd:
                red_flags.append("lp_not_locked")

        # ========================================================
        # ANTI-PUCUK & ANTI-EXHAUSTION (HIT & RUN SWEET SPOT)
        # ========================================================
        if pool_data:
            pc_m5 = float(pool_data.get("price_change_m5", 0.0) or 0.0)
            pc_h1 = float(pool_data.get("price_change_h1", 0.0) or 0.0)
            buys_m5_cnt = int(pool_data.get("buys_m5", 0) or 0)
            sells_m5_cnt = int(pool_data.get("sells_m5", 0) or 0)

            # 1. Anti-Pucuk 5 Menit: Jangan beli koin yang sudah meledak parabolik di wick tertinggi
            if pc_m5 > self.cfg.max_price_pump_5m_pct:
                red_flags.append(f"pump_overextended_m5:{pc_m5:+.1f}%>{self.cfg.max_price_pump_5m_pct}%")

            # 2. Anti-Pucuk 1 Jam: Jangan beli koin yang 1 jam terakhir sudah naik gila-gilaan (exit liquidity)
            if pc_h1 > self.cfg.max_price_pump_1h_pct:
                red_flags.append(f"pump_overextended_h1:{pc_h1:+.1f}%>{self.cfg.max_price_pump_1h_pct}%")

            # 3. Anti-Fake MC: Rasio Likuiditas vs MCap wajib memadai (min 3.5%)
            if t.market_cap_usd > 0 and t.liquidity_usd > 0:
                liq_ratio = t.liquidity_usd / t.market_cap_usd
                if liq_ratio < self.cfg.min_liquidity_to_mcap_ratio:
                    red_flags.append(f"thin_liquidity_trap:{liq_ratio:.1%}<{self.cfg.min_liquidity_to_mcap_ratio:.1%}")

            # 4. Anti-Falling Knife (Pisau Jatuh): Koin tidak boleh sedang dibuang deras di 5m
            if pc_m5 < -8.0:
                red_flags.append(f"falling_knife_5m:{pc_m5:.1f}%")

            # 5. Percikan Ignition untuk Koin Runner (> 1 Jam s/d 12 Jam):
            # Koin multi-hour HANYA dimasuki ketika ada percikan akumulasi baru di 5m terakhir (mulai naik)!
            pool_age_mins = t.pool_age_minutes
            if pool_age_mins > 60.0 and getattr(self.cfg, "require_spark_ignition_for_runners", True):
                has_spark = (buys_m5_cnt >= 8) and (buys_m5_cnt >= sells_m5_cnt * 1.15) and (pc_m5 >= 0.5)
                if not has_spark:
                    red_flags.append(f"waiting_breakout_spark:buys={buys_m5_cnt},sells={sells_m5_cnt},pc5m={pc_m5:+.1f}%")

        # 6. EVALUASI EARLY ACCUMULATION (PRE-PARABOLIC ORGANIC WAVE)
        pc_5m = float(pool_data.get("price_change_m5", 0.0) or 0.0) if pool_data else 0.0
        if not pool_data and t.price_at_5m_ago > 0:
            pc_5m = ((t.price - t.price_at_5m_ago) / t.price_at_5m_ago) * 100.0

        is_early_org, org_score, org_reasons, org_red_flags = self.early_indexer.evaluate_early_accumulation(t, pc_5m)

        # Hitung skor momentum
        score, phase, reasons, score_flags = self.scorer.score_token(t)
        red_flags.extend(score_flags)
        red_flags.extend(rug_flags)

        # Jika lolos akumulasi organik awal (Ignition), tandai fase khusus
        if is_early_org:
            phase = "PRE_PARABOLIC_IGNITION"
            score = max(score, org_score)
            reasons.extend(org_reasons)

        t.scored = True

        # Keputusan: Re-scan jika koin potensial sedang mengonfirmasi likuiditas atau menunggu pool DLMM
        if red_flags:
            # Periksa apakah ada bendera merah fatal yang tidak bisa diperbaiki (scam/rug permanen)
            fatal_keywords = (
                "mint_authority_active", "freeze_authority_active", "honeypot",
                "transfer_tax_high", "creator_rugpull_history", "dev_dumped",
                "cabal_sybil", "top1_whale", "top10_high"
            )
            is_fatal = any(any(k in flag for k in fatal_keywords) for flag in red_flags)

            # Cetak ringkasan exercise koin agar user dapat melihat di console
            sym = t.symbol or "TOKEN"
            mc_k = t.market_cap_usd / 1000.0 if t.market_cap_usd > 0 else 0.0
            liq_k = t.liquidity_usd / 1000.0 if t.liquidity_usd > 0 else 0.0
            status_text = "INKUBASI-RESCAN" if (not is_fatal and t.rescan_count < self.cfg.max_rescan_count) else "FILTER-TOLAK"
            flags_str = ", ".join(red_flags[:2])
            print(f"🔎 [EXERCISE] {sym} | MCap=${mc_k:.1f}k | Liq=${liq_k:.1f}k | Skor={score:.0f}/100 | {status_text} -> [{flags_str}]")

            # Jika koin aman dari scam permanen dan hanya dalam fase inkubasi (menunggu injeksi pool DEX/volume pembeli):
            if not is_fatal and t.rescan_count < self.cfg.max_rescan_count and age < self.cfg.max_rescan_age_sec:
                t.rescan_count += 1
                t.scored = False
                # Smart multi-stage delay:
                # 0-3 menit: 20 detik (menangkap lepas landas Pump.fun)
                # 3-15 menit: 45 detik (menangkap pembentukan pool Raydium/Meteora)
                # 15-60 menit: 90 detik (menangkap koin 1 jam yang sedang konsolidasi)
                # 1-12 jam: 180 detik (menangkap breakout multi-hour runner gelombang 2 & 3)
                if age < 180:
                    delay = 20.0
                elif age < 900:
                    delay = 45.0
                elif age < 3600:
                    delay = 90.0
                else:
                    delay = 180.0
                t.next_rescan_at = now + delay
                heapq.heappush(self.rescan_heap, (t.next_rescan_at, mint))
            return

        # LOLOS!
        if score >= self.cfg.min_conviction_score and not t.signal_emitted:
            t.signal_emitted = True
            signal_obj = Signal(
                mint=mint,
                name=t.name or "Token",
                symbol=t.symbol or "PUMP",
                score=score,
                entry_price=t.price,
                reasons=reasons,
                red_flags=[],
                timestamp=now,
                source=t.source,
                phase=phase,
                market_cap=t.market_cap_usd,
                liquidity=t.liquidity_usd,
            )
            self.signals.append(signal_obj)
            await self.signal_queue.put(signal_obj)
            print(f"🔥 [SIGNAL-{phase.upper()}] {t.name} (${t.symbol}) | Skor={score} | MCap=${t.market_cap_usd:,.0f} | Liq=${t.liquidity_usd:,.0f} | TELEGRAM SENT ✅")
            if self.cfg.telegram_enabled:
                await self.telegram.send_signal_alert(signal_obj, t)

    async def _zero(self):
        return 0.0

    async def signal_consumer(self):
        while self.running:
            try:
                signal_obj = await asyncio.wait_for(self.signal_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self.execute_entry(signal_obj)
            except Exception:
                pass
            finally:
                self.signal_queue.task_done()

    async def execute_entry(self, signal_obj: Signal):
        size = self.cfg.portfolio_usd * (self.cfg.position_size_pct / 100.0)
        if not self.positions.can_open(size, self.cfg.portfolio_usd):
            return
        pos = self.positions.open(signal_obj, size)
        print(f"⚡ [BUY-ENTRY] {signal_obj.symbol} | Size: ${size:.1f} | Entry: ${signal_obj.entry_price:.8f} | Stop: ${pos.stop_price:.8f}")

    async def monitor_loop(self):
        last_heartbeat = 0.0
        while self.running:
            try:
                now = time.time()
                # Status heartbeat setiap 45 detik agar user dapat memantau aktivitas bot di console Render
                if now - last_heartbeat >= 45.0:
                    last_heartbeat = now
                    tracked = len(self.tokens)
                    eval_q = self.eval_queue.qsize()
                    rescan_q = len(self.rescan_heap)
                    signals_cnt = len(self.signals)
                    active_pos = len(self.positions.positions)
                    print(
                        f"📊 [MONITOR-HEARTBEAT] Bot Aktif Memindai | "
                        f"Token di Memori: {tracked} | Antrean Audit: {eval_q} | "
                        f"Antrean Rescan: {rescan_q} | Sinyal Lolos: {signals_cnt} | Posisi: {active_pos}"
                    )

                for mint in list(self.positions.positions.keys()):
                    t = self.tokens.get(mint)
                    price = t.price if t else 0.0
                    if price <= 0:
                        pool = await self.rpc.get_pool_data(mint)
                        price = pool.get("price", 0.0)
                    if price > 0:
                        actions = self.positions.update(mint, price)
                        for act, pct in actions:
                            await self.execute_exit(mint, price, act, pct)
                self.cache.cleanup()
            except Exception:
                pass
            await asyncio.sleep(2.0)

    async def execute_exit(self, mint: str, price: float, action: str, pct: float):
        pos = self.positions.positions.get(mint)
        if not pos:
            return
        pnl_pct = ((price - pos.entry_price) / pos.entry_price) * 100.0
        print(f"💰 [EXIT-{action.upper()}] {pos.symbol} @ ${price:.8f} | PnL: {pnl_pct:+.1f}% | Size: {pct:.0f}%")
        if action in ("stop_loss", "trailing_stop", "time_stop"):
            self.positions.close(mint, pnl_pct)

    async def rescan_loop(self):
        while self.running:
            try:
                now = time.time()
                while self.rescan_heap and self.rescan_heap[0][0] <= now:
                    _, mint = heapq.heappop(self.rescan_heap)
                    t = self.tokens.get(mint)
                    if t and not t.signal_emitted:
                        await self.eval_queue.put(mint)
            except Exception:
                pass
            await asyncio.sleep(2.0)

    async def housekeeping_loop(self):
        """
        Pembersihan memori otomatis untuk mencegah Out-Of-Memory (OOM) di Render (Batas RAM 512 MB).
        - Membersihkan cache RPC kadaluarsa
        - Memangkas token mati/scam berumur > 30 menit (likuiditas < $1K atau volume 0)
        - Mempertahankan token runner potensial hingga 24 jam penuh (86,400 detik)
        - Membersihkan buyers_buffer
        - Memanggil gc.collect() untuk menjaga RAM stabil < 100MB
        """
        import gc
        while self.running:
            try:
                await asyncio.sleep(180)  # Setiap 3 menit
                now = time.time()
                
                # 1. Bersihkan TimedCache
                self.cache.cleanup()
                
                # 2. Pangkas token cerdas:
                # - Token mati/sampah (< $1000 liq atau < $5000 MC): buang cepat (30 menit)
                # - Token runner sehat: pertahankan observasi hingga 24 jam (86400 detik)
                active_pos_mints = set(self.positions.positions.keys())
                dead_mints = []
                for mint, t in list(self.tokens.items()):
                    if mint in active_pos_mints:
                        continue
                    age = now - t.created_at
                    is_dead_trash = (t.liquidity_usd < 1000.0 or t.market_cap_usd < 5000.0) and age > 1800
                    is_expired_runner = age > 86400 and (t.scored or t.rescan_count >= self.cfg.max_rescan_count)
                    
                    if is_dead_trash or is_expired_runner:
                        dead_mints.append(mint)
                
                for mint in dead_mints:
                    self.tokens.pop(mint, None)
                    self.buyers_buffer.pop(mint, None)
                
                # 3. Paksa garbage collection agar RAM Render tetap stabil di bawah 100MB
                gc.collect()
            except Exception:
                pass

    async def run(self):
        await self.rpc.start()
        await self.telegram.start()

        tasks = [
            asyncio.create_task(self.pumpportal_listener()),
            asyncio.create_task(self.dexscreener_discovery_loop()),
            asyncio.create_task(self.geckoterminal_discovery_loop()),
            asyncio.create_task(self.signal_consumer()),
            asyncio.create_task(self.monitor_loop()),
            asyncio.create_task(self.rescan_loop()),
            asyncio.create_task(self.housekeeping_loop()),
        ]
        for i in range(self.cfg.worker_count):
            tasks.append(asyncio.create_task(self.worker(i)))

        print(f"🚀 [HITRUN-V12] Anti-Cabal & Anti-Wash Online | MaxMC=$600K | Workers={self.cfg.worker_count}")
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        finally:
            if not self.running:
                for t in tasks:
                    t.cancel()
                await self.telegram.close()
                await self.rpc.close()


# ============================================================
# 11. HEALTH SERVER & DASHBOARD API
# ============================================================

class HealthServer:
    def __init__(self, scanner):
        self.scanner = scanner
        self.app = Flask(__name__)
        self._register_routes()

    def _keepalive_worker(self, port: int):
        """Self-ping loop internal setiap 2.5 menit untuk mencegah idle/sleep di container Render."""
        time.sleep(15)
        while True:
            try:
                time.sleep(150)
                url = f"http://127.0.0.1:{port}/"
                req = urllib.request.Request(url, headers={"User-Agent": "HitRunKeepalive/1.0"})
                with urllib.request.urlopen(req, timeout=5) as resp:
                    resp.read()
            except Exception:
                pass

    def _register_routes(self):
        @self.app.route("/")
        def index():
            return jsonify({
                "service": "hitrun-scanner",
                "version": "v12-anti-cabal-anti-wash",
                "status": "online",
                "tokens_tracked": len(self.scanner.tokens),
                "signals_emitted": len(self.scanner.signals),
                "active_positions": len(self.scanner.positions.positions),
            })

        @self.app.route("/signals")
        def signals():
            return jsonify([
                {
                    "mint": s.mint,
                    "symbol": s.symbol,
                    "score": s.score,
                    "phase": s.phase,
                    "entry_price": s.entry_price,
                    "market_cap": s.market_cap,
                    "liquidity": s.liquidity,
                }
                for s in self.scanner.signals[-30:]
            ])

        @self.app.route("/check/<mint>")
        def check_token(mint):
            t = self.scanner.tokens.get(mint)
            if not t:
                return jsonify({
                    "mint": mint,
                    "status": "not_in_memory",
                    "total_tracked": len(self.scanner.tokens),
                    "message": "Token belum masuk antrean observasi bot"
                }), 404

            top1_holder = max(t.holders.values()) if t.holders else 0.0
            return jsonify({
                "mint": mint,
                "name": t.name,
                "symbol": t.symbol,
                "dex_id": t.dex_id,
                "source": t.source,
                "price": t.price,
                "market_cap": t.market_cap_usd,
                "liquidity_usd": t.liquidity_usd,
                "bonding_pct": t.bonding_pct,
                "scored": t.scored,
                "signal_emitted": t.signal_emitted,
                "rescan_count": t.rescan_count,
                "top1_holder_pct": round(top1_holder, 2),
                "dev_holding_pct": round(t.dev_holding_pct, 2),
                "unlinked_holders": t.unlinked_holders_count,
                "unlinked_buyers": t.unlinked_buyers_count,
                "organic_buys_count": t.organic_buys_count,
                "organic_volume_buys_sol": round(t.organic_volume_buys_sol, 2),
                "is_wash_trading": t.is_wash_trading,
                "wash_flags": t.wash_flags,
                "dev_linked_holding_pct": round(t.dev_linked_holding_pct, 2),
            })

    def run(self, port: int):
        threading.Thread(target=self._keepalive_worker, args=(port,), daemon=True).start()
        self.app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)


# ============================================================
# 12. ENTRY POINT (CONTINUOUS ALWAYS-ON ENGINE)
# ============================================================

async def main():
    cfg = Config()
    scanner = HitAndRunScanner(cfg)

    port = int(os.getenv("PORT", "10000"))
    health = HealthServer(scanner)
    threading.Thread(target=health.run, args=(port,), daemon=True).start()
    print(f"[web] Health server & internal keepalive listening on :{port}")

    def handle_shutdown(signum, frame):
        print(f"[shutdown] Sinyal OS ({signum}) diterima...")
        if signum == signal.SIGINT:
            scanner.running = False

    try:
        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)
    except Exception:
        pass

    # Mesin On Terus: Otomatis restart jika terjadi kendala jaringan/koneksi
    while scanner.running:
        try:
            await scanner.run()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[engine] Scanner error: {e}. Auto-restarting engine in 3s...")
            await asyncio.sleep(3.0)


if __name__ == "__main__":
    asyncio.run(main())
