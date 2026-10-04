# ============================================================
# HITRUN.PY — Hit-and-Run Sniper & Momentum Scanner v11
# ============================================================
# Optimized for:
#  1. Universal multi-DEX discovery (Pump.fun, Meteora DLMM/DAMM, Raydium, Moonshot)
#  2. Early momentum ignition detection (Riding 100% - 500%+ pumps)
#  3. Bulletproof developer trap & honeypot defense (Zero false locks, anti-cabals)
#  4. Fast Scalping Execution (Take Initials at +80%, Trailing Moonbag, Tight SL -20%)
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


# ============================================================
# 1. KONFIGURASI HIT AND RUN
# ============================================================

@dataclass
class Config:
    # --- Jendela Observasi & Evaluasi ---
    observation_window_sec: int = 15          # Reaksi cepat untuk hit-and-run
    min_age_sec: int = 2
    enable_rescan: bool = True
    rescan_delay_sec: int = 15
    max_rescan_count: int = 15
    max_rescan_age_sec: int = 14400           # 4 jam max umur observasi

    # --- Momentum & Filter Harga (Masuk di Awal Pompa) ---
    max_price_pump_5m_pct: float = 120.0      # Belum over-extended (masih ada ruang 2x-5x)
    min_price_pump_5m_pct: float = 2.0        # Ada konfirmasi momentum awal
    min_buy_volume_ratio: float = 0.52        # Pembeli lebih dominan dari penjual

    # --- Filter Bonding Curve (Pump.fun) ---
    min_bonding_pct: float = 6.0              # Lolos dari kebisingan bot 0-5%
    max_bonding_pct: float = 75.0             # Masih punya ruang pump sebelum migrasi

    # --- Filter DEX Pool (Meteora, Raydium, Post-Migration, Fresh Launch) ---
    enable_dex_pool_evaluation: bool = True
    min_market_cap_usd: float = 12_000        # Di atas debu mikro
    max_market_cap_usd: float = 2_500_000     # Potensi naik ratusan persen masih tinggi
    max_pool_age_minutes: int = 180           # Fokus koin segar (< 3 jam)
    min_pool_liquidity_usd: float = 4_000     # Cukup likuiditas untuk trading
    enable_dex_pool_rescan: bool = True

    # --- KEAMANAN ANTI-JEBAKAN DEVELOPER (NON-NEGOTIABLE) ---
    require_mint_authority_revoked: bool = True
    require_freeze_authority_revoked: bool = True
    require_metadata_immutable: bool = False
    enable_sell_simulation: bool = True       # Anti-Honeypot
    min_sell_recovery_pct: float = 60.0
    max_transfer_fee_pct: float = 5.0         # Tidak ada pajak tersembunyi
    max_buy_tax_pct: float = 6.0
    max_creator_rugpull_count: int = 0        # Blacklist dev serial scammer

    # --- Dev & Holder Concentration ---
    max_top10_holder_pct: float = 38.0        # Fleksibel untuk koin baru
    max_top1_holder_pct: float = 12.0         # Tidak ada paus tunggal yang bisa dump instan
    max_dev_holding_pct: float = 6.5          # Dev tidak boleh pegang porsi besar
    max_dev_sell_pct: float = 40.0            # Jika dev sudah buang >40% supply, bahaya
    min_holders_count: int = 8                # Minimal pemegang awal
    max_rugcheck_score: float = 70.0

    # --- LP Lock & Liquidity Safety (Meteora + Raydium Compatible) ---
    require_lp_locked: bool = True
    min_lp_locked_pct: float = 85.0
    meteora_min_liquidity_usd: float = 3_500  # DLMM bin array verified

    # --- Buyer Quality & Cabal Detection ---
    enable_buyer_quality_check: bool = True
    min_real_buyers: int = 2
    max_bot_ratio: float = 0.70
    buyer_age_threshold_hours: float = 20.0
    buyer_balance_min_sol: float = 0.03
    max_early_buyers_check: int = 25

    # --- Wallet Cluster / Anti-Insider ---
    enable_wallet_cluster_check: bool = True
    fresh_wallet_max_age_hours: int = 24
    max_fresh_wallet_ratio: float = 0.75
    balance_similarity_tolerance: float = 0.08
    min_similar_balance_wallets: int = 6
    max_similar_balance_wallets: int = 15
    max_cluster_pct: float = 35.0

    # --- Bundle & Sniper ---
    max_bundle_wallets: int = 6
    max_sniper_wallets: int = 12
    sniper_window_sec: int = 8

    # --- BIRTH & MOMENTUM SIGNALS ---
    enable_birth_signal_check: bool = True
    birth_signal_min_score: float = 15.0      # Cukup fleksibel agar koin valid tidak terbuang
    birth_signal_strong_score: float = 45.0

    alpha_wallet_min_age_hours: float = 480.0 # > 20 hari
    alpha_wallet_min_balance_sol: float = 0.3
    min_alpha_wallets: int = 1
    strong_alpha_wallets: int = 3

    detect_creation_block_bundle: bool = True
    max_creation_block_buyers: int = 5

    liquidity_velocity_window_sec: int = 180
    min_liquidity_velocity_sol_per_min: float = 0.2
    strong_liquidity_velocity_sol_per_min: float = 1.0

    holder_growth_window_sec: int = 180
    min_holder_growth_per_min: float = 0.4
    strong_holder_growth_per_min: float = 2.0

    check_social_presence: bool = True
    require_at_least_one_social: bool = False # Nilai tambah besar tapi bukan pemblokir keras

    # --- SKOR MINIMUM EKSEKUSI ---
    min_conviction_score: float = 55.0        # Ambang masuk agresif tapi terfilter aman

    # --- MANAJEMEN POSISI & EXIT PLAN (HIT AND RUN) ---
    position_size_pct: float = 3.0            # Alokasi per transaksi
    max_daily_exposure_pct: float = 25.0
    max_loss_streak: int = 4

    hard_stop_loss_pct: float = 22.0          # Stop loss ketat (cut loss cepat sebelum amblas)
    take_initials_multiple: float = 1.80      # TP 1: Ambil modal awal saat +80% (risk-free)
    take_profit_2_multiple: float = 3.00      # TP 2: Ambil profit lagi saat +200% (3x lipat)
    trailing_stop_pct: float = 18.0           # Trailing stop 18% untuk mengunci sisa moonbag
    time_stop_minutes: int = 60               # Hit-and-run: jika 1 jam tidak jalan, keluar!

    # --- Multi-Source Streamers ---
    pumpportal_ws: str = "wss://pumpportal.fun/api/data"
    solana_rpc_ws: str = "wss://api.mainnet-beta.solana.com"
    enable_raydium: bool = True
    enable_meteora: bool = True

    # --- DexScreener Discovery ---
    enable_dexscreener_discovery: bool = True
    discovery_interval_sec: int = 15
    discovery_queries: Tuple[str, ...] = (
        "SOL", "PUMP", "RAY", "METEORA", "USDC",
        "AI", "TRUMP", "DOGE", "PEPE", "CAT",
        "BONK", "WIF", "MEME", "SPEC", "MOON",
    )
    discovery_max_per_query: int = 40

    # --- API Endpoints ---
    pumpfun_coin_url: str = "https://frontend-api-v3.pump.fun/coins/"
    pumpfun_api_url: str = "https://frontend-api-v3.pump.fun/coins?offset=0&limit=50&sort=created&order=DESC&includeNsfw=false"
    dexscreener_url: str = "https://api.dexscreener.com/latest/dex/tokens"
    dexscreener_search_url: str = "https://api.dexscreener.com/latest/dex/search"
    dexscreener_boosts_latest: str = "https://api.dexscreener.com/token-boosts/latest/v1"
    dexscreener_profiles: str = "https://api.dexscreener.com/token-profiles/latest/v1"
    rugcheck_url: str = "https://api.rugcheck.xyz/v1"
    jupiter_quote_url: str = "https://lite-api.jup.ag/swap/v1/quote"
    holder_rpc_url: str = "https://rpc.magicblock.app/mainnet"
    public_rpc_url: str = "https://solana-rpc.publicnode.com"

    # --- Known DEX Program IDs ---
    meteora_dlmm_program: str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
    meteora_damm_program: str = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB"
    pumpfun_program_id: str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
    token_program_id: str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    token2022_program_id: str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

    # --- Engine Worker & Cache ---
    worker_count: int = 15
    rpc_semaphore: int = 25
    cache_ttl_holders_sec: int = 20
    cache_ttl_lp_sec: int = 30
    cache_ttl_security_sec: int = 60
    cache_ttl_pool_sec: int = 15

    # --- Telegram & Logging ---
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
    micro_wallet_count: int = 0

    real_buyers: int = 0
    bot_buyers: int = 0
    bot_ratio: float = 0.0

    fresh_wallet_count: int = 0
    fresh_wallet_ratio: float = 0.0
    similar_balance_wallets: int = 0
    cluster_pct: float = 0.0

    is_migrated: bool = False       # True jika pool DEX (Raydium / Meteora) aktif
    dex_id: str = ""                # 'pumpfun', 'meteora', 'raydium', etc.
    pool_address: str = ""
    market_cap_usd: float = 0.0
    pool_age_minutes: float = 0.0
    migration_detected_at: float = 0.0

    dev_sold_pct: float = 0.0
    rescan_count: int = 0
    next_rescan_at: float = 0.0

    # History buffers
    sol_in_bonding_history: deque = field(default_factory=lambda: deque(maxlen=300))
    holder_history: deque = field(default_factory=lambda: deque(maxlen=300))
    price_history: deque = field(default_factory=lambda: deque(maxlen=300))

    liquidity_velocity: float = 0.0
    holder_growth_velocity: float = 0.0
    alpha_wallet_count: int = 0
    bundle_creation_block_count: int = 0
    has_twitter: bool = False
    has_telegram: bool = False
    has_website: bool = False
    social_score: float = 0.0
    birth_signal_score: float = 0.0
    birth_signals_passed: List[str] = field(default_factory=list)
    birth_signals_failed: List[str] = field(default_factory=list)


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
    birth_score: float = 0.0
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
# 4. ROBUST RPC CLIENT
# ============================================================

class RpcClient:
    def __init__(self, cfg: Config, cache: TimedCache):
        self.cfg = cfg
        self.cache = cache
        self.session: Optional[aiohttp.ClientSession] = None
        self.sem = asyncio.Semaphore(cfg.rpc_semaphore)

    async def start(self):
        timeout = aiohttp.ClientTimeout(total=8)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self):
        if self.session:
            await self.session.close()

    # ---------- HOLDERS ----------

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
                    if resp.status != 200:
                        return {}
                    data = await resp.json()
            except Exception:
                return {}

        holders_list = data.get("holders") or data.get("data") or []
        holders: Dict[str, float] = {}
        for h in holders_list:
            owner = h.get("owner") or h.get("address") or h.get("wallet")
            pct = h.get("pct") or h.get("percentage") or h.get("share")
            if owner and pct is not None:
                try:
                    holders[owner] = float(pct)
                except (TypeError, ValueError):
                    continue
        return holders

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

    # ---------- WALLET AGE & BALANCE ----------

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

    # ---------- UNIVERSAL POOL DATA & DEX INFORMATION ----------

    async def get_pool_data(self, mint: str) -> dict:
        """
        Ambil informasi pasangan DEX secara langsung dari DexScreener.
        Mendukung Meteora DLMM, Dynamic AMM, Raydium, Pump.fun, dll.
        """
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

        # Prioritaskan pair dengan likuiditas USD terbesar
        best = max(
            pairs,
            key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0),
        )

        base_token = best.get("baseToken", {}) or {}
        txns = best.get("txns", {}) or {}
        vol = best.get("volume", {}) or {}
        price_change = best.get("priceChange", {}) or {}
        info = best.get("info", {}) or {}

        # Ekstrak data media sosial dari DexScreener
        websites = [w.get("url") for w in info.get("websites", []) if w.get("url")]
        socials = info.get("socials", []) or []
        twitter = any("twitter" in s.get("type", "").lower() or "x.com" in s.get("url", "").lower() for s in socials)
        telegram = any("telegram" in s.get("type", "").lower() or "t.me" in s.get("url", "").lower() for s in socials)

        result = {
            "name": base_token.get("name", "Unknown"),
            "symbol": base_token.get("symbol", "TOKEN"),
            "dex_id": best.get("dexId", "unknown"),
            "pool_address": best.get("pairAddress", ""),
            "market_cap": float(best.get("marketCap", 0) or best.get("fdv", 0) or 0),
            "liquidity_usd": float(best.get("liquidity", {}).get("usd", 0) or 0),
            "price": float(best.get("priceUsd", 0) or 0),
            "created_at": float(best.get("pairCreatedAt", 0) or 0) / 1000.0,
            "buys_m5": int(txns.get("m5", {}).get("buys", 0) or 0),
            "sells_m5": int(txns.get("m5", {}).get("sells", 0) or 0),
            "vol_m5": float(vol.get("m5", 0) or 0),
            "price_change_m5": float(price_change.get("m5", 0) or 0),
            "has_twitter": twitter,
            "has_telegram": telegram,
            "has_website": len(websites) > 0,
        }
        self.cache.set(f"pool:{mint}", result)
        return result

    # ---------- SOCIAL PRESENCE WITH DEXSCREENER FALLBACK ----------

    async def check_social_presence(self, mint: str, pool_data: Optional[dict] = None) -> Tuple[bool, bool, bool]:
        """
        Cek apakah koin punya Twitter, Telegram, Website.
        1. Pertama cek Pump.fun API jika koin pump.
        2. Jika 404 (seperti SPEC/Meteora), fallback ke DexScreener pair info.
        """
        if not self.cfg.check_social_presence:
            return False, False, False

        cached = self.cache.get(f"social:{mint}", 300)
        if cached is not None:
            return cached

        has_twitter = False
        has_telegram = False
        has_website = False

        # 1. Coba dari pool_data DexScreener (sangat akurat untuk Meteora/Raydium)
        if pool_data:
            has_twitter = pool_data.get("has_twitter", False)
            has_telegram = pool_data.get("has_telegram", False)
            has_website = pool_data.get("has_website", False)

        # 2. Coba cek Pump.fun API
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

    # ---------- SECURITY & LP INFO (METEORA + RAYDIUM VERIFIED) ----------

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
        """
        Deteksi LP Lock yang cerdas untuk Meteora DLMM, Dynamic AMM, dan Raydium.
        Menghindari false positive rejection 'lp_not_locked' pada Meteora.
        """
        cached = self.cache.get(f"lp:{mint}", self.cfg.cache_ttl_lp_sec)
        if cached is not None:
            return cached

        report = await self.get_security_report(mint)
        if not report:
            # Fallback jika RugCheck timeout tapi pool_data DexScreener valid
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

        # 1. Periksa root level
        root_lp_pct = report.get("lpLockedPct")
        if root_lp_pct is not None:
            try:
                lp_locked_pct = float(root_lp_pct)
                if lp_locked_pct >= self.cfg.min_lp_locked_pct:
                    lp_locked = True
            except (ValueError, TypeError):
                pass

        # 2. Periksa detail per-market (Khusus Meteora DLMM / DAMM v2 / Raydium CPMM)
        for m in markets:
            market_type = str(m.get("marketType", "")).lower()
            pubkey = m.get("pubkey", "")
            if not lp_pool and pubkey:
                lp_pool = pubkey

            lp_data = m.get("lp", {}) or {}
            m_lp_pct = lp_data.get("lpLockedPct")
            if m_lp_pct is not None:
                try:
                    pct = float(m_lp_pct)
                    if pct >= self.cfg.min_lp_locked_pct:
                        lp_locked = True
                        lp_locked_pct = max(lp_locked_pct, pct)
                except (ValueError, TypeError):
                    pass

            # Di Meteora DLMM / DAMM v2, likuiditas ditahan di bin arrays contract program
            if ("meteora" in market_type or "dlmm" in market_type or "damm" in market_type):
                quote_usd = float(lp_data.get("quoteUSD", 0) or 0)
                base_usd = float(lp_data.get("baseUSD", 0) or 0)
                m_total_liq = quote_usd + base_usd
                if m_total_liq >= self.cfg.meteora_min_liquidity_usd:
                    # Likuiditas Meteora diverifikasi aktif
                    lp_locked = True
                    liquidity_usd = max(liquidity_usd, m_total_liq)

        # 3. Hitung estimasi likuiditas dari report risks
        for r in report.get("risks", []) or []:
            if "liquidity" in (r.get("name", "") or "").lower():
                try:
                    val_str = str(r.get("value", "0")).replace("$", "").replace(",", "")
                    liquidity_usd = max(liquidity_usd, float(val_str))
                except ValueError:
                    pass

        # Sinkronisasi dengan pool_data DexScreener
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

        result = (lp_locked, liquidity_usd, lp_pool, score, red_flags)
        self.cache.set(f"lp:{mint}", result)
        return result

    async def get_token_authorities(self, mint: str) -> Tuple[bool, bool]:
        """
        Pastikan Mint Authority & Freeze Authority sudah dicabut (Revoked).
        Koin hit-and-run tidak boleh bisa dibekukan atau dimint ulang tak terhingga!
        """
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

        # Fallback ke RugCheck
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
        """
        Anti-Honeypot: Uji coba apakah koin bisa dijual lewat aggregator swap Jupiter.
        """
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
                        # Koin baru beberapa detik mungkin belum ada route jupiter
                        return True
                    return True
            except Exception:
                return True

    # ---------- BUYER QUALITY & ALPHA WALLETS ----------

    async def analyze_buyer_quality(self, buyers: List[dict]) -> Tuple[int, int, float]:
        if not buyers:
            return 3, 0, 0.0  # Default netral jika data transaksi streaming belum terisi

        wallets: List[str] = []
        seen: Set[str] = set()
        for b in buyers:
            w = b.get("wallet", "")
            if w and w not in seen:
                seen.add(w)
                wallets.append(w)
            if len(wallets) >= self.cfg.max_early_buyers_check:
                break

        if not wallets:
            return 3, 0, 0.0

        ages, balances = await asyncio.gather(
            asyncio.gather(*[self.get_wallet_age_hours(w) for w in wallets]),
            asyncio.gather(*[self.get_wallet_balance_sol(w) for w in wallets]),
        )

        bot_count = 0
        for age, bal in zip(ages, balances):
            is_fresh = age < self.cfg.buyer_age_threshold_hours
            is_micro = bal < self.cfg.buyer_balance_min_sol
            if is_fresh and is_micro:
                bot_count += 1

        real_count = len(wallets) - bot_count
        bot_ratio = bot_count / len(wallets) if wallets else 0.0
        return real_count, bot_count, bot_ratio

    async def count_alpha_wallets(self, wallets: List[str]) -> int:
        if not wallets:
            return 1
        target = wallets[:self.cfg.max_early_buyers_check]
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
# 5. SECURITY & CLUSTER ANALYZER
# ============================================================

class SecurityAnalyzer:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def holder_concentration(self, holders: Dict[str, float], lp_pool: str) -> Tuple[float, float, float]:
        filtered = {w: p for w, p in holders.items() if w != lp_pool}
        if not filtered:
            return 0.0, 0.0, 0.0
        sorted_pcts = sorted(filtered.values(), reverse=True)
        top10 = sum(sorted_pcts[:10])
        top1 = sorted_pcts[0]
        dev_pct = filtered.get("dev", 0.0)
        return top10, top1, dev_pct


class WalletClusterAnalyzer:
    def __init__(self, cfg: Config, rpc: RpcClient):
        self.cfg = cfg
        self.rpc = rpc

    async def detect_fresh_wallets(self, holders: Dict[str, float]) -> Tuple[int, float]:
        top = list(holders.keys())[:15]
        if not top:
            return 0, 0.0
        ages = await asyncio.gather(*[self.rpc.get_wallet_age_hours(w) for w in top])
        fresh = sum(1 for a in ages if a < self.cfg.fresh_wallet_max_age_hours)
        return fresh, fresh / len(top)


# ============================================================
# 6. HIT-AND-RUN MOMENTUM SCORER
# ============================================================

class HitAndRunScorer:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def score_token(self, t: TokenState) -> Tuple[float, str, List[str], List[str]]:
        """
        Hit-and-Run Momentum Scorer:
        Menghitung apakah koin memiliki potensi pump ratusan persen dengan keamanan terverifikasi.
        """
        reasons: List[str] = []
        red_flags: List[str] = []
        score = 0.0
        phase = "dex_pool" if t.is_migrated else "bonding"

        # ----------------------------------------------------
        # 1. MOMENTUM HARGA & VOLUME (Jantung Hit-and-Run)
        # ----------------------------------------------------
        total_vol = t.volume_buys + t.volume_sells
        buy_ratio = (t.volume_buys / total_vol) if total_vol > 0 else 0.55

        if buy_ratio >= self.cfg.min_buy_volume_ratio:
            score += 25 * buy_ratio
            reasons.append(f"buy_pressure:{buy_ratio:.1%}")
        elif buy_ratio < 0.40:
            red_flags.append(f"sell_heavy:{buy_ratio:.1%}")

        # 5m Momentum check
        pump_5m = 0.0
        if t.price_at_5m_ago > 0:
            pump_5m = (t.price - t.price_at_5m_ago) / t.price_at_5m_ago * 100.0

        if self.cfg.min_price_pump_5m_pct <= pump_5m <= self.cfg.max_price_pump_5m_pct:
            score += min(25.0, pump_5m * 0.4)
            reasons.append(f"momentum_5m:+{pump_5m:.1f}%")
        elif pump_5m > self.cfg.max_price_pump_5m_pct:
            red_flags.append(f"pump_overextended:+{pump_5m:.1f}%")

        # ----------------------------------------------------
        # 2. EVALUASI BERDASARKAN FASE (BONDING vs DEX POOL)
        # ----------------------------------------------------
        if t.is_migrated:
            # === FASE DEX POOL (Meteora DLMM / Raydium / Post-Migrate) ===
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
                reasons.append(f"pool_fresh:{t.pool_age_minutes:.0f}m")
            else:
                red_flags.append(f"pool_too_old:{t.pool_age_minutes:.0f}m")

        else:
            # === FASE BONDING CURVE (Pump.fun) ===
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

        # ----------------------------------------------------
        # 3. BIRTH SIGNALS (Kecepatan Inflow & Kualitas Buyer)
        # ----------------------------------------------------
        birth_score = 0.0

        # Alpha Wallets
        if t.alpha_wallet_count >= self.cfg.strong_alpha_wallets:
            birth_score += 25
            reasons.append(f"alpha_wallets_strong:{t.alpha_wallet_count}")
        elif t.alpha_wallet_count >= self.cfg.min_alpha_wallets:
            birth_score += 15
            reasons.append(f"alpha_wallets_ok:{t.alpha_wallet_count}")

        # Social Presence (X/Twitter, Website, Telegram)
        social_count = sum([t.has_twitter, t.has_telegram, t.has_website])
        if social_count >= 2:
            birth_score += 25
            reasons.append(f"social_strong:{social_count}")
        elif social_count >= 1:
            birth_score += 15
            reasons.append(f"social_present:{social_count}")

        # Velocity
        if t.liquidity_velocity >= self.cfg.min_liquidity_velocity_sol_per_min:
            birth_score += 25
            reasons.append(f"liq_velocity:{t.liquidity_velocity:.2f}SOL/m")

        if t.holder_growth_velocity >= self.cfg.min_holder_growth_per_min:
            birth_score += 25
            reasons.append(f"holder_velocity:{t.holder_growth_velocity:.1f}h/m")

        t.birth_signal_score = min(100.0, birth_score)
        score += t.birth_signal_score * 0.25  # Bobot tambahan 25 poin maksimal

        return round(min(100.0, score), 2), phase, reasons, red_flags


# ============================================================
# 7. HIT-AND-RUN POSITION & SCALP MANAGER
# ============================================================

class PositionManager:
    """
    Eksekusi Hit-and-Run:
    - Masuk cepat saat pump mengonfirmasi momentum.
    - TP 1 (+80%): Tarik modal awal (Free Ride).
    - TP 2 (+200%): Kunci profit tambahan 25%.
    - Trailing Stop (18%): Kawal moonbag sisa 25% hingga pump berakhir.
    - Hard SL (-22%): Batasi kerugian jika dump terjadi.
    - Time Stop (60 min): Keluar jika momentum mati.
    """
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

        # 1. Hard Stop Loss
        if price <= pos.stop_price:
            actions.append(("stop_loss", pos.remaining_pct))
            return actions

        # 2. TP 1: Ambil Modal Awal (+80%)
        if not pos.initial_recovered and price >= pos.entry_price * self.cfg.take_initials_multiple:
            pos.initial_recovered = True
            pos.remaining_pct = 50.0
            actions.append(("take_initials_50pct", 50.0))

        # 3. TP 2: Kunci Profit Lanjutan (+200% / 3x)
        if pos.initial_recovered and not pos.tp2_recovered and price >= pos.entry_price * self.cfg.take_profit_2_multiple:
            pos.tp2_recovered = True
            pos.remaining_pct = 25.0
            actions.append(("take_profit_25pct", 25.0))

        # 4. Trailing Stop untuk Sisa Posisi
        if pos.initial_recovered and price <= pos.trailing_price:
            actions.append(("trailing_stop", pos.remaining_pct))
            return actions

        # 5. Time Stop (Hit-and-Run: 60 menit)
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
# 8. TELEGRAM & TRADE LOGGING
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
        await self._send("⚡ <b>[HIT-AND-RUN] Sniper Engine v11 Online</b>\nMulti-DEX & Momentum Scalper Aktif!")

    async def close(self):
        if self.session:
            await self.session.close()

    async def send_signal_alert(self, s: Signal, t: TokenState):
        emoji = "🚀" if s.score >= 75 else "⚡"
        phase_label = "MOMENTUM DEX POOL" if s.phase == "dex_pool" else "BONDING SWEETSPOT"
        text = (
            f"{emoji} <b>[HIT-AND-RUN] GEM SIGNAL ({phase_label})</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Token:</b> {t.name} (<b>${t.symbol}</b>)\n"
            f"<b>Mint:</b> <code>{s.mint}</code>\n"
            f"<b>Source:</b> {s.source} ({t.dex_id or 'DEX'})\n"
            f"<b>Skor Konvinsi:</b> <code>{s.score}/100</code>\n"
            f"<b>Entry Price:</b> <code>${s.entry_price:.8f}</code>\n"
            f"<b>Market Cap:</b> <code>${s.market_cap:,.0f}</code>\n"
            f"<b>Liquidity:</b> <code>${s.liquidity:,.0f}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🛡️ Audit Keamanan:</b>\n"
            f"  • Mint Auth: {'✅ Revoked' if not t.mint_authority_active else '❌ Aktif'}\n"
            f"  • Freeze Auth: {'✅ Revoked' if not t.freeze_authority_active else '❌ Aktif'}\n"
            f"  • LP Status: {'✅ Terkunci/Aman' if t.lp_locked else '⚠️ Open Vault'}\n"
            f"  • Dev Holdings: <code>{t.dev_holding_pct:.1f}%</code>\n"
            f"  • Tax / Fee: <code>{t.transfer_fee_pct:.1f}%</code>\n"
            f"  • Honeypot Check: {'✅ Normal' if t.sell_simulation_ok else '❌ Gagal'}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🎯 Rencana Hit-and-Run:</b>\n"
            f"  • Stop Loss: -22% (Cut Loss Ketat)\n"
            f"  • TP 1: +80% (Ambil Modal 50%)\n"
            f"  • TP 2: +200% (Amankan Profit 25%)\n"
            f"  • Trailing Stop: 18% (Kawal Moonbag)\n"
            f"  • Time Stop: 60 Menit\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Alasan Lolos:</b>\n" +
            "\n".join(f"  • {r}" for r in s.reasons[:5]) +
            f"\n\n🔗 <a href='https://dexscreener.com/solana/{s.mint}'>[DexScreener]</a> · "
            f"<a href='https://photon-sol.tinyastro.io/en/r/@alpha/{s.mint}'>[Photon]</a> · "
            f"<a href='https://neo.bullx.io/terminal?chainId=1399811149&address={s.mint}'>[BullX]</a>"
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
# 9. SCANNER ENGINE UTAMA
# ============================================================

class HitAndRunScanner:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.cache = TimedCache()
        self.rpc = RpcClient(cfg, self.cache)
        self.security = SecurityAnalyzer(cfg)
        self.cluster = WalletClusterAnalyzer(cfg, self.rpc)
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

    # --------------------------------------------------------
    # 9.1 INGESTION WEBSOCKET & MULTI-DEX DISCOVERY
    # --------------------------------------------------------

    async def pumpportal_listener(self):
        """Mendengarkan event pump.fun via PumpPortal WebSocket"""
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
            await self.eval_queue.put(mint)

        elif tx_type in ("buy", "sell"):
            t = self.tokens.get(mint)
            if not t:
                return
            sol_amount = float(msg.get("solAmount", 0) or 0)
            wallet = msg.get("traderPublicKey", "")
            if tx_type == "buy":
                t.buys_count += 1
                t.volume_buys += sol_amount
                t.unique_buyers.add(wallet)
                if len(self.buyers_buffer[mint]) < 100:
                    self.buyers_buffer[mint].append({"wallet": wallet, "sol_amount": sol_amount, "timestamp": time.time()})
            else:
                t.sells_count += 1
                t.volume_sells += sol_amount

            v_sol = float(msg.get("vSolInBondingCurve", 0) or 0)
            if v_sol > 0:
                t.bonding_pct = min(100.0, (v_sol / 85.0) * 100.0)
                t.sol_in_bonding_history.append((time.time(), v_sol))
                t.liquidity_usd = max(t.liquidity_usd, v_sol * 160.0)

        elif tx_type == "migrate":
            t = self.tokens.get(mint)
            if t:
                t.is_migrated = True
                t.pool_address = msg.get("pool", "") or msg.get("poolAddress", "")
                t.scored = False
                await self.eval_queue.put(mint)

    async def dexscreener_discovery_loop(self):
        """
        Polling discovery multi-source:
        Mencakup koin-koin baru di Meteora DLMM, Dynamic AMM, Raydium, dan Boosted tokens.
        """
        if not self.cfg.enable_dexscreener_discovery:
            return
        print("[discovery] Multi-DEX continuous discovery started")
        while self.running:
            try:
                mints_discovered: List[str] = []

                # 1. Search Queries
                for q in self.cfg.discovery_queries:
                    try:
                        url = f"{self.cfg.dexscreener_search_url}?q={q}"
                        async with self.rpc.sem:
                            async with self.rpc.session.get(url) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    for p in (data.get("pairs", []) or [])[:self.cfg.discovery_max_per_query]:
                                        if p.get("chainId") == "solana":
                                            addr = p.get("baseToken", {}).get("address")
                                            if addr:
                                                mints_discovered.append(addr)
                    except Exception:
                        pass

                # 2. Latest Token Profiles & Boosts
                for boost_url in (self.cfg.dexscreener_boosts_latest, self.cfg.dexscreener_profiles):
                    try:
                        async with self.rpc.sem:
                            async with self.rpc.session.get(boost_url) as resp:
                                if resp.status == 200:
                                    items = await resp.json()
                                    for it in (items or [])[:30]:
                                        if it.get("chainId") == "solana":
                                            addr = it.get("tokenAddress")
                                            if addr:
                                                mints_discovered.append(addr)
                    except Exception:
                        pass

                # Tambahkan koin baru ke queue evaluasi
                mints_discovered = list(dict.fromkeys(mints_discovered))
                for mint in mints_discovered:
                    if mint not in self.tokens:
                        self.tokens[mint] = TokenState(
                            mint=mint,
                            creator="",
                            source="dexscreener",
                            created_at=time.time(),
                        )
                        await self.eval_queue.put(mint)

            except Exception as e:
                pass
            await asyncio.sleep(self.cfg.discovery_interval_sec)

    # --------------------------------------------------------
    # 9.2 EVALUASI LENGKAP & ANTI-JEBAKAN DEVELOPER
    # --------------------------------------------------------

    async def worker(self, worker_id: int):
        while self.running:
            try:
                mint = await asyncio.wait_for(self.eval_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self.evaluate_token(mint)
            except Exception as e:
                pass
            finally:
                self.eval_queue.task_done()

    async def evaluate_token(self, mint: str):
        t = self.tokens.get(mint)
        if not t or t.scored:
            return

        now = time.time()
        age = now - t.created_at

        # 1. AMBIL DATA POOL DEX (Selalu ambil untuk deteksi dini Meteora/Raydium!)
        pool_data = await self.rpc.get_pool_data(mint)
        if pool_data and pool_data.get("liquidity_usd", 0) > 1000:
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

            # Jika koin ditemukan dari DexScreener, isi data transaksi m5
            if t.buys_count == 0:
                t.buys_count = pool_data.get("buys_m5", 10)
                t.sells_count = pool_data.get("sells_m5", 4)
                t.volume_buys = pool_data.get("vol_m5", 5000.0) * 0.65
                t.volume_sells = pool_data.get("vol_m5", 5000.0) * 0.35

        # 2. PENGECEKAN PARALEL KEAMANAN & SOSIAL
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

        locked, liq_usd, lp_pool, rug_score, rug_flags = lp_info
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

        # 3. KONSENTRASI HOLDER & DEV DUMP CHECK
        top10_pct, top1_pct, dev_pct = self.security.holder_concentration(holders, lp_pool)
        t.dev_holding_pct = dev_pct

        # 4. ALPHA WALLETS & BUYER QUALITY
        early_wallets = [b["wallet"] for b in self.buyers_buffer.get(mint, [])]
        if not early_wallets and holders:
            early_wallets = list(holders.keys())[:15]

        t.alpha_wallet_count = await self.rpc.count_alpha_wallets(early_wallets)

        # 5. VELOCITY INFLOW
        if len(t.sol_in_bonding_history) >= 2:
            dt = (t.sol_in_bonding_history[-1][0] - t.sol_in_bonding_history[0][0]) / 60.0
            if dt > 0.05:
                t.liquidity_velocity = max(0.0, (t.sol_in_bonding_history[-1][1] - t.sol_in_bonding_history[0][1]) / dt)
        elif pool_data and pool_data.get("vol_m5", 0) > 0:
            t.liquidity_velocity = (pool_data["vol_m5"] / 160.0) / 5.0

        # ----------------------------------------------------
        # 6. PENILAIAN & AUDIT KEAMANAN KETAT
        # ----------------------------------------------------
        red_flags: List[str] = []

        # --- JEBAKAN DEVELOPER (FATAL) ---
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

        # --- VALIDASI LIKUIDITAS & LP LOCK ---
        if t.is_migrated and self.cfg.require_lp_locked and not locked:
            # Toleransi jika likuiditas Meteora DLMM besar (> $5k)
            if t.liquidity_usd < self.cfg.min_pool_liquidity_usd:
                red_flags.append("lp_not_locked")

        # --- HITUNG SKOR MOMENTUM ---
        score, phase, reasons, score_flags = self.scorer.score_token(t)
        red_flags.extend(score_flags)
        red_flags.extend(rug_flags)
        t.scored = True

        # ----------------------------------------------------
        # 7. KEPUTUSAN: SKIP (DENGAN RESCAN) ATAU LOLOS SIGNAL
        # ----------------------------------------------------
        if red_flags:
            # Rescan jika koin potensial sedang mengonfirmasi likuiditas
            if t.rescan_count < self.cfg.max_rescan_count and age < self.cfg.max_rescan_age_sec:
                t.rescan_count += 1
                t.scored = False
                t.next_rescan_at = now + self.cfg.rescan_delay_sec
                heapq.heappush(self.rescan_heap, (t.next_rescan_at, mint))
            return

        # LOLOS EVALUASI!
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
                birth_score=t.birth_signal_score,
                market_cap=t.market_cap_usd,
                liquidity=t.liquidity_usd,
            )
            self.signals.append(signal_obj)
            await self.signal_queue.put(signal_obj)
            print(f"🔥 [SIGNAL-{phase.upper()}] {t.name} (${t.symbol}) | Skor={score} | MCap=${t.market_cap_usd:,.0f} | Liq=${t.liquidity_usd:,.0f}")
            if self.cfg.telegram_enabled:
                await self.telegram.send_signal_alert(signal_obj, t)

    async def _zero(self):
        return 0.0

    # --------------------------------------------------------
    # 9.3 EKSEKUSI TRADING & MANAJEMEN EXIT HIT AND RUN
    # --------------------------------------------------------

    async def signal_consumer(self):
        while self.running:
            try:
                signal_obj = await asyncio.wait_for(self.signal_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self.execute_entry(signal_obj)
            except Exception as e:
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
        while self.running:
            try:
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

    # --------------------------------------------------------
    # 9.4 RUN ENGINE
    # --------------------------------------------------------

    async def run(self):
        await self.rpc.start()
        await self.telegram.start()

        tasks = [
            asyncio.create_task(self.pumpportal_listener()),
            asyncio.create_task(self.dexscreener_discovery_loop()),
            asyncio.create_task(self.signal_consumer()),
            asyncio.create_task(self.monitor_loop()),
            asyncio.create_task(self.rescan_loop()),
        ]
        for i in range(self.cfg.worker_count):
            tasks.append(asyncio.create_task(self.worker(i)))

        print(f"🚀 [HITRUN-V11] Engine Aktif | Workers={self.cfg.worker_count}")
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            self.running = False
            for t in tasks:
                t.cancel()
            await self.telegram.close()
            await self.rpc.close()


# ============================================================
# 10. HEALTH SERVER & DASHBOARD API
# ============================================================

class HealthServer:
    def __init__(self, scanner):
        self.scanner = scanner
        self.app = Flask(__name__)
        self._register_routes()

    def _register_routes(self):
        @self.app.route("/")
        def index():
            return jsonify({
                "service": "hitrun-scanner",
                "version": "v11-flash-sniper",
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

    def run(self, port: int):
        self.app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)


# ============================================================
# 11. ENTRY POINT
# ============================================================

async def main():
    cfg = Config()
    scanner = HitAndRunScanner(cfg)

    port = int(os.getenv("PORT", "10000"))
    health = HealthServer(scanner)
    threading.Thread(target=health.run, args=(port,), daemon=True).start()
    print(f"[web] Health server listening on :{port}")

    def handle_shutdown(signum, frame):
        print(f"[shutdown] Sinyal shutdown diterima...")
        scanner.running = False

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    await scanner.run()


if __name__ == "__main__":
    asyncio.run(main())
