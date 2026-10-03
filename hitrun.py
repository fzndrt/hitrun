# ============================================================
# HITRUN.PY — Hit-and-Run Scanner v10 (Birth Signals)
# ============================================================
# v10 menambahkan 5 sinyal kelahiran untuk deteksi dini:
#  1. Alpha wallet count (kualitas 20 wallet pertama)
#  2. Bundle detection di creation block
#  3. Liquidity velocity (SOL/menit)
#  4. Holder growth velocity (holder/menit)
#  5. Social presence (Twitter/Telegram/Website)
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
# 1. KONFIGURASI
# ============================================================

@dataclass
class Config:
    # --- Jendela observasi & rescan ---
    observation_window_sec: int = 25
    min_age_sec: int = 3
    enable_rescan: bool = True
    rescan_delay_sec: int = 30
    max_rescan_count: int = 20
    max_rescan_age_sec: int = 21600

    # --- Filter harga ---
    max_price_pump_5m_pct: float = 60.0
    min_price_pump_5m_pct: float = 2.0

    # --- Filter bonding curve ---
    min_bonding_pct: float = 2.0
    max_bonding_pct: float = 40.0

    # --- Post-graduation ---
    enable_post_migration: bool = True
    min_market_cap_usd: float = 15_000
    max_market_cap_usd: float = 5_000_000
    max_pool_age_minutes: int = 10080
    min_pool_liquidity_usd: float = 8_000
    enable_post_migration_rescan: bool = True

    # --- Likuiditas & holder ---
    min_liquidity_usd: float = 5_000
    max_top10_holder_pct: float = 28.0
    max_top1_holder_pct: float = 4.0
    max_dev_holding_pct: float = 5.0
    min_holders_count: int = 10
    require_lp_locked: bool = True
    min_lp_locked_pct: float = 90.0
    max_rugcheck_score: float = 65.0

    # --- Authority & metadata ---
    require_mint_authority_revoked: bool = True
    require_freeze_authority_revoked: bool = True
    require_metadata_immutable: bool = False

    # --- Tax / fee ---
    max_transfer_fee_pct: float = 5.0
    max_buy_tax_pct: float = 8.0

    # --- Sell simulation ---
    enable_sell_simulation: bool = True
    min_sell_recovery_pct: float = 60.0

    # --- Creator reputation ---
    max_creator_rugpull_count: int = 0

    # --- Micro wallet / bot farm ---
    max_micro_wallet_count: int = 25
    micro_wallet_balance_sol: float = 0.02

    # --- BUYER QUALITY ---
    enable_buyer_quality_check: bool = True
    min_real_buyers: int = 3
    max_bot_ratio: float = 0.6
    buyer_age_threshold_hours: float = 24.0
    buyer_balance_min_sol: float = 0.05
    max_early_buyers_check: int = 30

    # --- Wallet cluster ---
    enable_wallet_cluster_check: bool = True
    fresh_wallet_max_age_hours: int = 24
    max_fresh_wallet_ratio: float = 0.6
    balance_similarity_tolerance: float = 0.1
    min_similar_balance_wallets: int = 5
    max_similar_balance_wallets: int = 12

    # --- Bundle & sniper ---
    max_bundle_wallets: int = 4
    max_sniper_wallets: int = 8
    sniper_window_sec: int = 10

    # --- PUMP.FUN TRAP ---
    enable_dev_sell_check: bool = True
    enable_migration_sniper_check: bool = True
    max_dev_sell_pct: float = 0.0
    max_migration_snipers: int = 5

    # ========================================================
    # --- BIRTH SIGNALS (v10) ---
    # ========================================================
    enable_birth_signal_check: bool = True
    birth_signal_min_score: float = 20.0
    birth_signal_strong_score: float = 50.0

    # Sinyal 1: Alpha wallet (umur wallet, bukan bot baru)
    alpha_wallet_min_age_hours: float = 720.0        # > 30 hari = wallet lama
    alpha_wallet_min_balance_sol: float = 0.5        # punya cukup SOL
    min_alpha_wallets: int = 2                       # minimal 2 alpha di 20 buyer pertama
    strong_alpha_wallets: int = 4

    # Sinyal 2: Bundle di creation block
    detect_creation_block_bundle: bool = True
    max_creation_block_buyers: int = 3               # >3 di blok kreasi = bundle

    # Sinyal 3: Liquidity velocity (SOL/menit)
    liquidity_velocity_window_sec: int = 300         # window 5 menit
    min_liquidity_velocity_sol_per_min: float = 0.3  # 0.3 SOL/menit minimum
    strong_liquidity_velocity_sol_per_min: float = 1.5

    # Sinyal 4: Holder growth velocity (holder/menit)
    holder_growth_window_sec: int = 300
    min_holder_growth_per_min: float = 0.5
    strong_holder_growth_per_min: float = 3.0

    # Sinyal 5: Social presence
    check_social_presence: bool = True
    social_presence_weight: float = 15.0
    require_at_least_one_social: bool = False        # jangan wajib, tapi bonus

    # --- Skor minimum ---
    min_conviction_score: float = 60.0

    # --- Manajemen posisi ---
    position_size_pct: float = 2.5
    max_daily_exposure_pct: float = 20.0
    max_loss_streak: int = 3

    # --- Exit plan ---
    hard_stop_loss_pct: float = 35.0
    take_initials_multiple: float = 2.0
    trailing_stop_pct: float = 25.0
    time_stop_hours: int = 24

    # --- Multi-source WS ---
    pumpportal_ws: str = "wss://pumpportal.fun/api/data"
    raydium_ws: str = "wss://api.raydium.io/v2/ws"
    meteora_ws: str = "wss://meteora.ag/ws"
    enable_raydium: bool = True
    enable_meteora: bool = True

    # --- DEXSCREENER DISCOVERY ---
    enable_dexscreener_discovery: bool = True
    discovery_interval_sec: int = 25
    discovery_queries: Tuple[str, ...] = (
        "SOL", "PUMP", "RAY", "METEORA", "USDC",
        "AI", "TRUMP", "DOGE", "PEPE", "CAT",
        "BONK", "WIF", "MEME", "MOON", "SHIB",
        "BABY", "FROG", "ELON", "MUSK", "BIDEN",
        "GEM", "COIN", "TOKEN", "CRYPTO", "SOLANA",
    )
    discovery_max_per_query: int = 50

    # --- WATCHLIST ---
    enable_watchlist: bool = True
    watchlist_mints: Tuple[str, ...] = ()

    # --- PUMP.FUN API ---
    enable_pumpfun_api_discovery: bool = True
    pumpfun_api_url: str = (
        "https://frontend-api-v3.pump.fun/coins"
        "?offset=0&limit=50&sort=created&order=DESC&includeNsfw=false"
    )
    pumpfun_coin_url: str = "https://frontend-api-v3.pump.fun/coins/"

    # --- NEW POOLS ---
    enable_new_pools_discovery: bool = True
    new_pools_interval_sec: int = 20
    dexscreener_new_pools_url: str = (
        "https://api.dexscreener.com/token-profiles/latest/v1"
    )

    # --- GECKOTERMINAL ---
    enable_geckoterminal_discovery: bool = True
    geckoterminal_new_pools_url: str = (
        "https://api.geckoterminal.com/api/v2/networks/solana/new_pools"
    )

    # --- SOLANA TRACKER ---
    enable_solanatracker_discovery: bool = True
    solanatracker_trending_url: str = (
        "https://data.solanatracker.io/tokens/trending"
    )

    # --- RPC LOGS ---
    enable_rpc_logs_discovery: bool = True
    pumpfun_program_id: str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

    # --- SWEEP ---
    enable_sweep: bool = True
    sweep_interval_sec: int = 30

    # --- MIGRATION POLLER ---
    enable_migration_poller: bool = True
    migration_poll_interval_sec: int = 45
    migration_poll_track_hours: int = 24
    migration_poll_min_bonding: float = 80.0

    # --- TOKEN-2022 ---
    enable_token2022_rpc_check: bool = True
    token2022_program_id: str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
    token_program_id: str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

    # --- Endpoint GRATIS ---
    threews_api_url: str = "https://three.ws/api/crypto"
    holder_rpc_url: str = "https://rpc.magicblock.app/mainnet"
    public_rpc_url: str = "https://solana-rpc.publicnode.com"
    rugcheck_url: str = "https://api.rugcheck.xyz/v1"
    jupiter_quote_url: str = "https://lite-api.jup.ag/swap/v1/quote"
    dexscreener_url: str = "https://api.dexscreener.com/latest/dex/tokens"
    dexscreener_search_url: str = "https://api.dexscreener.com/latest/dex/search"
    dexscreener_boosts_latest: str = "https://api.dexscreener.com/token-boosts/latest/v1"
    dexscreener_boosts_top: str = "https://api.dexscreener.com/token-boosts/top/v1"
    dexscreener_profiles: str = "https://api.dexscreener.com/token-profiles/latest/v1"
    raydium_pools_url: str = (
        "https://api-v3.raydium.io/pools/info/list?"
        "poolType=all&poolSortField=default&sortType=desc&pageSize=30&page=1"
    )

    # --- Worker & cache ---
    worker_count: int = 20
    rpc_semaphore: int = 20
    evaluate_interval_sec: float = 1.0
    monitor_interval_sec: float = 2.0
    cache_ttl_holders_sec: int = 15
    cache_ttl_lp_sec: int = 30
    cache_ttl_supply_sec: int = 60
    cache_ttl_security_sec: int = 45
    cache_ttl_wallet_age_sec: int = 600
    cache_ttl_buyer_quality_sec: int = 120
    cache_ttl_social_sec: int = 300

    # --- Funding graph ---
    funding_window_hours: int = 72
    max_cluster_pct: float = 30.0
    max_cluster_size: int = 6
    max_clustered_wallets: int = 12
    max_clusters: int = 3

    # --- Telegram ---
    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", "")
    )
    telegram_enabled: bool = True

    # --- Portfolio & logging ---
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
    buyers: List[dict] = field(default_factory=list)
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

    is_migrated: bool = False
    pool_address: str = ""
    market_cap_usd: float = 0.0
    pool_age_minutes: float = 0.0
    migration_detected_at: float = 0.0
    migration_snipers: int = 0

    dev_sold_pct: float = 0.0

    is_token_2022: bool = False
    is_from_dexscreener_pool: bool = False

    rescan_count: int = 0
    next_rescan_at: float = 0.0

    # --- BIRTH SIGNALS (v10) ---
    liquidity_history: deque = field(default_factory=lambda: deque(maxlen=600))
    holder_history: deque = field(default_factory=lambda: deque(maxlen=600))
    sol_in_bonding_history: deque = field(default_factory=lambda: deque(maxlen=600))

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
    score: float
    entry_price: float
    reasons: List[str]
    red_flags: List[str]
    timestamp: float
    source: str = ""
    phase: str = "bonding"
    birth_score: float = 0.0


@dataclass
class Position:
    mint: str
    entry_price: float
    size_usd: float
    remaining_pct: float = 100.0
    initial_recovered: bool = False
    high_water: float = 0.0
    opened_at: float = 0.0
    stop_price: float = 0.0
    trailing_price: float = 0.0


# ============================================================
# 3. CACHE
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
        expired = [k for k, (ts, _) in self._store.items() if now - ts > 600]
        for k in expired:
            self._store.pop(k, None)


# ============================================================
# 4. RPC CLIENT
# ============================================================

class RpcClient:
    def __init__(self, cfg: Config, cache: TimedCache):
        self.cfg = cfg
        self.cache = cache
        self.session: Optional[aiohttp.ClientSession] = None
        self.sem = asyncio.Semaphore(cfg.rpc_semaphore)

    async def start(self):
        timeout = aiohttp.ClientTimeout(total=10)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self):
        if self.session:
            await self.session.close()

    # ---------- HOLDERS ----------

    async def get_holders(self, mint: str) -> Dict[str, float]:
        cached = self.cache.get(f"holders:{mint}", self.cfg.cache_ttl_holders_sec)
        if cached is not None:
            return cached

        holders = await self._holders_threews(mint)
        if not holders:
            holders = await self._holders_rpc(mint, self.cfg.holder_rpc_url)
        if not holders:
            holders = await self._holders_rpc(mint, self.cfg.public_rpc_url)
        if holders:
            self.cache.set(f"holders:{mint}", holders)
        return holders

    async def _holders_threews(self, mint: str) -> Dict[str, float]:
        url = f"{self.cfg.threews_api_url}/holders?address={mint}"
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
        cached = self.cache.get(f"supply:{mint}", self.cfg.cache_ttl_supply_sec)
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

    # ---------- WALLET ----------

    async def get_wallet_age_hours(self, wallet: str) -> float:
        cached = self.cache.get(f"wage:{wallet}", self.cfg.cache_ttl_wallet_age_sec)
        if cached is not None:
            return cached

        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getSignaturesForAddress",
            "params": [wallet, {"limit": 1000}],
        }
        async with self.sem:
            try:
                async with self.session.post(
                    self.cfg.holder_rpc_url, json=payload
                ) as resp:
                    data = await resp.json()
                sigs = data.get("result", []) or []
                if not sigs:
                    age = 0.0
                else:
                    earliest = min(
                        (s.get("blockTime", 0) or 0) for s in sigs
                    )
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
                async with self.session.post(
                    self.cfg.holder_rpc_url, json=payload
                ) as resp:
                    data = await resp.json()
                lamports = data.get("result", {}).get("value", 0)
                bal = lamports / 1_000_000_000
            except Exception:
                bal = 999.0
        self.cache.set(f"wbal:{wallet}", bal)
        return bal

    async def get_signatures(self, address: str, limit: int = 50) -> list:
        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getSignaturesForAddress",
            "params": [address, {"limit": limit}],
        }
        async with self.sem:
            try:
                async with self.session.post(
                    self.cfg.holder_rpc_url, json=payload
                ) as resp:
                    data = await resp.json()
                return data.get("result", [])
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
                async with self.session.post(
                    self.cfg.holder_rpc_url, json=payload
                ) as resp:
                    data = await resp.json()
                return data.get("result")
            except Exception:
                return None

    # ---------- BIRTH SIGNAL: ALPHA WALLET ----------

    async def count_alpha_wallets(self, wallets: List[str]) -> int:
        """
        Hitung wallet 'alpha' dari daftar buyer awal.
        Heuristik: umur wallet > 30 hari DAN saldo > 0.5 SOL.
        """
        if not wallets:
            return 0
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

    # ---------- BIRTH SIGNAL: CREATION BLOCK BUNDLE ----------

    async def detect_creation_block_bundle(
        self, mint: str, creator: str, created_at: float
    ) -> int:
        """
        Deteksi jumlah buyer di blok yang sama dengan creation block.
        Return: jumlah buyer di creation block (creator dikecualikan).
        """
        if not self.cfg.detect_creation_block_bundle:
            return 0

        cached = self.cache.get(f"cbundle:{mint}", 300)
        if cached is not None:
            return cached

        # Ambil signature paling awal dari mint account
        sigs = await self.get_signatures(mint, limit=5)
        if not sigs:
            self.cache.set(f"cbundle:{mint}", 0)
            return 0

        # Cari signature creation (paling tua)
        creation_sig = None
        for s in sigs:
            bt = s.get("blockTime", 0) or 0
            if abs(bt - created_at) < 60:
                creation_sig = s.get("signature")
                break

        if not creation_sig:
            self.cache.set(f"cbundle:{mint}", 0)
            return 0

        # Ambil transaksi creation
        tx = await self.get_transaction(creation_sig)
        if not tx or not tx.get("meta"):
            self.cache.set(f"cbundle:{mint}", 0)
            return 0

        creation_slot = tx.get("slot", 0)
        if creation_slot <= 0:
            self.cache.set(f"cbundle:{mint}", 0)
            return 0

        # Hitung wallet unik yang melakukan pembelian di slot yang sama
        # Heuristik: cek pre/post token balances
        pre = tx["meta"].get("preTokenBalances", []) or []
        post = tx["meta"].get("postTokenBalances", []) or []
        buyers = set()
        for pb in post:
            if pb.get("mint") == mint:
                owner = pb.get("owner", "")
                if owner and owner != creator:
                    buyers.add(owner)

        count = len(buyers)
        self.cache.set(f"cbundle:{mint}", count)
        return count

    # ---------- BIRTH SIGNAL: SOCIAL PRESENCE ----------

    async def check_social_presence(self, mint: str) -> Tuple[bool, bool, bool]:
        """
        Cek apakah koin punya Twitter, Telegram, Website via pump.fun API.
        Return: (has_twitter, has_telegram, has_website)
        """
        if not self.cfg.check_social_presence:
            return False, False, False

        cached = self.cache.get(f"social:{mint}", self.cfg.cache_ttl_social_sec)
        if cached is not None:
            return cached

        url = f"{self.cfg.pumpfun_coin_url}{mint}"
        async with self.sem:
            try:
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        result = (False, False, False)
                        self.cache.set(f"social:{mint}", result)
                        return result
                    data = await resp.json()
            except Exception:
                result = (False, False, False)
                self.cache.set(f"social:{mint}", result)
                return result

        has_twitter = bool(data.get("twitter"))
        has_telegram = bool(data.get("telegram"))
        has_website = bool(data.get("website"))
        result = (has_twitter, has_telegram, has_website)
        self.cache.set(f"social:{mint}", result)
        return result

    # ---------- DEV & SECURITY ----------

    async def get_developer_holdings(
        self, mint: str, creator: str, lp_pool: str
    ) -> Tuple[float, bool]:
        holders = await self.get_holders(mint)
        dev_pct = 0.0
        for wallet, pct in holders.items():
            if wallet == creator:
                dev_pct = pct
                break
        return dev_pct, dev_pct > 5.0

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

    async def get_token_authorities(self, mint: str) -> Tuple[bool, bool]:
        if not self.cfg.enable_token2022_rpc_check:
            report = await self.get_security_report(mint)
            token = report.get("token", {}) or {}
            return bool(token.get("mintAuthority")), bool(token.get("freezeAuthority"))

        cached = self.cache.get(f"auth:{mint}", 30)
        if cached is not None:
            return cached

        for program_id in (self.cfg.token_program_id, self.cfg.token2022_program_id):
            result = await self._get_account_info_authority(mint, program_id)
            if result is not None:
                self.cache.set(f"auth:{mint}", result)
                return result

        report = await self.get_security_report(mint)
        token = report.get("token", {}) or {}
        result = (bool(token.get("mintAuthority")), bool(token.get("freezeAuthority")))
        self.cache.set(f"auth:{mint}", result)
        return result

    async def _get_account_info_authority(
        self, mint: str, program_id: str
    ) -> Optional[Tuple[bool, bool]]:
        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getAccountInfo",
            "params": [
                mint,
                {"encoding": "jsonParsed", "commitment": "confirmed"},
            ],
        }
        async with self.sem:
            try:
                async with self.session.post(
                    self.cfg.holder_rpc_url, json=payload
                ) as resp:
                    data = await resp.json()
            except Exception:
                return None

        value = (data.get("result") or {}).get("value")
        if not value:
            return None
        owner = value.get("owner", "")
        if owner != program_id:
            return None

        parsed = (value.get("data") or {}).get("parsed") or {}
        info = parsed.get("info") or {}
        mint_auth = info.get("mintAuthority")
        freeze_auth = info.get("freezeAuthority")
        return (bool(mint_auth), bool(freeze_auth))

    async def is_token_2022(self, mint: str) -> bool:
        payload = {
            "jsonrpc": "2.0", "id": "1",
            "method": "getAccountInfo",
            "params": [mint, {"encoding": "jsonParsed"}],
        }
        async with self.sem:
            try:
                async with self.session.post(
                    self.cfg.holder_rpc_url, json=payload
                ) as resp:
                    data = await resp.json()
            except Exception:
                return False
        value = (data.get("result") or {}).get("value")
        if not value:
            return False
        return value.get("owner", "") == self.cfg.token2022_program_id

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
            and ("creator" in r.get("name", "").lower()
                 or "rug" in r.get("name", "").lower())
        )
        return rugpull_count, 1

    async def get_micro_wallet_count(self, mint: str) -> int:
        holders = await self.get_holders(mint)
        if not holders:
            return 0
        top_wallets = list(holders.keys())[:20]
        balances = await asyncio.gather(
            *[self.get_wallet_balance_sol(w) for w in top_wallets]
        )
        return sum(1 for b in balances if b < self.cfg.micro_wallet_balance_sol)

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
                    if resp.status != 200:
                        return False
                    data = await resp.json()
                    return float(data.get("outAmount", 0) or 0) > 0
            except Exception:
                return True

    # ---------- BUYER QUALITY ----------

    async def analyze_buyer_quality(
        self, buyers: List[dict]
    ) -> Tuple[int, int, float]:
        cached = self.cache.get(
            f"bq:{id(buyers)}", self.cfg.cache_ttl_buyer_quality_sec
        )
        if cached is not None:
            return cached

        if not buyers:
            return 0, 0, 0.0

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
            return 0, 0, 0.0

        ages, balances = await asyncio.gather(
            asyncio.gather(*[self.get_wallet_age_hours(w) for w in wallets]),
            asyncio.gather(*[self.get_wallet_balance_sol(w) for w in wallets]),
        )

        bot_count = 0
        for age, bal in zip(ages, balances):
            is_fresh = age < self.cfg.buyer_age_threshold_hours
            is_micro = bal < self.cfg.buyer_balance_min_sol
            if is_fresh or is_micro:
                bot_count += 1

        real_count = len(wallets) - bot_count
        bot_ratio = bot_count / len(wallets) if wallets else 0.0
        result = (real_count, bot_count, bot_ratio)
        self.cache.set(f"bq:{id(buyers)}", result)
        return result

    # ---------- PUMP.FUN TRAP ----------

    async def check_dev_sold(
        self, mint: str, creator: str, created_at: float
    ) -> float:
        if not self.cfg.enable_dev_sell_check or not creator:
            return 0.0

        cached = self.cache.get(f"devsell:{mint}", 60)
        if cached is not None:
            return cached

        sigs = await self.get_signatures(creator, limit=30)
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
                pre_amt = 0.0
                post_amt = 0.0
                for pb in pre:
                    if pb.get("mint") == mint and pb.get("owner") == creator:
                        pre_amt = float(pb.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)
                for pb in post:
                    if pb.get("mint") == mint and pb.get("owner") == creator:
                        post_amt = float(pb.get("uiTokenAmount", {}).get("uiAmount", 0) or 0)
                if pre_amt > post_amt:
                    sold_pct += (pre_amt - post_amt) / max(pre_amt, 1.0) * 100
            except Exception:
                continue

        sold_pct = min(100.0, sold_pct)
        self.cache.set(f"devsell:{mint}", sold_pct)
        return sold_pct

    # ---------- LP & POOL ----------

    async def get_lp_info(
        self, mint: str
    ) -> Tuple[bool, float, str, float, List[str]]:
        cached = self.cache.get(f"lp:{mint}", self.cfg.cache_ttl_lp_sec)
        if cached is not None:
            return cached

        report = await self.get_security_report(mint)
        if not report:
            result = (False, 0.0, "", 100.0, ["rugcheck_unavailable"])
            self.cache.set(f"lp:{mint}", result)
            return result

        score = float(report.get("score_normalised", 100.0))
        lp_locked_pct = float(report.get("lpLockedPct", 0.0) or 0.0)
        lp_locked = lp_locked_pct >= self.cfg.min_lp_locked_pct

        liquidity_usd = 0.0
        for r in report.get("risks", []) or []:
            if "liquidity" in (r.get("name", "") or "").lower():
                try:
                    liquidity_usd = float(
                        str(r.get("value", "0")).replace("$", "").replace(",", "")
                    )
                except ValueError:
                    pass

        red_flags: List[str] = []
        for r in report.get("risks", []) or []:
            level = r.get("level", "")
            name = r.get("name", "")
            if level == "danger":
                red_flags.append(f"rugcheck_danger:{name}")
            elif level == "warn" and "liquidity" in name.lower():
                red_flags.append(f"rugcheck_warn:{name}")

        lp_pool = ""
        markets = report.get("markets", []) or []
        if markets:
            lp_pool = markets[0].get("pubkey", "")

        result = (lp_locked, liquidity_usd, lp_pool, score, red_flags)
        self.cache.set(f"lp:{mint}", result)
        return result

    async def get_pool_data(self, mint: str) -> dict:
        cached = self.cache.get(f"pool:{mint}", 30)
        if cached is not None:
            return cached

        url = f"{self.cfg.dexscreener_url}/{mint}"
        async with self.sem:
            try:
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        empty = {}
                        self.cache.set(f"pool:{mint}", empty)
                        return empty
                    data = await resp.json()
            except Exception:
                empty = {}
                self.cache.set(f"pool:{mint}", empty)
                return empty

        pairs = data.get("pairs", []) or []
        if not pairs:
            empty = {}
            self.cache.set(f"pool:{mint}", empty)
            return empty

        best = max(
            pairs,
            key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0),
        )
        result = {
            "market_cap": float(best.get("marketCap", 0) or best.get("fdv", 0) or 0),
            "liquidity_usd": float(best.get("liquidity", {}).get("usd", 0) or 0),
            "pool_address": best.get("pairAddress", ""),
            "price": float(best.get("priceUsd", 0) or 0),
            "created_at": float(best.get("pairCreatedAt", 0) or 0) / 1000.0,
            "buys_m5": int(best.get("txns", {}).get("m5", {}).get("buys", 0)),
            "buys_h1": int(best.get("txns", {}).get("h1", {}).get("buys", 0)),
            "vol_h1": float(best.get("volume", {}).get("h1", 0) or 0),
        }
        self.cache.set(f"pool:{mint}", result)
        return result

    # ---------- DISCOVERY TAMBAHAN ----------

    async def get_new_pools_from_dexscreener(self) -> List[str]:
        mints: List[str] = []
        try:
            url = self.cfg.dexscreener_new_pools_url
            async with self.sem:
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        return mints
                    items = await resp.json()
            for it in (items or [])[:80]:
                if it.get("chainId") == "solana":
                    addr = it.get("tokenAddress")
                    if addr:
                        mints.append(addr)
        except Exception:
            pass
        return mints

    async def get_pumpfun_recent_coins(self) -> List[str]:
        mints: List[str] = []
        try:
            url = self.cfg.pumpfun_api_url
            async with self.sem:
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        return mints
                    items = await resp.json()
            if not isinstance(items, list):
                return mints
            for it in items:
                addr = it.get("mint") or it.get("address")
                if addr:
                    mints.append(addr)
        except Exception:
            pass
        return mints

    async def get_geckoterminal_new_pools(self) -> List[str]:
        mints: List[str] = []
        try:
            async with self.sem:
                async with self.session.get(
                    self.cfg.geckoterminal_new_pools_url,
                    headers={"Accept": "application/json;version=20230302"},
                ) as resp:
                    if resp.status != 200:
                        return mints
                    data = await resp.json()
            for p in (data.get("data") or [])[:80]:
                rels = p.get("relationships") or {}
                base = (rels.get("base_token") or {}).get("data") or {}
                addr = base.get("id", "")
                if "_" in addr:
                    addr = addr.split("_", 1)[1]
                if addr:
                    mints.append(addr)
        except Exception:
            pass
        return mints

    async def get_solanatracker_trending(self) -> List[str]:
        mints: List[str] = []
        try:
            async with self.sem:
                async with self.session.get(
                    self.cfg.solanatracker_trending_url
                ) as resp:
                    if resp.status != 200:
                        return mints
                    data = await resp.json()
            items = data if isinstance(data, list) else data.get("tokens", [])
            for it in items[:80]:
                addr = it.get("mint") or it.get("address") or it.get("tokenAddress")
                if addr:
                    mints.append(addr)
        except Exception:
            pass
        return mints

    async def get_pumpfun_new_tokens_rpc(self) -> List[str]:
        mints: List[str] = []
        try:
            payload = {
                "jsonrpc": "2.0", "id": "1",
                "method": "getSignaturesForAddress",
                "params": [
                    self.cfg.pumpfun_program_id,
                    {"limit": 30},
                ],
            }
            async with self.sem:
                async with self.session.post(
                    self.cfg.holder_rpc_url, json=payload
                ) as resp:
                    data = await resp.json()
            sigs = data.get("result", []) or []
            for s in sigs[:15]:
                sig = s.get("signature")
                if not sig:
                    continue
                tx = await self.get_transaction(sig)
                if not tx:
                    continue
                try:
                    msg = tx["transaction"]["message"]
                    for key in msg.get("accountKeys", []):
                        pub = key.get("pubkey") if isinstance(key, dict) else key
                        if pub and pub.endswith("pump"):
                            mints.append(pub)
                            break
                except Exception:
                    continue
        except Exception:
            pass
        return mints

    async def check_migration_status(self, mint: str) -> Optional[dict]:
        cached = self.cache.get(f"migstat:{mint}", 45)
        if cached is not None:
            return cached if cached != {} else None

        url = f"{self.cfg.dexscreener_url}/{mint}"
        async with self.sem:
            try:
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        self.cache.set(f"migstat:{mint}", {})
                        return None
                    data = await resp.json()
            except Exception:
                self.cache.set(f"migstat:{mint}", {})
                return None

        pairs = data.get("pairs", []) or []
        valid = [
            p for p in pairs
            if float(p.get("liquidity", {}).get("usd", 0) or 0) > 1000
        ]
        if not valid:
            self.cache.set(f"migstat:{mint}", {})
            return None

        best = max(
            valid,
            key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0),
        )
        result = {
            "pool_address": best.get("pairAddress", ""),
            "dex_id": best.get("dexId", ""),
            "liquidity_usd": float(best.get("liquidity", {}).get("usd", 0) or 0),
            "market_cap": float(best.get("marketCap", 0) or best.get("fdv", 0) or 0),
            "price": float(best.get("priceUsd", 0) or 0),
            "created_at": float(best.get("pairCreatedAt", 0) or 0) / 1000.0,
        }
        self.cache.set(f"migstat:{mint}", result)
        return result


# ============================================================
# 5. SECURITY ANALYZER
# ============================================================

class SecurityAnalyzer:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def detect_bundle(self, early_buys: List[dict]) -> Tuple[int, List[str]]:
        if not early_buys:
            return 0, []
        by_slot = defaultdict(list)
        for b in early_buys:
            by_slot[b["slot"]].append(b)
        bundled: List[str] = []
        for slot, trades in by_slot.items():
            if len(trades) < self.cfg.max_bundle_wallets:
                continue
            sizes = [t["sol_amount"] for t in trades]
            avg = sum(sizes) / len(sizes)
            if avg <= 0:
                continue
            similar = [s for s in sizes if abs(s - avg) / avg < 0.2]
            if len(similar) >= self.cfg.max_bundle_wallets:
                bundled.extend(t["wallet"] for t in trades)
        return len(set(bundled)), list(set(bundled))

    def detect_sniper(
        self, early_buys: List[dict], created_at: float
    ) -> Tuple[int, List[str]]:
        snipers = [
            b["wallet"] for b in early_buys
            if (b["timestamp"] - created_at) <= self.cfg.sniper_window_sec
        ]
        return len(set(snipers)), list(set(snipers))

    def detect_migration_snipers(
        self, buyers: List[dict], migration_time: float
    ) -> int:
        if migration_time <= 0:
            return 0
        snipers = [
            b["wallet"] for b in buyers
            if 0 <= (b["timestamp"] - migration_time) <= 10
        ]
        return len(set(snipers))

    def holder_concentration(
        self, holders: Dict[str, float], lp_pool: str
    ) -> Tuple[float, float, float]:
        filtered = {w: p for w, p in holders.items() if w != lp_pool}
        if not filtered:
            return 0.0, 0.0, 0.0
        sorted_pcts = sorted(filtered.values(), reverse=True)
        return sum(sorted_pcts[:10]), sorted_pcts[0], filtered.get("dev", 0.0)


# ============================================================
# 6. WALLET CLUSTER ANALYZER
# ============================================================

class WalletClusterAnalyzer:
    KNOWN_EXCHANGES = {
        "5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9",
        "2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S",
        "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
    }

    def __init__(self, cfg: Config, rpc_client: RpcClient):
        self.cfg = cfg
        self.rpc = rpc_client

    async def find_funder(
        self, wallet: str, window_start: float, window_end: float
    ) -> Optional[str]:
        sigs = await self.rpc.get_signatures(wallet, limit=30)
        for s in sigs:
            block_time = s.get("blockTime", 0)
            if not block_time or block_time > window_end:
                continue
            if block_time < window_start:
                return None
            tx = await self.rpc.get_transaction(s["signature"])
            if not tx or not tx.get("meta"):
                continue
            try:
                keys = [k["pubkey"] for k in tx["transaction"]["message"]["accountKeys"]]
            except (KeyError, TypeError):
                continue
            idx = keys.index(wallet) if wallet in keys else -1
            if idx < 0:
                continue
            received = tx["meta"]["postBalances"][idx] - tx["meta"]["preBalances"][idx]
            if received < 5_000_000:
                continue
            for i, key in enumerate(keys):
                if i == idx:
                    continue
                sent = tx["meta"]["preBalances"][i] - tx["meta"]["postBalances"][i]
                if sent >= 5_000_000:
                    return key
        return None

    async def analyze_clusters(
        self, wallets: List[str], window_start: float, window_end: float
    ) -> Dict[str, List[str]]:
        if not self.cfg.enable_wallet_cluster_check:
            return {}
        funder_map: Dict[str, List[str]] = defaultdict(list)
        for w in wallets[:15]:
            if w in self.KNOWN_EXCHANGES:
                continue
            funder = await self.find_funder(w, window_start, window_end)
            if funder and funder not in self.KNOWN_EXCHANGES:
                funder_map[funder].append(w)
        return {f: ws for f, ws in funder_map.items() if len(ws) >= 2}

    async def detect_fresh_wallets(
        self, holders: Dict[str, float]
    ) -> Tuple[int, float]:
        top = list(holders.keys())[:20]
        if not top:
            return 0, 0.0
        ages = await asyncio.gather(
            *[self.rpc.get_wallet_age_hours(w) for w in top]
        )
        fresh = sum(1 for a in ages if a < self.cfg.fresh_wallet_max_age_hours)
        return fresh, fresh / len(top)

    async def detect_balance_pattern(self, holders: Dict[str, float]) -> int:
        if not self.cfg.enable_wallet_cluster_check:
            return 0
        top = list(holders.keys())[:20]
        if len(top) < self.cfg.min_similar_balance_wallets:
            return 0
        balances = await asyncio.gather(
            *[self.rpc.get_wallet_balance_sol(w) for w in top]
        )
        tol = self.cfg.balance_similarity_tolerance
        groups: Dict[float, int] = defaultdict(int)
        for b in balances:
            if b <= 0 or b >= 999:
                continue
            groups[round(b, 3)] += 1
        if not groups:
            return 0
        keys = sorted(groups.keys())
        largest = 0
        for i, k in enumerate(keys):
            count = groups[k]
            for j in range(i + 1, len(keys)):
                if abs(keys[j] - k) / max(k, 0.001) <= tol:
                    count += groups[keys[j]]
                else:
                    break
            largest = max(largest, count)
        return largest

    def cluster_risk_score(
        self,
        clusters: Dict[str, List[str]],
        total_holders: int,
        fresh_count: int,
        fresh_ratio: float,
        similar_balance: int,
    ) -> Tuple[float, List[str]]:
        red_flags: List[str] = []
        total_clustered = sum(len(ws) for ws in clusters.values())
        clustered_pct = (total_clustered / total_holders * 100) if total_holders > 0 else 0
        largest_cluster = max((len(ws) for ws in clusters.values()), default=0)
        score = min(100, clustered_pct * 2)

        if clustered_pct > self.cfg.max_cluster_pct:
            red_flags.append(f"cluster_pct:{clustered_pct:.0f}%")
        if largest_cluster > self.cfg.max_cluster_size:
            red_flags.append(f"cluster_size:{largest_cluster}")
        if total_clustered > self.cfg.max_clustered_wallets:
            red_flags.append(f"clustered_wallets:{total_clustered}")
        if len(clusters) >= self.cfg.max_clusters:
            red_flags.append(f"multiple_clusters:{len(clusters)}")
        if fresh_ratio > self.cfg.max_fresh_wallet_ratio:
            red_flags.append(f"fresh_wallets:{fresh_count}({fresh_ratio:.0%})")
        if similar_balance > self.cfg.max_similar_balance_wallets:
            red_flags.append(f"similar_balance_wallets:{similar_balance}")
        return round(score, 2), red_flags


# ============================================================
# 7. FAST SCORER + BIRTH SIGNAL SCORER
# ============================================================

class FastScorer:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def score_birth_signals(self, t: TokenState) -> Tuple[float, List[str], List[str]]:
        """
        Skor sinyal kelahiran. Return (score, passed_signals, failed_signals).
        Total max = 100 (5 sinyal × 20).
        """
        passed: List[str] = []
        failed: List[str] = []
        score = 0.0

        # Sinyal 1: Alpha wallet count
        if t.alpha_wallet_count >= self.cfg.strong_alpha_wallets:
            score += 25
            passed.append(f"alpha_strong:{t.alpha_wallet_count}")
        elif t.alpha_wallet_count >= self.cfg.min_alpha_wallets:
            score += 15
            passed.append(f"alpha_ok:{t.alpha_wallet_count}")
        else:
            failed.append(f"alpha_low:{t.alpha_wallet_count}")

        # Sinyal 2: Bundle di creation block
        if t.bundle_creation_block_count <= 1:
            score += 20
            passed.append("no_creation_bundle")
        elif t.bundle_creation_block_count <= self.cfg.max_creation_block_buyers:
            score += 10
            passed.append(f"creation_bundle_minor:{t.bundle_creation_block_count}")
        else:
            failed.append(f"creation_bundle:{t.bundle_creation_block_count}")

        # Sinyal 3: Liquidity velocity
        if t.liquidity_velocity >= self.cfg.strong_liquidity_velocity_sol_per_min:
            score += 20
            passed.append(f"liq_velocity_strong:{t.liquidity_velocity:.2f}")
        elif t.liquidity_velocity >= self.cfg.min_liquidity_velocity_sol_per_min:
            score += 10
            passed.append(f"liq_velocity_ok:{t.liquidity_velocity:.2f}")
        else:
            failed.append(f"liq_velocity_low:{t.liquidity_velocity:.2f}")

        # Sinyal 4: Holder growth velocity
        if t.holder_growth_velocity >= self.cfg.strong_holder_growth_per_min:
            score += 20
            passed.append(f"holder_velocity_strong:{t.holder_growth_velocity:.2f}")
        elif t.holder_growth_velocity >= self.cfg.min_holder_growth_per_min:
            score += 10
            passed.append(f"holder_velocity_ok:{t.holder_growth_velocity:.2f}")
        else:
            failed.append(f"holder_velocity_low:{t.holder_growth_velocity:.2f}")

        # Sinyal 5: Social presence
        if self.cfg.check_social_presence:
            social_count = sum([t.has_twitter, t.has_telegram, t.has_website])
            if social_count >= 2:
                score += 15
                passed.append(f"social_strong:{social_count}")
            elif social_count >= 1:
                score += 8
                passed.append(f"social_ok:{social_count}")
            else:
                failed.append("no_social")

        return round(score, 2), passed, failed

    def score_bonding(self, t: TokenState) -> Tuple[float, List[str], List[str]]:
        reasons: List[str] = []
        red_flags: List[str] = []
        score = 0.0

        if t.price_at_5m_ago > 0:
            pump_pct = (t.price - t.price_at_5m_ago) / t.price_at_5m_ago * 100
        else:
            pump_pct = 0.0
        if pump_pct > self.cfg.max_price_pump_5m_pct:
            red_flags.append(f"price_pump_too_high:{pump_pct:.1f}%")
        elif pump_pct >= self.cfg.min_price_pump_5m_pct:
            score += min(25, pump_pct)
            reasons.append(f"momentum:{pump_pct:.1f}%")

        total_vol = t.volume_buys + t.volume_sells
        if total_vol > 0:
            buy_ratio = t.volume_buys / total_vol
            score += buy_ratio * 25
            if buy_ratio > 0.6:
                reasons.append(f"buy_pressure:{buy_ratio:.2f}")
            elif buy_ratio < 0.35:
                red_flags.append(f"sell_pressure:{buy_ratio:.2f}")

        if self.cfg.enable_buyer_quality_check:
            if t.real_buyers >= self.cfg.min_real_buyers:
                score += 20
                reasons.append(f"real_buyers:{t.real_buyers}")
            elif t.real_buyers >= 1:
                score += 10
                reasons.append(f"few_real_buyers:{t.real_buyers}")
            else:
                red_flags.append(f"no_real_buyers")
            if t.bot_ratio > self.cfg.max_bot_ratio:
                red_flags.append(f"bot_dominated:{t.bot_ratio:.0%}")

        if self.cfg.min_bonding_pct <= t.bonding_pct <= self.cfg.max_bonding_pct:
            score += 15
            reasons.append(f"bonding_ok:{t.bonding_pct:.1f}%")
        elif t.bonding_pct > 0:
            red_flags.append(f"bonding_bad:{t.bonding_pct:.1f}%")

        if t.liquidity_usd >= self.cfg.min_liquidity_usd:
            score += 5
            reasons.append(f"liq_ok:${t.liquidity_usd:,.0f}")

        return round(score, 2), reasons, red_flags

    def score_post_migration(self, t: TokenState) -> Tuple[float, List[str], List[str]]:
        reasons: List[str] = []
        red_flags: List[str] = []
        score = 0.0

        if self.cfg.min_market_cap_usd <= t.market_cap_usd <= self.cfg.max_market_cap_usd:
            score += 25
            reasons.append(f"mcap_ok:${t.market_cap_usd:,.0f}")
        else:
            red_flags.append(f"mcap_out:${t.market_cap_usd:,.0f}")

        if t.pool_age_minutes <= self.cfg.max_pool_age_minutes:
            score += 20
            reasons.append(f"pool_age:{t.pool_age_minutes:.0f}min")
        else:
            red_flags.append(f"pool_too_old:{t.pool_age_minutes:.0f}min")

        if t.liquidity_usd >= self.cfg.min_pool_liquidity_usd:
            score += 20
            reasons.append(f"pool_liq:${t.liquidity_usd:,.0f}")
        else:
            red_flags.append(f"pool_liq_low:${t.liquidity_usd:,.0f}")

        total_vol = t.volume_buys + t.volume_sells
        if total_vol > 0:
            buy_ratio = t.volume_buys / total_vol
            score += buy_ratio * 20
            if buy_ratio > 0.55:
                reasons.append(f"buy_pressure:{buy_ratio:.2f}")

        hc = len(t.holders)
        if hc >= 200:
            score += 10
            reasons.append(f"holders:{hc}")
        elif hc >= 50:
            score += 6
        elif hc >= 20:
            score += 3
        else:
            red_flags.append(f"few_holders:{hc}")

        if t.price_at_5m_ago > 0:
            pump_pct = (t.price - t.price_at_5m_ago) / t.price_at_5m_ago * 100
            if 2 <= pump_pct <= self.cfg.max_price_pump_5m_pct:
                score += 5
                reasons.append(f"momentum:{pump_pct:.1f}%")

        return round(score, 2), reasons, red_flags


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
        stop = signal.entry_price * (1 - self.cfg.hard_stop_loss_pct / 100)
        pos = Position(
            mint=signal.mint,
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

    def update(self, mint: str, price: float) -> List[str]:
        actions: List[str] = []
        pos = self.positions.get(mint)
        if not pos:
            return actions
        if price > pos.high_water:
            pos.high_water = price
        new_trailing = pos.high_water * (1 - self.cfg.trailing_stop_pct / 100)
        if new_trailing > pos.trailing_price:
            pos.trailing_price = new_trailing
        if price <= pos.stop_price:
            actions.append("stop_loss")
            return actions
        if (
            not pos.initial_recovered
            and price >= pos.entry_price * self.cfg.take_initials_multiple
        ):
            actions.append("take_initials")
            pos.initial_recovered = True
        if pos.initial_recovered and price <= pos.trailing_price:
            actions.append("trailing")
        if (time.time() - pos.opened_at) > self.cfg.time_stop_hours * 3600:
            actions.append("time_stop")
        return actions

    def close(self, mint: str, pnl_usd: float):
        if self.positions.pop(mint, None):
            if pnl_usd < 0:
                self.daily_loss_streak += 1
            else:
                self.daily_loss_streak = 0


# ============================================================
# 9. TRADE LOGGER
# ============================================================

class TradeLogger:
    def __init__(self, log_file: str):
        self.log_file = log_file

    def log_signal(self, signal: Signal, token: TokenState):
        self._append({
            "type": "signal",
            "ts": signal.timestamp,
            "mint": signal.mint,
            "source": signal.source,
            "phase": signal.phase,
            "score": signal.score,
            "birth_score": signal.birth_score,
            "price": signal.entry_price,
            "reasons": signal.reasons,
            "bonding_pct": token.bonding_pct,
            "mcap": token.market_cap_usd,
            "liquidity": token.liquidity_usd,
            "alpha_wallets": token.alpha_wallet_count,
            "liq_velocity": token.liquidity_velocity,
            "holder_velocity": token.holder_growth_velocity,
            "social_score": token.social_score,
        })

    def log_exit(self, mint: str, price: float, reason: str, pnl_pct: float):
        self._append({
            "type": "exit", "ts": time.time(), "mint": mint,
            "price": price, "reason": reason, "pnl_pct": pnl_pct,
        })

    def _append(self, entry: dict):
        try:
            with open(self.log_file, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            print(f"[logger-error] {e}")


# ============================================================
# 10. TELEGRAM ALERTER
# ============================================================

class TelegramAlerter:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if not self.bot_token or not self.chat_id:
            print("[telegram] token/chat_id kosong, alert dinonaktifkan")
            return
        self.session = aiohttp.ClientSession()
        await self._send("✅ <b>[HIT-AND-RUN] scanner v10 online (Birth Signals)</b>")

    async def close(self):
        if self.session:
            await self.session.close()

    async def send_signal_alert(self, signal: Signal, token: TokenState):
        emoji = "🟢" if signal.score >= 80 else "🟡"
        birth_emoji = "🔥" if signal.birth_score >= 50 else "⭐"
        phase_label = "PRE-BONDING" if signal.phase == "bonding" else "POST-MIGRATION"
        text = (
            f"{emoji} <b>[HIT-AND-RUN] SIGNAL ({phase_label})</b>\n"
            f"{birth_emoji} <b>BIRTH SCORE: {signal.birth_score:.0f}/100</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Token:</b> <code>{signal.mint}</code>\n"
            f"<b>Source:</b> {signal.source}\n"
            f"<b>Conviction:</b> {signal.score}/100\n"
            f"<b>Entry Price:</b> <code>{signal.entry_price:.8f}</code>\n"
            + (
                f"<b>Bonding:</b> {token.bonding_pct:.1f}%\n"
                if signal.phase == "bonding"
                else f"<b>MCap:</b> ${token.market_cap_usd:,.0f}\n"
                     f"<b>Pool Age:</b> {token.pool_age_minutes:.0f} min\n"
            ) +
            f"<b>Liquidity:</b> ${token.liquidity_usd:,.0f}\n"
            f"<b>Holders:</b> {len(token.holders)}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🌟 BIRTH SIGNALS:</b>\n"
            f"  Alpha Wallets: {token.alpha_wallet_count}\n"
            f"  Creation Bundle: {token.bundle_creation_block_count}\n"
            f"  Liq Velocity: {token.liquidity_velocity:.2f} SOL/min\n"
            f"  Holder Velocity: {token.holder_growth_velocity:.2f} holder/min\n"
            f"  Social: {'✅' if (token.has_twitter or token.has_telegram or token.has_website) else '❌'}"
            f" (TW:{'✅' if token.has_twitter else '❌'} TG:{'✅' if token.has_telegram else '❌'} WEB:{'✅' if token.has_website else '❌'})\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Keamanan:</b>\n"
            f"  Mint Auth: {'❌' if token.mint_authority_active else '✅'}\n"
            f"  Freeze Auth: {'❌' if token.freeze_authority_active else '✅'}\n"
            f"  LP Lock: {'✅' if token.lp_locked else '❌'}\n"
            f"  Honeypot: {'❌' if not token.sell_simulation_ok else '✅'}\n"
            f"  Dev Sold: {token.dev_sold_pct:.1f}%\n"
            f"  Cluster: {token.cluster_pct:.0f}%\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Alasan Lolos:</b>\n" +
            "\n".join(f"  • {r}" for r in signal.reasons) +
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Exit Plan:</b>\n"
            f"  • SL: -35%\n"
            f"  • TP: +100% (jual 50%)\n"
            f"  • Trailing: 25%\n"
            f"  • Time Stop: 24 jam"
        )
        await self._send(text)

    async def send_exit_alert(
        self, mint: str, price: float, reason: str, pnl_pct: float
    ):
        emoji = "🟢" if pnl_pct > 0 else "🔴"
        label = {
            "stop_loss": "🛑 STOP LOSS",
            "take_initials": "💰 TAKE INITIALS (50%)",
            "trailing": "📉 TRAILING STOP",
            "time_stop": "⏰ TIME STOP",
        }.get(reason, reason)
        text = (
            f"{emoji} <b>[HIT-AND-RUN] EXIT — {label}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Token:</b> <code>{mint}</code>\n"
            f"<b>Exit Price:</b> <code>{price:.8f}</code>\n"
            f"<b>PnL:</b> {pnl_pct:+.1f}%\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )
        await self._send(text)

    async def _send(self, text: str):
        if not self.session:
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
                if resp.status != 200:
                    body = await resp.text()
                    print(f"[telegram-error] {resp.status} {body[:200]}")
        except Exception as e:
            print(f"[telegram-error] {e}")


# ============================================================
# 10b. HEALTH SERVER
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
                "version": "v10",
                "ok": True,
                "tokens_cached": len(self.scanner.tokens),
                "signals_emitted": len(self.scanner.signals),
                "positions_open": len(self.scanner.positions.positions),
                "rescan_queue": len(self.scanner.rescan_heap),
                "daily_exposure_usd": self.scanner.positions.daily_exposure,
                "loss_streak": self.scanner.positions.daily_loss_streak,
                "running": self.scanner.running,
            })

        @self.app.route("/healthz")
        def healthz():
            return "ok", 200

        @self.app.route("/signals")
        def signals():
            return jsonify({
                "count": len(self.scanner.signals),
                "signals": [
                    {
                        "mint": s.mint,
                        "score": s.score,
                        "birth_score": s.birth_score,
                        "phase": s.phase,
                        "source": s.source,
                        "entry_price": s.entry_price,
                        "ts": s.timestamp,
                    }
                    for s in self.scanner.signals[-50:]
                ],
            })

        @self.app.route("/positions")
        def positions():
            return jsonify({
                "count": len(self.scanner.positions.positions),
                "positions": [
                    {
                        "mint": p.mint,
                        "entry_price": p.entry_price,
                        "size_usd": p.size_usd,
                        "high_water": p.high_water,
                        "stop_price": p.stop_price,
                        "opened_at": p.opened_at,
                    }
                    for p in self.scanner.positions.positions.values()
                ],
            })

    def run(self, port: int):
        self.app.run(
            host="0.0.0.0",
            port=port,
            debug=False,
            use_reloader=False,
            threaded=True,
        )


# ============================================================
# 11. SCANNER UTAMA
# ============================================================

class HitAndRunScanner:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.cache = TimedCache()
        self.rpc = RpcClient(cfg, self.cache)
        self.security = SecurityAnalyzer(cfg)
        self.scorer = FastScorer(cfg)
        self.positions = PositionManager(cfg)
        self.logger = TradeLogger(cfg.log_file)
        self.telegram = TelegramAlerter(
            bot_token=cfg.telegram_bot_token,
            chat_id=cfg.telegram_chat_id,
        )
        self.cluster_analyzer = WalletClusterAnalyzer(cfg, self.rpc)

        self.tokens: Dict[str, TokenState] = {}
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=600))
        self.buyers_buffer: Dict[str, List[dict]] = defaultdict(list)
        self.signals: List[Signal] = []

        self.eval_queue: asyncio.Queue = asyncio.Queue()
        self.signal_queue: asyncio.Queue = asyncio.Queue()
        self.rescan_heap: List[Tuple[float, str]] = []

        self.portfolio_usd = cfg.portfolio_usd
        self.running = True

    # --------------------------------------------------------
    # 11.1 INGESTION
    # --------------------------------------------------------

    async def pumpportal_listener(self):
        while self.running:
            try:
                async with websockets.connect(
                    self.cfg.pumpportal_ws, ping_interval=20, ping_timeout=20
                ) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    await ws.send(json.dumps({"method": "subscribeMigration"}))
                    print("[ws:pumpfun] connected")
                    async for raw in ws:
                        if not self.running:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        await self.handle_pumpportal(msg)
            except Exception as e:
                print(f"[ws:pumpfun] error: {e}, reconnect 3s")
                await asyncio.sleep(3)

    async def raydium_listener(self):
        if not self.cfg.enable_raydium:
            return
        while self.running:
            try:
                async with websockets.connect(
                    self.cfg.raydium_ws, ping_interval=20, ping_timeout=20
                ) as ws:
                    print("[ws:raydium] connected")
                    async for raw in ws:
                        if not self.running:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        await self.handle_generic(msg, source="raydium")
            except Exception as e:
                print(f"[ws:raydium] error: {e}, reconnect 5s")
                await asyncio.sleep(5)

    async def meteora_listener(self):
        if not self.cfg.enable_meteora:
            return
        while self.running:
            try:
                async with websockets.connect(
                    self.cfg.meteora_ws, ping_interval=20, ping_timeout=20
                ) as ws:
                    print("[ws:meteora] connected")
                    async for raw in ws:
                        if not self.running:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        await self.handle_generic(msg, source="meteora")
            except Exception as e:
                print(f"[ws:meteora] error: {e}, reconnect 5s")
                await asyncio.sleep(5)

    async def handle_pumpportal(self, msg: dict):
        tx_type = msg.get("txType")
        if tx_type == "create":
            mint = msg["mint"]
            if mint in self.tokens:
                return
            v_sol = float(msg.get("vSolInBondingCurve", 0) or 0)
            bonding_pct = min(100.0, (v_sol / 85.0) * 100) if v_sol > 0 else 0.0
            t = TokenState(
                mint=mint,
                creator=msg.get("traderPublicKey", ""),
                source="pumpfun",
                created_at=time.time(),
                price=float(msg.get("initialBuy", 0) or 0),
                bonding_pct=bonding_pct,
                liquidity_usd=max(0.0, v_sol * 150),
            )
            t.sol_in_bonding_history.append((time.time(), v_sol))
            self.tokens[mint] = t
            await self.eval_queue.put(mint)
            print(f"[new] {mint} (pumpfun, bonding={bonding_pct:.1f}%)")
        elif tx_type == "migrate":
            await self.handle_migration(msg)
        elif tx_type in ("buy", "sell"):
            await self.handle_trade(msg, source="pumpfun")

    async def handle_generic(self, msg: dict, source: str):
        mint = msg.get("mint") or msg.get("token") or msg.get("address")
        if not mint:
            return
        if mint not in self.tokens:
            self.tokens[mint] = TokenState(
                mint=mint,
                creator=msg.get("creator", ""),
                source=source,
                created_at=time.time(),
                price=float(msg.get("price", 0) or 0),
            )
            await self.eval_queue.put(mint)
            print(f"[new] {mint} ({source})")
        await self.handle_trade(msg, source=source)

    async def handle_migration(self, msg: dict):
        mint = msg.get("mint")
        if not mint:
            return
        now = time.time()
        if mint not in self.tokens:
            self.tokens[mint] = TokenState(
                mint=mint,
                creator=msg.get("traderPublicKey", ""),
                source="pumpfun",
                created_at=now,
            )
        t = self.tokens[mint]
        t.is_migrated = True
        t.pool_address = msg.get("pool", "") or msg.get("poolAddress", "")
        t.migration_detected_at = now
        t.scored = False
        t.signal_emitted = False
        await self.eval_queue.put(mint)
        print(f"[migration] {mint} → pool {t.pool_address}")

    async def handle_trade(self, msg: dict, source: str):
        mint = msg.get("mint") or msg.get("token") or msg.get("address")
        t = self.tokens.get(mint)
        if not t:
            return
        side = msg.get("txType", msg.get("side", "buy"))
        price = float(msg.get("price", 0) or 0)
        sol_amount = float(msg.get("solAmount", 0) or 0)
        wallet = msg.get("traderPublicKey", msg.get("wallet", ""))
        trade = {
            "wallet": wallet, "sol_amount": sol_amount, "price": price,
            "slot": int(msg.get("slot", 0) or 0),
            "timestamp": time.time(), "side": side,
        }
        if side == "buy":
            t.buys_count += 1
            t.volume_buys += sol_amount
            t.unique_buyers.add(wallet)
            if len(self.buyers_buffer[mint]) < 300:
                self.buyers_buffer[mint].append(trade)
        else:
            t.sells_count += 1
            t.volume_sells += sol_amount
        t.volume_usd = (t.volume_buys + t.volume_sells) * 150
        if price > 0:
            t.price = price
            self.price_history[mint].append((time.time(), price))

        # BIRTH SIGNAL: track SOL in bonding untuk velocity
        v_sol = float(msg.get("vSolInBondingCurve", 0) or 0)
        if v_sol > 0:
            t.bonding_pct = min(100.0, (v_sol / 85.0) * 100)
            t.sol_in_bonding_history.append((time.time(), v_sol))
            if not t.is_migrated:
                t.liquidity_usd = max(t.liquidity_usd, v_sol * 150)

    # --------------------------------------------------------
    # 11.2 DISCOVERY
    # --------------------------------------------------------

    async def dexscreener_discovery_loop(self):
        if not self.cfg.enable_dexscreener_discovery:
            return
        print("[discovery] multi-source discovery started")
        while self.running:
            try:
                mints: List[str] = []

                for q in self.cfg.discovery_queries:
                    try:
                        url = f"{self.cfg.dexscreener_search_url}?q={q}"
                        async with self.rpc.sem:
                            async with self.rpc.session.get(url) as resp:
                                if resp.status != 200:
                                    continue
                                data = await resp.json()
                        for p in (data.get("pairs", []) or [])[:self.cfg.discovery_max_per_query]:
                            if p.get("chainId") == "solana":
                                addr = p.get("baseToken", {}).get("address")
                                if addr:
                                    mints.append(addr)
                    except Exception:
                        pass

                for boost_url in (
                    self.cfg.dexscreener_boosts_latest,
                    self.cfg.dexscreener_boosts_top,
                    self.cfg.dexscreener_profiles,
                ):
                    try:
                        async with self.rpc.sem:
                            async with self.rpc.session.get(boost_url) as resp:
                                if resp.status != 200:
                                    continue
                                items = await resp.json()
                        for it in (items or [])[:40]:
                            if it.get("chainId") == "solana":
                                addr = it.get("tokenAddress")
                                if addr:
                                    mints.append(addr)
                    except Exception:
                        pass

                try:
                    async with self.rpc.sem:
                        async with self.rpc.session.get(self.cfg.raydium_pools_url) as resp:
                            if resp.status == 200:
                                ray = await resp.json()
                                for pool in ray.get("data", {}).get("data", []) or []:
                                    mint_a = pool.get("mintA", {}).get("address")
                                    sym_b = pool.get("mintB", {}).get("symbol", "")
                                    mint_b = pool.get("mintB", {}).get("address")
                                    t_mint = mint_a if sym_b in ("WSOL", "SOL") else mint_b
                                    if t_mint:
                                        mints.append(t_mint)
                except Exception:
                    pass

                if self.cfg.enable_new_pools_discovery:
                    try:
                        mints.extend(await self.rpc.get_new_pools_from_dexscreener())
                    except Exception:
                        pass

                if self.cfg.enable_pumpfun_api_discovery:
                    try:
                        mints.extend(await self.rpc.get_pumpfun_recent_coins())
                    except Exception:
                        pass

                if self.cfg.enable_watchlist and self.cfg.watchlist_mints:
                    mints.extend(self.cfg.watchlist_mints)

                if self.cfg.enable_geckoterminal_discovery:
                    try:
                        mints.extend(await self.rpc.get_geckoterminal_new_pools())
                    except Exception:
                        pass

                if self.cfg.enable_solanatracker_discovery:
                    try:
                        mints.extend(await self.rpc.get_solanatracker_trending())
                    except Exception:
                        pass

                if self.cfg.enable_rpc_logs_discovery:
                    try:
                        mints.extend(await self.rpc.get_pumpfun_new_tokens_rpc())
                    except Exception:
                        pass

                mints = list(dict.fromkeys(mints))
                added = 0
                for mint in mints:
                    if mint in self.tokens:
                        continue
                    self.tokens[mint] = TokenState(
                        mint=mint,
                        creator="",
                        source="dexscreener",
                        created_at=time.time(),
                    )
                    await self.eval_queue.put(mint)
                    added += 1

                print(f"[discovery] scanned {len(mints)} mints, {added} new")
            except Exception as e:
                print(f"[discovery-error] {e}")
            await asyncio.sleep(self.cfg.discovery_interval_sec)

    # --------------------------------------------------------
    # 11.3 WORKER + EVALUATE
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
                print(f"[worker-{worker_id}] error {mint}: {e}")
            finally:
                self.eval_queue.task_done()

    async def evaluate_token(self, mint: str):
        t = self.tokens.get(mint)
        if not t or t.scored:
            return

        now = time.time()
        age = now - t.created_at
        if age < self.cfg.min_age_sec and t.rescan_count == 0:
            await asyncio.sleep(self.cfg.min_age_sec - age)
            now = time.time()
            age = now - t.created_at

        max_age = (
            self.cfg.max_rescan_age_sec
            if self.cfg.enable_rescan
            else self.cfg.observation_window_sec
        )
        if age > max_age and not t.signal_emitted and not t.is_migrated:
            self.tokens.pop(mint, None)
            return

        # ---------- PENGECEKAN PARALEL ----------
        (
            holders,
            lp_info,
            authorities,
            transfer_fee,
            metadata_mutable,
            sell_ok,
            creator_rep,
            micro_wallets,
            pool_data,
            buyer_quality,
            dev_sold,
            social,
            bundle_block,
        ) = await asyncio.gather(
            self.rpc.get_holders(mint),
            self.rpc.get_lp_info(mint),
            self.rpc.get_token_authorities(mint),
            self.rpc.get_transfer_fee(mint),
            self.rpc.get_metadata_mutability(mint),
            self.rpc.simulate_sell(mint),
            self.rpc.get_creator_reputation(mint),
            self.rpc.get_micro_wallet_count(mint),
            self.rpc.get_pool_data(mint) if t.is_migrated else self._empty(),
            self.rpc.analyze_buyer_quality(self.buyers_buffer.get(mint, [])),
            self.rpc.check_dev_sold(mint, t.creator, t.created_at)
            if t.creator else self._zero(),
            self.rpc.check_social_presence(mint),
            self.rpc.detect_creation_block_bundle(mint, t.creator, t.created_at),
        )

        locked, liq_usd, lp_pool, rug_score, rug_flags = lp_info
        mint_auth, freeze_auth = authorities
        rugpull_count, _ = creator_rep
        real_buyers, bot_buyers, bot_ratio = buyer_quality
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
        t.micro_wallet_count = micro_wallets
        t.real_buyers = real_buyers
        t.bot_buyers = bot_buyers
        t.bot_ratio = bot_ratio
        t.dev_sold_pct = dev_sold
        t.has_twitter = has_tw
        t.has_telegram = has_tg
        t.has_website = has_web
        t.bundle_creation_block_count = bundle_block

        if pool_data:
            t.market_cap_usd = pool_data.get("market_cap", 0.0)
            t.pool_address = t.pool_address or pool_data.get("pool_address", "")
            created_at_pool = pool_data.get("created_at", 0.0)
            t.pool_age_minutes = (
                (now - created_at_pool) / 60.0 if created_at_pool > 0 else 0.0
            )
            pool_liq = pool_data.get("liquidity_usd", 0.0)
            if pool_liq > 0:
                t.liquidity_usd = pool_liq
            if pool_data.get("price", 0) > 0:
                t.price = pool_data["price"]
                self.price_history[mint].append((now, t.price))

        # ---------- BIRTH SIGNALS CALCULATION ----------
        # 1. Track holder history untuk velocity
        t.holder_history.append((now, len(holders)))

        # 2. Liquidity velocity (SOL/menit)
        if len(t.sol_in_bonding_history) >= 2:
            oldest = t.sol_in_bonding_history[0]
            newest = t.sol_in_bonding_history[-1]
            dt_min = (newest[0] - oldest[0]) / 60.0
            if dt_min > 0.05:
                t.liquidity_velocity = max(0.0, (newest[1] - oldest[1]) / dt_min)
        elif t.is_migrated and pool_data:
            # Fallback untuk post-migration: pakai USD liquidity history
            pass

        # 3. Holder growth velocity
        if len(t.holder_history) >= 2:
            oldest_h = t.holder_history[0]
            newest_h = t.holder_history[-1]
            dt_min_h = (newest_h[0] - oldest_h[0]) / 60.0
            if dt_min_h > 0.05:
                t.holder_growth_velocity = max(
                    0.0, (newest_h[1] - oldest_h[1]) / dt_min_h
                )

        # 4. Alpha wallet count
        if self.cfg.enable_birth_signal_check:
            early_wallets = [b["wallet"] for b in self.buyers_buffer.get(mint, [])]
            t.alpha_wallet_count = await self.rpc.count_alpha_wallets(early_wallets)

        # 5. Social score
        t.social_score = (1 if has_tw else 0) + (1 if has_tg else 0) + (1 if has_web else 0)

        # Hitung skor sinyal kelahiran
        birth_score, birth_passed, birth_failed = self.scorer.score_birth_signals(t)
        t.birth_signal_score = birth_score
        t.birth_signals_passed = birth_passed
        t.birth_signals_failed = birth_failed

        # ---------- HARGA 5M LALU ----------
        history = self.price_history.get(mint)
        if history and len(history) > 1:
            cutoff = now - 300
            past = [p for ts, p in history if ts <= cutoff]
            t.price_at_5m_ago = past[-1] if past else history[0][1]
        else:
            t.price_at_5m_ago = t.price

        # ---------- SECURITY CHECKS ----------
        buyers = self.buyers_buffer.get(mint, [])
        bundle_count, _ = self.security.detect_bundle(buyers)
        sniper_count, _ = self.security.detect_sniper(buyers, t.created_at)
        top10_pct, top1_pct, dev_pct = self.security.holder_concentration(holders, lp_pool)

        dev_pct2, dev_flagged = await self.rpc.get_developer_holdings(
            mint, t.creator, lp_pool
        )
        t.dev_holding_pct = max(dev_pct, dev_pct2)

        if t.is_migrated and self.cfg.enable_migration_sniper_check:
            t.migration_snipers = self.security.detect_migration_snipers(
                buyers, t.migration_detected_at
            )

        early_wallets = [b["wallet"] for b in buyers][:20]
        window_start = t.created_at - self.cfg.funding_window_hours * 3600
        window_end = t.created_at + 60
        (
            clusters,
            (fresh_count, fresh_ratio),
            similar_balance,
        ) = await asyncio.gather(
            self.cluster_analyzer.analyze_clusters(early_wallets, window_start, window_end),
            self.cluster_analyzer.detect_fresh_wallets(holders),
            self.cluster_analyzer.detect_balance_pattern(holders),
        )
        _, cluster_flags = self.cluster_analyzer.cluster_risk_score(
            clusters, len(holders), fresh_count, fresh_ratio, similar_balance
        )
        t.fresh_wallet_count = fresh_count
        t.fresh_wallet_ratio = fresh_ratio
        t.similar_balance_wallets = similar_balance
        t.cluster_pct = (
            sum(len(ws) for ws in clusters.values()) / len(holders) * 100
            if holders else 0
        )

        # ---------- RED FLAGS ----------
        red_flags: List[str] = []

        if bundle_count > self.cfg.max_bundle_wallets:
            red_flags.append(f"bundle:{bundle_count}")
        if sniper_count > self.cfg.max_sniper_wallets:
            red_flags.append(f"sniper:{sniper_count}")
        if t.migration_snipers > self.cfg.max_migration_snipers:
            red_flags.append(f"migration_snipers:{t.migration_snipers}")
        if top10_pct > self.cfg.max_top10_holder_pct:
            red_flags.append(f"top10:{top10_pct:.1f}%")
        if top1_pct > self.cfg.max_top1_holder_pct:
            red_flags.append(f"top1:{top1_pct:.1f}%")
        if t.dev_holding_pct > self.cfg.max_dev_holding_pct:
            red_flags.append(f"dev_hold:{t.dev_holding_pct:.1f}%")
        if dev_flagged:
            red_flags.append("dev_still_holds")
        if t.dev_sold_pct > self.cfg.max_dev_sell_pct:
            red_flags.append(f"dev_sold:{t.dev_sold_pct:.1f}%")
        if len(holders) < self.cfg.min_holders_count:
            red_flags.append(f"too_few_holders:{len(holders)}")
        if self.cfg.require_mint_authority_revoked and mint_auth:
            red_flags.append("mint_authority_active")
        if self.cfg.require_freeze_authority_revoked and freeze_auth:
            red_flags.append("freeze_authority_active")
        if self.cfg.require_metadata_immutable and metadata_mutable:
            red_flags.append("metadata_mutable")
        if transfer_fee > self.cfg.max_transfer_fee_pct:
            red_flags.append(f"transfer_fee:{transfer_fee:.1f}%")
        if not sell_ok:
            red_flags.append("honeypot_sell_blocked")
        if rugpull_count > self.cfg.max_creator_rugpull_count:
            red_flags.append(f"creator_rugpull_history:{rugpull_count}")
        if micro_wallets > self.cfg.max_micro_wallet_count:
            red_flags.append(f"micro_wallets:{micro_wallets}")
        if self.cfg.require_lp_locked and not locked and t.is_migrated:
            red_flags.append("lp_not_locked")
        if t.liquidity_usd < self.cfg.min_liquidity_usd and not t.is_migrated:
            red_flags.append(f"liq_low:${t.liquidity_usd:,.0f}")
        if rug_score > self.cfg.max_rugcheck_score:
            red_flags.append(f"rugcheck_score:{rug_score:.0f}")
        if t.sells_count == 0 and age > 30 and not t.is_migrated:
            red_flags.append("no_sell_yet")

        if self.cfg.enable_buyer_quality_check:
            if bot_ratio > self.cfg.max_bot_ratio:
                red_flags.append(f"bot_ratio:{bot_ratio:.0%}")
            if real_buyers < self.cfg.min_real_buyers:
                red_flags.append(f"real_buyers_low:{real_buyers}")

        red_flags.extend(cluster_flags)
        red_flags.extend(rug_flags)

        # ---------- BIRTH SIGNAL GATE ----------
        if self.cfg.enable_birth_signal_check:
            if t.birth_signal_score < self.cfg.birth_signal_min_score:
                red_flags.append(
                    f"birth_score_low:{t.birth_signal_score:.0f}"
                    f"(min:{self.cfg.birth_signal_min_score})"
                )
            if self.cfg.require_at_least_one_social and not any([has_tw, has_tg, has_web]):
                red_flags.append("no_social_presence")

        # ---------- SKOR ----------
        if t.is_migrated and self.cfg.enable_post_migration:
            score, reasons, score_flags = self.scorer.score_post_migration(t)
            phase = "post_migration"
        else:
            score, reasons, score_flags = self.scorer.score_bonding(t)
            phase = "bonding"

        # Bonus: tambahkan birth score ke conviction score
        score += t.birth_signal_score * 0.3
        score = round(min(100, score), 2)

        reasons.extend(t.birth_signals_passed)
        red_flags.extend(score_flags)
        t.scored = True

        # ---------- SKIP ----------
        if red_flags:
            print(
                f"[skip-{phase}] {mint} score={score} birth={t.birth_signal_score:.0f} "
                f"flags={red_flags}"
            )

            can_rescan = (
                self.cfg.enable_rescan
                and t.rescan_count < self.cfg.max_rescan_count
                and age < self.cfg.max_rescan_age_sec
            )
            if t.is_migrated and self.cfg.enable_post_migration_rescan:
                can_rescan = (
                    t.rescan_count < self.cfg.max_rescan_count
                    and t.pool_age_minutes < self.cfg.max_pool_age_minutes
                )

            if can_rescan:
                t.rescan_count += 1
                t.scored = False
                t.next_rescan_at = now + self.cfg.rescan_delay_sec
                heapq.heappush(self.rescan_heap, (t.next_rescan_at, mint))
                print(
                    f"[rescan-scheduled] {mint} in {self.cfg.rescan_delay_sec}s "
                    f"(attempt {t.rescan_count}/{self.cfg.max_rescan_count})"
                )
            elif not t.is_migrated:
                self.tokens.pop(mint, None)
            return

        # ---------- LOLOS ----------
        if score >= self.cfg.min_conviction_score:
            signal = Signal(
                mint=mint, score=score, entry_price=t.price,
                reasons=reasons, red_flags=[], timestamp=now,
                source=t.source, phase=phase,
                birth_score=t.birth_signal_score,
            )
            t.signal_emitted = True
            self.signals.append(signal)
            self.logger.log_signal(signal, t)
            await self.signal_queue.put(signal)
            print(
                f"[SIGNAL-{phase}] {mint} score={score} "
                f"birth={t.birth_signal_score:.0f} price={t.price:.8f}"
            )
            print(f"  birth_signals: {t.birth_signals_passed}")
            if self.cfg.telegram_enabled:
                await self.telegram.send_signal_alert(signal, t)

    async def _empty(self):
        return {}

    async def _zero(self):
        return 0.0

    # --------------------------------------------------------
    # 11.4 MIGRATION POLLER & RESCAN + SWEEP
    # --------------------------------------------------------

    async def migration_poller(self):
        if not self.cfg.enable_migration_poller:
            return
        print("[migration-poller] started")
        while self.running:
            try:
                now = time.time()
                candidates: List[str] = []
                for mint, t in list(self.tokens.items()):
                    if t.is_migrated or t.signal_emitted:
                        continue
                    age_hours = (now - t.created_at) / 3600.0
                    if age_hours > self.cfg.migration_poll_track_hours:
                        continue
                    if t.source in ("pumpfun", "dexscreener"):
                        candidates.append(mint)
                    elif t.bonding_pct > 0:
                        candidates.append(mint)

                for mint in candidates[:50]:
                    t = self.tokens.get(mint)
                    if not t or t.is_migrated:
                        continue
                    status = await self.rpc.check_migration_status(mint)
                    if not status:
                        continue

                    t.is_migrated = True
                    t.pool_address = status.get("pool_address", "")
                    t.migration_detected_at = now
                    t.market_cap_usd = status.get("market_cap", 0.0)
                    if status.get("liquidity_usd", 0) > 0:
                        t.liquidity_usd = status["liquidity_usd"]
                    if status.get("price", 0) > 0:
                        t.price = status["price"]
                        self.price_history[mint].append((now, t.price))

                    created_at_pool = status.get("created_at", 0.0)
                    if created_at_pool > 0:
                        t.pool_age_minutes = (now - created_at_pool) / 60.0

                    t.scored = False
                    t.signal_emitted = False
                    await self.eval_queue.put(mint)
                    print(
                        f"[migration-poller] detected {mint} → "
                        f"pool {t.pool_address} via {status.get('dex_id', '')}"
                    )
            except Exception as e:
                print(f"[migration-poller-error] {e}")
            await asyncio.sleep(self.cfg.migration_poll_interval_sec)

    async def rescan_loop(self):
        sweep_counter = 0
        sweep_interval_ticks = max(1, self.cfg.sweep_interval_sec // 2)
        while self.running:
            try:
                now = time.time()
                while self.rescan_heap and self.rescan_heap[0][0] <= now:
                    _, mint = heapq.heappop(self.rescan_heap)
                    t = self.tokens.get(mint)
                    if t and not t.signal_emitted:
                        await self.eval_queue.put(mint)
                        print(
                            f"[rescan] requeue {mint} "
                            f"(attempt {t.rescan_count}/{self.cfg.max_rescan_count})"
                        )

                if self.cfg.enable_sweep:
                    sweep_counter += 1
                    if sweep_counter >= sweep_interval_ticks:
                        sweep_counter = 0
                        swept = 0
                        for mint, t in list(self.tokens.items()):
                            if t.signal_emitted or t.scored:
                                continue
                            age = now - t.created_at
                            if age > self.cfg.max_rescan_age_sec:
                                continue
                            if t.rescan_count >= self.cfg.max_rescan_count:
                                continue
                            t.scored = False
                            t.rescan_count += 1
                            await self.eval_queue.put(mint)
                            swept += 1
                        if swept > 0:
                            print(f"[sweep] requeue {swept} tokens")
            except Exception as e:
                print(f"[rescan-error] {e}")
            await asyncio.sleep(2.0)

    # --------------------------------------------------------
    # 11.5 SIGNAL CONSUMER
    # --------------------------------------------------------

    async def signal_consumer(self):
        while self.running:
            try:
                signal = await asyncio.wait_for(self.signal_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self.execute_entry(signal)
            except Exception as e:
                print(f"[entry-error] {signal.mint}: {e}")
            finally:
                self.signal_queue.task_done()

    async def execute_entry(self, signal: Signal):
        size_total = self.portfolio_usd * (self.cfg.position_size_pct / 100)
        if not self.positions.can_open(size_total, self.portfolio_usd):
            print(f"[risk] skip {signal.mint} (exposure/loss streak)")
            return
        pos = self.positions.open(signal, size_total * 0.3)
        print(
            f"[ENTRY-1] {signal.mint} 30% @ {signal.entry_price:.8f} "
            f"stop={pos.stop_price:.8f} birth_score={signal.birth_score:.0f}"
        )

    # --------------------------------------------------------
    # 11.6 MONITOR & EXIT
    # --------------------------------------------------------

    async def monitor_loop(self):
        while self.running:
            try:
                for mint in list(self.positions.positions.keys()):
                    history = self.price_history.get(mint)
                    if not history:
                        continue
                    _, price = history[-1]
                    actions = self.positions.update(mint, price)
                    for a in actions:
                        await self.execute_exit(mint, price, a)
                self.cache.cleanup()
            except Exception as e:
                print(f"[monitor-error] {e}")
            await asyncio.sleep(self.cfg.monitor_interval_sec)

    async def execute_exit(self, mint: str, price: float, action: str):
        pos = self.positions.positions.get(mint)
        if not pos:
            return
        if action == "stop_loss":
            pnl = (price - pos.entry_price) / pos.entry_price * 100
            print(f"[EXIT-SL] {mint} @ {price:.8f} pnl={pnl:.1f}%")
            self.logger.log_exit(mint, price, "stop_loss", pnl)
            if self.cfg.telegram_enabled:
                await self.telegram.send_exit_alert(mint, price, "stop_loss", pnl)
            self.positions.close(mint, pnl)
        elif action == "take_initials":
            print(f"[EXIT-50%] {mint} @ {price:.8f}")
            pos.remaining_pct = 50.0
            self.logger.log_exit(mint, price, "take_initials", 100.0)
            if self.cfg.telegram_enabled:
                await self.telegram.send_exit_alert(mint, price, "take_initials", 100.0)
        elif action == "trailing":
            pnl = (price - pos.entry_price) / pos.entry_price * 100
            print(f"[EXIT-TRAIL] {mint} @ {price:.8f} pnl={pnl:.1f}%")
            self.logger.log_exit(mint, price, "trailing", pnl)
            if self.cfg.telegram_enabled:
                await self.telegram.send_exit_alert(mint, price, "trailing", pnl)
            self.positions.close(mint, pnl)
        elif action == "time_stop":
            pnl = (price - pos.entry_price) / pos.entry_price * 100
            print(f"[EXIT-TIME] {mint} @ {price:.8f} pnl={pnl:.1f}%")
            self.logger.log_exit(mint, price, "time_stop", pnl)
            if self.cfg.telegram_enabled:
                await self.telegram.send_exit_alert(mint, price, "time_stop", pnl)
            self.positions.close(mint, pnl)

    # --------------------------------------------------------
    # 11.7 ORCHESTRATION
    # --------------------------------------------------------

    async def run(self):
        await self.rpc.start()
        await self.telegram.start()

        tasks = [
            asyncio.create_task(self.pumpportal_listener()),
            asyncio.create_task(self.raydium_listener()),
            asyncio.create_task(self.meteora_listener()),
            asyncio.create_task(self.dexscreener_discovery_loop()),
            asyncio.create_task(self.migration_poller()),
            asyncio.create_task(self.signal_consumer()),
            asyncio.create_task(self.monitor_loop()),
            asyncio.create_task(self.rescan_loop()),
        ]
        for i in range(self.cfg.worker_count):
            tasks.append(asyncio.create_task(self.worker(i)))

        print(f"[scanner] started, workers={self.cfg.worker_count}")
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
# 12. BACKTEST
# ============================================================

async def backtest(csv_file: str, cfg: Config):
    scorer = FastScorer(cfg)
    total = 0
    with open(csv_file) as f:
        for row in csv.DictReader(f):
            t = TokenState(
                mint=row["mint"], creator="", source="backtest",
                created_at=float(row["timestamp"]),
                bonding_pct=float(row["bonding_pct"]),
                price=float(row["price"]),
                liquidity_usd=float(row["liquidity"]),
                unique_buyers=set(f"w{i}" for i in range(int(row["unique_buyers"]))),
                volume_buys=float(row["volume_buys"]),
                volume_sells=float(row["volume_sells"]),
            )
            score, _, flags = scorer.score_bonding(t)
            if flags:
                continue
            if score >= cfg.min_conviction_score:
                total += 1
    print(f"[backtest] signals={total}")


# ============================================================
# 13. ENTRY POINT
# ============================================================

async def main():
    cfg = Config()
    scanner = HitAndRunScanner(cfg)

    port = int(os.getenv("PORT", "10000"))
    health = HealthServer(scanner)
    health_thread = threading.Thread(
        target=health.run, args=(port,), daemon=True
    )
    health_thread.start()
    print(f"[web] health server listening on :{port}")

    def handle_shutdown(signum, frame):
        print(f"[shutdown] signal {signum}, menutup scanner...")
        scanner.running = False

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    await scanner.run()


if __name__ == "__main__":
    asyncio.run(main())
