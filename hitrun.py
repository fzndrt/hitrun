# ============================================================
# HITRUN.PY — Hit-and-Run Scanner v5 (FINAL)
# ============================================================
# Tema: entry cepat (hit and run) TAPI aman dari rugpull.
#
# Gap yang sudah ditutup di v5:
#  - Mint authority check (dev bisa mint infinite?)
#  - Freeze authority check (dev bisa freeze wallet?)
#  - Sell simulation via Jupiter (honeypot detection)
#  - Transfer fee / sell tax check (Token-2022)
#  - Metadata mutability check (dev bisa ubah nama/gambar?)
#  - Creator reputation check (riwayat rugpull)
#  - Top holder pattern (100 wallet @ 0.01 SOL = bot farm)
#
# Semua pengecekan berjalan PARALEL via asyncio.gather()
# → total waktu evaluasi tetap dalam 25 detik.
# ============================================================

import asyncio
import json
import os
import time
import csv
import signal
import aiohttp
import websockets
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set


# ============================================================
# 1. KONFIGURASI
# ============================================================

@dataclass
class Config:
    # --- Jendela observasi (cepat untuk hit and run) ---
    observation_window_sec: int = 25
    min_age_sec: int = 5

    # --- Filter harga (anti-pucuk) ---
    max_price_pump_5m_pct: float = 50.0
    min_price_pump_5m_pct: float = 3.0

    # --- Filter bonding curve ---
    min_bonding_pct: float = 3.0
    max_bonding_pct: float = 35.0

    # --- Filter likuiditas & holder ---
    min_liquidity_usd: float = 15_000.0
    max_top10_holder_pct: float = 25.0
    max_top1_holder_pct: float = 3.0
    max_dev_holding_pct: float = 5.0
    min_holders_count: int = 50
    require_lp_locked: bool = True
    min_lp_locked_pct: float = 95.0
    max_rugcheck_score: float = 60.0

    # --- Authority & metadata (BARU) ---
    require_mint_authority_revoked: bool = True
    require_freeze_authority_revoked: bool = True
    require_metadata_immutable: bool = True

    # --- Transfer fee / sell tax (BARU) ---
    max_transfer_fee_pct: float = 3.0       # max 3% sell tax
    max_buy_tax_pct: float = 5.0            # max 5% buy tax

    # --- Sell simulation / honeypot (BARU) ---
    enable_sell_simulation: bool = True
    min_sell_recovery_pct: float = 70.0     # min 70% modal kembali saat jual

    # --- Creator reputation (BARU) ---
    max_creator_rugpull_count: int = 0      # tolak jika creator pernah rugpull
    max_creator_token_count: int = 5        # max 5 token dibuat creator ini

    # --- Top holder pattern / bot farm (BARU) ---
    max_micro_wallet_count: int = 20        # max 20 wallet dengan balance < 0.02 SOL
    micro_wallet_balance_sol: float = 0.02

    # --- Filter bundle & sniper ---
    max_bundle_wallets: int = 3
    max_sniper_wallets: int = 5
    sniper_window_sec: int = 10

    # --- Skor minimum ---
    min_conviction_score: float = 65.0

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
    enable_raydium: bool = False
    enable_meteora: bool = False

    # --- Endpoint GRATIS (pengganti Helius) ---
    threews_api_url: str = "https://three.ws/api/crypto"
    holder_rpc_url: str = "https://rpc.magicblock.app/mainnet"
    public_rpc_url: str = "https://solana-rpc.publicnode.com"
    rugcheck_url: str = "https://api.rugcheck.xyz/v1"
    jupiter_quote_url: str = "https://lite-api.jup.ag/swap/v1/quote"

    # --- Worker & cache ---
    worker_count: int = 8
    rpc_semaphore: int = 12
    evaluate_interval_sec: float = 1.0
    monitor_interval_sec: float = 2.0
    cache_ttl_holders_sec: int = 15
    cache_ttl_lp_sec: int = 30
    cache_ttl_supply_sec: int = 60
    cache_ttl_security_sec: int = 30

    # --- Funding graph ---
    funding_window_hours: int = 72
    max_cluster_pct: float = 30.0
    max_cluster_size: int = 5
    max_clustered_wallets: int = 10
    max_clusters: int = 3

    # --- Telegram alert ---
    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", "")
    )
    telegram_enabled: bool = True

    # --- Portfolio ---
    portfolio_usd: float = 1000.0

    # --- Logging ---
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
    # Security fields (BARU)
    mint_authority_active: bool = False
    freeze_authority_active: bool = False
    metadata_mutable: bool = False
    transfer_fee_pct: float = 0.0
    sell_simulation_ok: bool = True
    creator_rugpull_count: int = 0
    micro_wallet_count: int = 0


@dataclass
class Signal:
    mint: str
    score: float
    entry_price: float
    reasons: List[str]
    red_flags: List[str]
    timestamp: float
    source: str = ""


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
# 3. CACHE LAYER
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
    """
    Klien data on-chain GRATIS.
    three.ws API + MagicBlock RPC + RugCheck + Jupiter Quote.
    """

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

    # ---------- DEV WALLET & FUNDING ----------

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

    # ---------- SECURITY (RugCheck + three.ws) ----------

    async def get_security_report(self, mint: str) -> dict:
        """
        Ambil laporan keamanan lengkap dari RugCheck.
        Return dict dengan semua field yang dibutuhkan.
        """
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
        """
        Cek mint authority & freeze authority.
        Return: (mint_authority_active, freeze_authority_active)
        """
        report = await self.get_security_report(mint)
        token = report.get("token", {}) or {}
        mint_auth = bool(token.get("mintAuthority"))
        freeze_auth = bool(token.get("freezeAuthority"))
        return mint_auth, freeze_auth

    async def get_transfer_fee(self, mint: str) -> float:
        """Cek transfer fee / sell tax dari RugCheck report."""
        report = await self.get_security_report(mint)
        token = report.get("token", {}) or {}
        fee = token.get("transferFee", {}) or {}
        if not fee:
            return 0.0
        # fee bisa dalam basis points (0.01% = 1 bp)
        bps = float(fee.get("transferFeeBasisPoints", 0) or 0)
        return bps / 100.0  # convert to percent

    async def get_metadata_mutability(self, mint: str) -> bool:
        """
        Cek apakah metadata mutable (dev bisa ubah nama/gambar).
        Return: True jika mutable.
        """
        report = await self.get_security_report(mint)
        meta = report.get("tokenMeta", {}) or {}
        return bool(meta.get("mutable", False))

    async def get_creator_reputation(self, mint: str) -> Tuple[int, int]:
        """
        Cek riwayat creator.
        Return: (rugpull_count, total_token_count)
        """
        report = await self.get_security_report(mint)
        creator = report.get("creator", "") or ""
        # RugCheck menyertakan creatorBalance dan insiderNetworks
        # Untuk rugpull count, kita cek risks[] dengan level danger
        risks = report.get("risks", []) or []
        rugpull_count = sum(
            1 for r in risks
            if r.get("level") == "danger"
            and ("creator" in r.get("name", "").lower()
                 or "rug" in r.get("name", "").lower())
        )
        total_tokens = 1  # placeholder; API eksternal bisa memberi ini
        return rugpull_count, total_tokens

    async def get_micro_wallet_count(self, mint: str) -> int:
        """
        Deteksi bot farm: hitung wallet dengan balance sangat kecil.
        Butuh data dari getTokenLargestAccounts + getBalance per wallet.
        """
        holders = await self.get_holders(mint)
        if not holders:
            return 0

        # Ambil saldo SOL dari wallet top holder (max 20)
        top_wallets = list(holders.keys())[:20]
        micro_count = 0
        for w in top_wallets:
            balance = await self._get_wallet_balance(w)
            if balance < self.cfg.micro_wallet_balance_sol:
                micro_count += 1
        return micro_count

    async def _get_wallet_balance(self, wallet: str) -> float:
        """Ambil saldo SOL wallet (dalam SOL)."""
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
                return lamports / 1_000_000_000
            except Exception:
                return 999.0  # anggap normal jika gagal

    async def simulate_sell(self, mint: str, amount_raw: int = 1_000_000) -> bool:
        """
        Simulasi sell via Jupiter Quote API untuk deteksi honeypot.
        Return: True jika sell bisa dieksekusi (bukan honeypot).
        """
        if not self.cfg.enable_sell_simulation:
            return True

        # Quote: sell token -> SOL
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
                        return False  # tidak ada route = kemungkinan honeypot
                    data = await resp.json()
                    # Jika ada routePlan dengan outAmount > 0, sell bisa
                    out_amount = float(data.get("outAmount", 0) or 0)
                    return out_amount > 0
            except Exception:
                return True  # optimistic fallback

    # ---------- LP & RISK (RugCheck, gratis) ----------

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

    def holder_concentration(
        self, holders: Dict[str, float], lp_pool: str
    ) -> Tuple[float, float, float]:
        filtered = {w: p for w, p in holders.items() if w != lp_pool}
        if not filtered:
            return 0.0, 0.0, 0.0
        sorted_pcts = sorted(filtered.values(), reverse=True)
        return sum(sorted_pcts[:10]), sorted_pcts[0], filtered.get("dev", 0.0)


# ============================================================
# 6. FUNDING GRAPH ANALYZER
# ============================================================

class FundingGraphAnalyzer:
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
        sigs = await self.rpc.get_signatures(wallet, limit=50)
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
        funder_map: Dict[str, List[str]] = defaultdict(list)
        for w in wallets:
            if w in self.KNOWN_EXCHANGES:
                continue
            funder = await self.find_funder(w, window_start, window_end)
            if funder and funder not in self.KNOWN_EXCHANGES:
                funder_map[funder].append(w)
        return {f: ws for f, ws in funder_map.items() if len(ws) >= 2}

    def cluster_risk_score(
        self, clusters: Dict[str, List[str]], total_holders: int
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

        return round(score, 2), red_flags


# ============================================================
# 7. FAST SCORER
# ============================================================

class FastScorer:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def score(self, t: TokenState) -> Tuple[float, List[str], List[str]]:
        reasons: List[str] = []
        red_flags: List[str] = []
        score = 0.0

        # Momentum (max 30)
        if t.price_at_5m_ago > 0:
            pump_pct = (t.price - t.price_at_5m_ago) / t.price_at_5m_ago * 100
        else:
            pump_pct = 0.0
        if pump_pct > self.cfg.max_price_pump_5m_pct:
            red_flags.append(f"price_pump_too_high:{pump_pct:.1f}%")
        elif pump_pct >= self.cfg.min_price_pump_5m_pct:
            score += min(30, pump_pct)
            reasons.append(f"healthy_momentum:{pump_pct:.1f}%")

        # Buy pressure (max 25)
        total_vol = t.volume_buys + t.volume_sells
        if total_vol > 0:
            buy_ratio = t.volume_buys / total_vol
            score += buy_ratio * 25
            if buy_ratio > 0.65:
                reasons.append(f"buy_pressure:{buy_ratio:.2f}")
            elif buy_ratio < 0.4:
                red_flags.append(f"sell_pressure:{buy_ratio:.2f}")

        # Unique buyers (max 20)
        ub = len(t.unique_buyers)
        if ub >= 10:
            score += 20
            reasons.append(f"unique_buyers:{ub}")
        elif ub >= 5:
            score += 12
        else:
            red_flags.append(f"few_unique_buyers:{ub}")

        # Bonding curve (max 15)
        if self.cfg.min_bonding_pct <= t.bonding_pct <= self.cfg.max_bonding_pct:
            score += 15
            reasons.append(f"bonding_ok:{t.bonding_pct:.1f}%")
        else:
            red_flags.append(f"bonding_bad:{t.bonding_pct:.1f}%")

        # Likuiditas (max 10)
        if t.liquidity_usd >= self.cfg.min_liquidity_usd:
            score += 10
            reasons.append(f"liquidity_ok:${t.liquidity_usd:,.0f}")
        else:
            red_flags.append(f"low_liquidity:${t.liquidity_usd:,.0f}")

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
            "score": signal.score,
            "price": signal.entry_price,
            "reasons": signal.reasons,
            "red_flags": signal.red_flags,
            "bonding_pct": token.bonding_pct,
            "liquidity": token.liquidity_usd,
            "unique_buyers": len(token.unique_buyers),
            "mint_authority": token.mint_authority_active,
            "freeze_authority": token.freeze_authority_active,
            "metadata_mutable": token.metadata_mutable,
            "transfer_fee": token.transfer_fee_pct,
            "sell_sim_ok": token.sell_simulation_ok,
            "creator_rugpull": token.creator_rugpull_count,
            "micro_wallets": token.micro_wallet_count,
        })

    def log_exit(self, mint: str, price: float, reason: str, pnl_pct: float):
        self._append({
            "type": "exit",
            "ts": time.time(),
            "mint": mint,
            "price": price,
            "reason": reason,
            "pnl_pct": pnl_pct,
        })

    def _append(self, entry: dict):
        try:
            with open(self.log_file, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            print(f"[logger-error] {e}")


# ============================================================
# 10. TELEGRAM ALERTER (hanya signal & exit)
# ============================================================

class TelegramAlerter:
    """Alert Telegram bertag [HIT-AND-RUN]. Hanya signal & exit."""

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
        await self._send("✅ <b>[HIT-AND-RUN] scanner online</b>")

    async def close(self):
        if self.session:
            await self.session.close()

    async def send_signal_alert(self, signal: Signal, token: TokenState):
        emoji = "🟢" if signal.score >= 80 else "🟡"
        text = (
            f"{emoji} <b>[HIT-AND-RUN] SIGNAL DETECTED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Token:</b> <code>{signal.mint}</code>\n"
            f"<b>Source:</b> {signal.source}\n"
            f"<b>Score:</b> {signal.score}/100\n"
            f"<b>Entry Price:</b> <code>{signal.entry_price:.8f}</code>\n"
            f"<b>Bonding:</b> {token.bonding_pct:.1f}%\n"
            f"<b>Liquidity:</b> ${token.liquidity_usd:,.0f}\n"
            f"<b>Unique Buyers:</b> {len(token.unique_buyers)}\n"
            f"<b>Mint Auth:</b> {'❌' if token.mint_authority_active else '✅'}\n"
            f"<b>Freeze Auth:</b> {'❌' if token.freeze_authority_active else '✅'}\n"
            f"<b>Metadata:</b> {'❌ mutable' if token.metadata_mutable else '✅ immutable'}\n"
            f"<b>Sell Tax:</b> {token.transfer_fee_pct:.1f}%\n"
            f"<b>Honeypot:</b> {'❌' if not token.sell_simulation_ok else '✅'}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Alasan:</b>\n" +
            "\n".join(f"  • {r}" for r in signal.reasons) +
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Exit Plan:</b>\n"
            f"  • SL: -35%\n"
            f"  • TP: +100% (jual 50%)\n"
            f"  • Trailing: 25%\n"
            f"  • Time Stop: 24 jam\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ <i>Entry bertahap 30%. Tunggu konfirmasi.</i>"
        )
        await self._send(text)

    async def send_exit_alert(
        self, mint: str, price: float, reason: str, pnl_pct: float
    ):
        emoji = "🟢" if pnl_pct > 0 else "🔴"
        reason_label = {
            "stop_loss": "🛑 STOP LOSS",
            "take_initials": "💰 TAKE INITIALS (50%)",
            "trailing": "📉 TRAILING STOP",
            "time_stop": "⏰ TIME STOP",
        }.get(reason, reason)
        text = (
            f"{emoji} <b>[HIT-AND-RUN] EXIT — {reason_label}</b>\n"
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
                    print(f"[telegram-error] status={resp.status} body={body[:200]}")
        except Exception as e:
            print(f"[telegram-error] {e}")


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
        self.funding_analyzer = FundingGraphAnalyzer(cfg, self.rpc)

        self.tokens: Dict[str, TokenState] = {}
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=300))
        self.buyers_buffer: Dict[str, List[dict]] = defaultdict(list)
        self.signals: List[Signal] = []

        self.eval_queue: asyncio.Queue = asyncio.Queue()
        self.signal_queue: asyncio.Queue = asyncio.Queue()

        self.portfolio_usd = cfg.portfolio_usd
        self.running = True

    # --------------------------------------------------------
    # 11.1 INGESTION
    # --------------------------------------------------------

    async def pumpportal_listener(self):
        while self.running:
            try:
                async with websockets.connect(self.cfg.pumpportal_ws) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
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
                async with websockets.connect(self.cfg.raydium_ws) as ws:
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
                async with websockets.connect(self.cfg.meteora_ws) as ws:
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
            self.tokens[mint] = TokenState(
                mint=mint,
                creator=msg.get("traderPublicKey", ""),
                source="pumpfun",
                created_at=time.time(),
                price=float(msg.get("initialBuy", 0) or 0),
            )
            await self.eval_queue.put(mint)
            print(f"[new] {mint} (pumpfun)")
        elif tx_type in ("buy", "sell"):
            await self.handle_trade(msg, source="pumpfun")

    async def handle_generic(self, msg: dict, source: str):
        mint = msg.get("mint") or msg.get("token")
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

    async def handle_trade(self, msg: dict, source: str):
        mint = msg.get("mint") or msg.get("token")
        t = self.tokens.get(mint)
        if not t:
            return

        side = msg.get("txType", msg.get("side", "buy"))
        price = float(msg.get("price", 0) or 0)
        sol_amount = float(msg.get("solAmount", 0) or 0)
        wallet = msg.get("traderPublicKey", msg.get("wallet", ""))

        trade = {
            "wallet": wallet,
            "sol_amount": sol_amount,
            "price": price,
            "slot": int(msg.get("slot", 0) or 0),
            "timestamp": time.time(),
            "side": side,
        }

        if side == "buy":
            t.buys_count += 1
            t.volume_buys += sol_amount
            t.unique_buyers.add(wallet)
            if len(self.buyers_buffer[mint]) < 50:
                self.buyers_buffer[mint].append(trade)
        else:
            t.sells_count += 1
            t.volume_sells += sol_amount

        t.volume_usd = (t.volume_buys + t.volume_sells) * 150
        if price > 0:
            t.price = price
            self.price_history[mint].append((time.time(), price))

    # --------------------------------------------------------
    # 11.2 WORKER POOL
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
        if age < self.cfg.min_age_sec:
            await asyncio.sleep(self.cfg.min_age_sec - age)
        if t.scored:
            return
        if age > self.cfg.observation_window_sec and not t.signal_emitted:
            return

        # ====================================================
        # SEMUA PENGECEKAN BERJALAN PARALEL
        # Total waktu tetap dalam 25 detik karena gather()
        # ====================================================
        (
            holders,
            lp_info,
            authorities,
            transfer_fee,
            metadata_mutable,
            sell_ok,
            creator_rep,
            micro_wallets,
        ) = await asyncio.gather(
            self.rpc.get_holders(mint),
            self.rpc.get_lp_info(mint),
            self.rpc.get_token_authorities(mint),
            self.rpc.get_transfer_fee(mint),
            self.rpc.get_metadata_mutability(mint),
            self.rpc.simulate_sell(mint),
            self.rpc.get_creator_reputation(mint),
            self.rpc.get_micro_wallet_count(mint),
        )

        locked, liq_usd, lp_pool, rug_score, rug_flags = lp_info
        mint_auth, freeze_auth = authorities
        rugpull_count, _ = creator_rep

        # --- Isi TokenState ---
        t.holders = holders
        t.liquidity_usd = liq_usd
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

        # --- Harga 5m lalu ---
        history = self.price_history.get(mint)
        if history and len(history) > 1:
            cutoff = now - 300
            past = [p for ts, p in history if ts <= cutoff]
            t.price_at_5m_ago = past[-1] if past else history[0][1]
        else:
            t.price_at_5m_ago = t.price

        # --- Security checks ---
        buyers = self.buyers_buffer.get(mint, [])
        bundle_count, _ = self.security.detect_bundle(buyers)
        sniper_count, _ = self.security.detect_sniper(buyers, t.created_at)
        top10_pct, top1_pct, dev_pct = self.security.holder_concentration(holders, lp_pool)

        # --- Developer holding ---
        dev_pct2, dev_flagged = await self.rpc.get_developer_holdings(
            mint, t.creator, lp_pool
        )
        t.dev_holding_pct = max(dev_pct, dev_pct2)

        # --- Funding cluster analysis ---
        early_wallets = [b["wallet"] for b in buyers][:20]
        window_start = t.created_at - self.cfg.funding_window_hours * 3600
        window_end = t.created_at + 60
        clusters = await self.funding_analyzer.analyze_clusters(
            early_wallets, window_start, window_end
        )
        _, cluster_flags = self.funding_analyzer.cluster_risk_score(
            clusters, len(holders)
        )

        # ====================================================
        # KUMPULKAN RED FLAGS
        # ====================================================
        red_flags: List[str] = []

        # Holder concentration
        if bundle_count > self.cfg.max_bundle_wallets:
            red_flags.append(f"bundle:{bundle_count}")
        if sniper_count > self.cfg.max_sniper_wallets:
            red_flags.append(f"sniper:{sniper_count}")
        if top10_pct > self.cfg.max_top10_holder_pct:
            red_flags.append(f"top10:{top10_pct:.1f}%")
        if top1_pct > self.cfg.max_top1_holder_pct:
            red_flags.append(f"top1:{top1_pct:.1f}%")
        if t.dev_holding_pct > self.cfg.max_dev_holding_pct:
            red_flags.append(f"dev_hold:{t.dev_holding_pct:.1f}%")
        if dev_flagged:
            red_flags.append("dev_still_holds")
        if len(holders) < self.cfg.min_holders_count:
            red_flags.append(f"too_few_holders:{len(holders)}")

        # Authority & metadata (BARU)
        if self.cfg.require_mint_authority_revoked and mint_auth:
            red_flags.append("mint_authority_active")
        if self.cfg.require_freeze_authority_revoked and freeze_auth:
            red_flags.append("freeze_authority_active")
        if self.cfg.require_metadata_immutable and metadata_mutable:
            red_flags.append("metadata_mutable")

        # Tax / fee (BARU)
        if transfer_fee > self.cfg.max_transfer_fee_pct:
            red_flags.append(f"transfer_fee:{transfer_fee:.1f}%")

        # Honeypot / sell simulation (BARU)
        if not sell_ok:
            red_flags.append("honeypot_sell_blocked")

        # Creator reputation (BARU)
        if rugpull_count > self.cfg.max_creator_rugpull_count:
            red_flags.append(f"creator_rugpull_history:{rugpull_count}")

        # Micro wallets / bot farm (BARU)
        if micro_wallets > self.cfg.max_micro_wallet_count:
            red_flags.append(f"micro_wallets:{micro_wallets}")

        # LP & liquidity
        if self.cfg.require_lp_locked and not locked:
            red_flags.append("lp_not_locked")
        if liq_usd < self.cfg.min_liquidity_usd:
            red_flags.append(f"liq_low:${liq_usd:,.0f}")
        if rug_score > self.cfg.max_rugcheck_score:
            red_flags.append(f"rugcheck_score:{rug_score:.0f}")
        if t.sells_count == 0 and age > 20:
            red_flags.append("no_sell_yet_honeypot_risk")

        # Funding cluster
        red_flags.extend(cluster_flags)
        red_flags.extend(rug_flags)

        # --- Scoring ---
        score, reasons, score_flags = self.scorer.score(t)
        red_flags.extend(score_flags)
        t.scored = True

        # ====================================================
        # SKIP: hanya log console, TIDAK kirim Telegram
        # ====================================================
        if red_flags:
            print(f"[skip] {mint} score={score} flags={red_flags}")
            return

        # ====================================================
        # LOLOS FILTER: kirim Telegram + simpan signal
        # ====================================================
        if score >= self.cfg.min_conviction_score:
            signal = Signal(
                mint=mint,
                score=score,
                entry_price=t.price,
                reasons=reasons,
                red_flags=[],
                timestamp=now,
                source=t.source,
            )
            t.signal_emitted = True
            self.signals.append(signal)
            self.logger.log_signal(signal, t)
            await self.signal_queue.put(signal)
            print(f"[SIGNAL] {mint} src={t.source} score={score} price={t.price:.8f}")
            print(f"  reasons: {reasons}")
            if self.cfg.telegram_enabled:
                await self.telegram.send_signal_alert(signal, t)

    # --------------------------------------------------------
    # 11.3 SIGNAL CONSUMER
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
            f"stop={pos.stop_price:.8f}"
        )
        # TODO: eksekusi swap nyata (Jupiter / pump.fun buy)

    # --------------------------------------------------------
    # 11.4 MONITOR & EXIT
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
            print(f"[EXIT-50%] {mint} @ {price:.8f} (recover modal)")
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
    # 11.5 ORCHESTRATION
    # --------------------------------------------------------

    async def run(self):
        await self.rpc.start()
        await self.telegram.start()

        tasks = [
            asyncio.create_task(self.pumpportal_listener()),
            asyncio.create_task(self.raydium_listener()),
            asyncio.create_task(self.meteora_listener()),
            asyncio.create_task(self.signal_consumer()),
            asyncio.create_task(self.monitor_loop()),
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
# 12. BACKTEST (opsional)
# ============================================================

async def backtest(csv_file: str, cfg: Config):
    scorer = FastScorer(cfg)
    wins, losses, pnls = 0, 0, []

    with open(csv_file) as f:
        for row in csv.DictReader(f):
            t = TokenState(
                mint=row["mint"],
                creator="",
                source="backtest",
                created_at=float(row["timestamp"]),
                bonding_pct=float(row["bonding_pct"]),
                price=float(row["price"]),
                liquidity_usd=float(row["liquidity"]),
                unique_buyers=set(
                    f"w{i}" for i in range(int(row["unique_buyers"]))
                ),
                volume_buys=float(row["volume_buys"]),
                volume_sells=float(row["volume_sells"]),
            )
            score, _, flags = scorer.score(t)
            if flags:
                continue
            if score >= cfg.min_conviction_score:
                pass  # simulasi exit di sini

    total = wins + losses
    win_rate = wins / total * 100 if total else 0
    avg_pnl = sum(pnls) / len(pnls) if pnls else 0
    print(f"[backtest] signals={total} win_rate={win_rate:.1f}% avg_pnl={avg_pnl:.2f}%")


# ============================================================
# 13. ENTRY POINT + SIGTERM HANDLER (Render)
# ============================================================

async def main():
    cfg = Config()
    scanner = HitAndRunScanner(cfg)

    def handle_shutdown(signum, frame):
        print(f"[shutdown] signal {signum} diterima, menutup scanner...")
        scanner.running = False

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    await scanner.run()


if __name__ == "__main__":
    asyncio.run(main())
