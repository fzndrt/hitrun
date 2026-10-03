# ============================================================
# HIT-AND-RUN SCANNER (v3 FINAL)
# ============================================================
# Tema: entry cepat (hit and run) TAPI aman dari rugpull.
#
# Perubahan kunci dibanding v1 & v2:
#  - TANPA Helius (berbayar). Diganti:
#      * three.ws API       -> data holders (gratis, no key)
#      * MagicBlock RPC     -> fallback getTokenLargestAccounts
#      * RugCheck API       -> LP lock, rug score, risks (gratis)
#  - Multi-source ingestion (PumpPortal + optional Raydium/Meteora)
#  - Worker pool paralel (8 worker default) untuk scan lebih banyak token
#  - Cache TTL untuk hemat rate-limit endpoint gratis
#  - Lapisan keamanan berlapis: bundle, sniper, holder concentration,
#    LP lock, rug score, dev holding, honeypot heuristic
#  - Entry bertahap, SL keras, TP bertahap, trailing, time stop
#  - Logging JSONL + hook backtest
# ============================================================

import asyncio
import json
import os
import time
import csv
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
    max_dev_holding_pct: float = 5.0
    require_lp_locked: bool = True
    min_lp_locked_pct: float = 95.0
    max_rugcheck_score: float = 60.0

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

    # --- Sumber data multi-source ---
    pumpportal_ws: str = "wss://pumpportal.fun/api/data"
    raydium_ws: str = "wss://api.raydium.io/v2/ws"
    meteora_ws: str = "wss://meteora.ag/ws"
    enable_raydium: bool = False
    enable_meteora: bool = False

    # --- Endpoint GRATIS (pengganti Helius) ---
    # three.ws API -> data holders tanpa API key
    threews_api_url: str = "https://three.ws/api/crypto"
    # MagicBlock RPC -> fallback getTokenLargestAccounts tanpa API key
    holder_rpc_url: str = "https://rpc.magicblock.app/mainnet"
    # RPC publik untuk fallback umum
    public_rpc_url: str = "https://solana-rpc.publicnode.com"
    # RugCheck API -> gratis, tanpa key
    rugcheck_url: str = "https://api.rugcheck.xyz/v1"

    # --- Konkurensi & worker pool ---
    worker_count: int = 8
    rpc_semaphore: int = 12
    evaluate_interval_sec: float = 1.0
    monitor_interval_sec: float = 2.0

    # --- Cache TTL (hemat rate limit endpoint gratis) ---
    cache_ttl_holders_sec: int = 15
    cache_ttl_lp_sec: int = 30
    cache_ttl_supply_sec: int = 60

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
    """Cache dengan TTL per entry untuk hemat rate-limit endpoint gratis."""

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
# 4. RPC CLIENT (GRATIS: three.ws + MagicBlock + RugCheck)
# ============================================================

class RpcClient:
    """
    Klien data on-chain GRATIS. Tidak butuh Helius.
    Urutan strategi get_holders:
      1) three.ws API   (paling mudah, sudah terformat)
      2) MagicBlock RPC (fallback, JSON-RPC langsung)
    get_lp_info: RugCheck API (gratis, tanpa key).
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

        # Strategy 1: three.ws
        holders = await self._holders_threews(mint)
        # Strategy 2: MagicBlock RPC
        if not holders:
            holders = await self._holders_rpc(mint, self.cfg.holder_rpc_url)
        # Strategy 3: public RPC fallback
        if not holders:
            holders = await self._holders_rpc(mint, self.cfg.public_rpc_url)

        if holders:
            self.cache.set(f"holders:{mint}", holders)
        return holders

    async def _holders_threews(self, mint: str) -> Dict[str, float]:
        """
        Ambil holders dari three.ws API.
        Struktur respons bisa berbeda; sesuaikan parser di sini.
        """
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

    # ---------- LP & RISK (RugCheck, gratis) ----------

    async def get_lp_info(
        self, mint: str
    ) -> Tuple[bool, float, str, float, List[str]]:
        """
        Return: (lp_locked, liquidity_usd, lp_pool, rug_score, red_flags)
        """
        cached = self.cache.get(f"lp:{mint}", self.cfg.cache_ttl_lp_sec)
        if cached is not None:
            return cached

        url = f"{self.cfg.rugcheck_url}/tokens/{mint}/report/summary"
        async with self.sem:
            try:
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        result = (False, 0.0, "", 100.0, ["rugcheck_unavailable"])
                        self.cache.set(f"lp:{mint}", result)
                        return result
                    data = await resp.json()
            except Exception:
                result = (False, 0.0, "", 100.0, ["rugcheck_error"])
                self.cache.set(f"lp:{mint}", result)
                return result

        score = float(data.get("score_normalised", 100.0))
        lp_locked_pct = float(data.get("lpLockedPct", 0.0) or 0.0)
        lp_locked = lp_locked_pct >= self.cfg.min_lp_locked_pct

        liquidity_usd = 0.0
        for r in data.get("risks", []) or []:
            if "liquidity" in (r.get("name", "") or "").lower():
                try:
                    liquidity_usd = float(
                        str(r.get("value", "0")).replace("$", "").replace(",", "")
                    )
                except ValueError:
                    pass

        red_flags: List[str] = []
        for r in data.get("risks", []) or []:
            level = r.get("level", "")
            name = r.get("name", "")
            if level == "danger":
                red_flags.append(f"rugcheck_danger:{name}")
            elif level == "warn" and "liquidity" in name.lower():
                red_flags.append(f"rugcheck_warn:{name}")

        lp_pool = ""
        markets = data.get("markets", []) or []
        if markets:
            lp_pool = markets[0].get("pubkey", "")

        result = (lp_locked, liquidity_usd, lp_pool, score, red_flags)
        self.cache.set(f"lp:{mint}", result)
        return result


# ============================================================
# 5. SECURITY ANALYZER
# ============================================================

class SecurityAnalyzer:
    """Deteksi bundle, sniper, dan konsentrasi holder."""

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
    ) -> Tuple[float, float]:
        filtered = {w: p for w, p in holders.items() if w != lp_pool}
        top10 = sum(sorted(filtered.values(), reverse=True)[:10])
        dev_pct = filtered.get("dev", 0.0)
        return top10, dev_pct


# ============================================================
# 6. FAST SCORER
# ============================================================

class FastScorer:
    """Skor cepat (data detik, bukan menit) untuk entry hit-and-run."""

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
# 7. POSITION MANAGER
# ============================================================

class PositionManager:
    """
    Exit plan disiplin:
      - Entry bertahap (30% dulu, sisanya tunggu konfirmasi)
      - Hard SL 35%
      - Take initials 50% di 2x (recover modal)
      - Trailing stop 25% untuk moon bag
      - Time stop 24 jam
      - Circuit breaker 3 loss berturut-turut
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

    def open(
        self, signal: Signal, size_usd: float, portfolio_usd: float
    ) -> Position:
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

        # Hard SL
        if price <= pos.stop_price:
            actions.append("stop_loss")
            return actions

        # Take initials di 2x
        if (
            not pos.initial_recovered
            and price >= pos.entry_price * self.cfg.take_initials_multiple
        ):
            actions.append("take_initials")
            pos.initial_recovered = True

        # Trailing setelah recover
        if pos.initial_recovered and price <= pos.trailing_price:
            actions.append("trailing")

        # Time stop
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
# 8. TRADE LOGGER
# ============================================================

class TradeLogger:
    """Catat sinyal & exit ke JSONL untuk audit dan backtest."""

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
# 9. SCANNER UTAMA
# ============================================================

class HitAndRunScanner:
    """
    Alur:
      WS multi-source -> tokens store -> eval_queue
      -> worker pool -> security check -> signal_queue
      -> signal_consumer (entry bertahap)
      -> monitor_loop (exit plan)
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.cache = TimedCache()
        self.rpc = RpcClient(cfg, self.cache)
        self.security = SecurityAnalyzer(cfg)
        self.scorer = FastScorer(cfg)
        self.positions = PositionManager(cfg)
        self.logger = TradeLogger(cfg.log_file)

        self.tokens: Dict[str, TokenState] = {}
        self.price_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=300))
        self.buyers_buffer: Dict[str, List[dict]] = defaultdict(list)
        self.signals: List[Signal] = []

        self.eval_queue: asyncio.Queue = asyncio.Queue()
        self.signal_queue: asyncio.Queue = asyncio.Queue()

        self.portfolio_usd = 1000.0
        self.running = True

    # --------------------------------------------------------
    # 9.1 INGESTION: WebSocket multi-source
    # --------------------------------------------------------

    async def pumpportal_listener(self):
        while self.running:
            try:
                async with websockets.connect(self.cfg.pumpportal_ws) as ws:
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    print("[ws:pumpfun] connected")
                    async for raw in ws:
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
    # 9.2 WORKER POOL: evaluasi paralel
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

        # --- Refresh data on-chain (paralel, gratis) ---
        holders_task = asyncio.create_task(self.rpc.get_holders(mint))
        lp_task = asyncio.create_task(self.rpc.get_lp_info(mint))
        holders, lp_info = await asyncio.gather(holders_task, lp_task)
        locked, liq_usd, lp_pool, rug_score, rug_flags = lp_info

        t.holders = holders
        t.liquidity_usd = liq_usd
        t.lp_locked = locked
        t.lp_pool = lp_pool
        t.rug_score = rug_score

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
        top10_pct, dev_pct = self.security.holder_concentration(holders, lp_pool)
        t.dev_holding_pct = dev_pct

        red_flags: List[str] = []
        if bundle_count > self.cfg.max_bundle_wallets:
            red_flags.append(f"bundle:{bundle_count}")
        if sniper_count > self.cfg.max_sniper_wallets:
            red_flags.append(f"sniper:{sniper_count}")
        if top10_pct > self.cfg.max_top10_holder_pct:
            red_flags.append(f"top10:{top10_pct:.1f}%")
        if dev_pct > self.cfg.max_dev_holding_pct:
            red_flags.append(f"dev_hold:{dev_pct:.1f}%")
        if self.cfg.require_lp_locked and not locked:
            red_flags.append("lp_not_locked")
        if liq_usd < self.cfg.min_liquidity_usd:
            red_flags.append(f"liq_low:${liq_usd:,.0f}")
        if rug_score > self.cfg.max_rugcheck_score:
            red_flags.append(f"rugcheck_score:{rug_score:.0f}")
        # Honeypot heuristic: tidak ada sell setelah 20 detik
        if t.sells_count == 0 and age > 20:
            red_flags.append("no_sell_yet_honeypot_risk")
        red_flags.extend(rug_flags)

        # --- Scoring ---
        score, reasons, score_flags = self.scorer.score(t)
        red_flags.extend(score_flags)
        t.scored = True

        if red_flags:
            print(f"[skip] {mint} score={score} flags={red_flags}")
            return

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

    # --------------------------------------------------------
    # 9.3 SIGNAL CONSUMER: entry bertahap
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
        """
        Entry bertahap: 30% di sinyal awal.
        Sisanya tunggu konfirmasi (dipanggil manual atau via logika tambahan).
        """
        size_total = self.portfolio_usd * (self.cfg.position_size_pct / 100)
        if not self.positions.can_open(size_total, self.portfolio_usd):
            print(f"[risk] skip {signal.mint} (exposure/loss streak)")
            return

        pos = self.positions.open(signal, size_total * 0.3, self.portfolio_usd)
        print(
            f"[ENTRY-1] {signal.mint} 30% @ {signal.entry_price:.8f} "
            f"stop={pos.stop_price:.8f}"
        )
        # TODO: eksekusi swap nyata (Jupiter/pump.fun buy)

    # --------------------------------------------------------
    # 9.4 MONITOR: exit plan
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
            self.positions.close(mint, pnl)

        elif action == "take_initials":
            print(f"[EXIT-50%] {mint} @ {price:.8f} (recover modal)")
            pos.remaining_pct = 50.0
            self.logger.log_exit(mint, price, "take_initials", 100.0)
            # TODO: swap sell 50%

        elif action == "trailing":
            pnl = (price - pos.entry_price) / pos.entry_price * 100
            print(f"[EXIT-TRAIL] {mint} @ {price:.8f} pnl={pnl:.1f}%")
            self.logger.log_exit(mint, price, "trailing", pnl)
            self.positions.close(mint, pnl)

        elif action == "time_stop":
            pnl = (price - pos.entry_price) / pos.entry_price * 100
            print(f"[EXIT-TIME] {mint} @ {price:.8f} pnl={pnl:.1f}%")
            self.logger.log_exit(mint, price, "time_stop", pnl)
            self.positions.close(mint, pnl)

    # --------------------------------------------------------
    # 9.5 ORCHESTRATION
    # --------------------------------------------------------

    async def run(self):
        await self.rpc.start()
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
            await self.rpc.close()


# ============================================================
# 10. BACKTEST SEDERHANA
# ============================================================

async def backtest(csv_file: str, cfg: Config):
    """
    Format CSV kolom:
      timestamp,mint,price,bonding_pct,volume_buys,volume_sells,
      unique_buyers,top10_pct,lp_locked,liquidity,dev_pct
    """
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
            score, reasons, flags = scorer.score(t)
            if flags:
                continue
            if score >= cfg.min_conviction_score:
                # Simulasi exit sederhana; sesuaikan dengan harga selanjutnya.
                # Placeholder: hitung win/loss berdasarkan aturan TP/SL.
                pass

    total = wins + losses
    win_rate = wins / total * 100 if total else 0
    avg_pnl = sum(pnls) / len(pnls) if pnls else 0
    print(
        f"[backtest] signals={total} win_rate={win_rate:.1f}% "
        f"avg_pnl={avg_pnl:.2f}%"
    )


# ============================================================
# 11. ENTRY POINT
# ============================================================

async def main():
    cfg = Config()
    scanner = HitAndRunScanner(cfg)
    await scanner.run()


if __name__ == "__main__":
    asyncio.run(main())
