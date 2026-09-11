import os
import requests
import time
import threading
from flask import Flask, request, jsonify

# ================= KONFIGURASI UTAMA =================
NETWORK = "solana" 
MIN_LIQUIDITY = 10000
MAX_MARKET_CAP = 150000
MAX_HOLDER_PCT = 5.0 

# ================= MASUKKAN DATA TELEGRAM =================
TG_BOT_TOKEN = "MASUKKAN_TOKEN_BOT_TELEGRAM_ANDA_DI_SINI"
TG_CHAT_ID = "MASUKKAN_CHAT_ID_ANDA_DI_SINI"

# API Endpoints
GECKO_NEW_POOLS = f"https://api.geckoterminal.com/api/v2/networks/{NETWORK}/new_pools"
RUGCHECK_API = "https://api.rugcheck.xyz/v1/tokens/"

app = Flask(__name__)

class SmartScreener:
    def __init__(self):
        self.scanned_tokens = set()

    def kirim_telegram(self, pesan):
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TG_CHAT_ID, "text": pesan, "parse_mode": "Markdown", "disable_web_page_preview": True}
        try:
            requests.post(url, json=payload)
        except Exception as e:
            print(f"Gagal kirim Telegram: {e}")

    def cek_sosial_media(self, pool_data):
        links = pool_data.get("attributes", {}).get("websites", [])
        return len(links) > 0  

    def cek_akumulasi_volume(self, pool_address):
        try:
            url = f"https://api.geckoterminal.com/api/v2/networks/{NETWORK}/pools/{pool_address}/ohlcv/minute"
            res = requests.get(url, headers={'Accept': 'application/json'})
            if res.status_code != 200: return False
            
            data = res.json().get("data", {}).get("attributes", {}).get("ohlcv_list", [])
            if len(data) < 30: return False 
            
            recent_5m = data[:5] 
            past_25m = data[5:30]
            
            harga_tinggi = max(candle[2] for candle in past_25m)
            harga_rendah = min(candle[3] for candle in past_25m)
            if harga_rendah == 0: return False
            
            fluktuasi = (harga_tinggi - harga_rendah) / harga_rendah
            if fluktuasi >= 0.15: return False # Gagal: Harga tidak sideways
            
            avg_vol_lama = sum(candle[5] for candle in past_25m) / 25
            avg_vol_baru = sum(candle[5] for candle in recent_5m) / 5
            
            return avg_vol_baru > (avg_vol_lama * 3) # Lolos jika volume naik 3x lipat
        except:
            return False

    def cek_keamanan_onchain(self, token_address):
        try:
            res = requests.get(f"{RUGCHECK_API}{token_address}/report")
            if res.status_code != 200: return False
            
            report = res.json()
            
            top_holders = report.get("topHolders", [])
            for holder in top_holders:
                if holder.get("isContract", False): continue 
                if holder.get("pct", 100) > MAX_HOLDER_PCT: return False 
                    
            risks = report.get("risks", [])
            for risk in risks:
                name = risk.get("name", "").lower()
                if "mint" in name or "unlocked" in name or "honeypot" in name:
                    return False
                    
            return True
        except:
            return False

    def run_crawler(self):
        print("[*] Crawler & Auditor Aktif...")
        self.kirim_telegram("🤖 *Sistem Sniper Online!*\nMulai memantau koin baru 24/7...")
        
        while True:
            try:
                res = requests.get(GECKO_NEW_POOLS, headers={'Accept': 'application/json'})
                if res.status_code == 200:
                    pools = res.json().get("data", [])
                    
                    for pool in pools:
                        attr = pool.get("attributes", {})
                        token_address = pool["relationships"]["base_token"]["data"]["id"].split("_")[1]
                        pool_address = attr.get("address")
                        token_symbol = attr.get("name")
                        
                        if token_address in self.scanned_tokens: continue
                        self.scanned_tokens.add(token_address)
                        
                        liquidity = float(attr.get("reserve_in_usd") or 0)
                        fdv = float(attr.get("fdv_usd") or 0)
                        
                        if MIN_LIQUIDITY <= liquidity and fdv <= MAX_MARKET_CAP:
                            if not self.cek_sosial_media(pool): continue
                            if self.cek_akumulasi_volume(pool_address):
                                if self.cek_keamanan_onchain(token_address):
                                    pesan = (
                                        f"🚀 *POTENSI KOIN MELEDAK!*\n\n"
                                        f"🪙 *Koin:* {token_symbol}\n"
                                        f"💰 *MCap:* ${fdv:,.2f}\n"
                                        f"💧 *Liq:* ${liquidity:,.2f}\n\n"
                                        f"✅ *Audit Lolos:*\n"
                                        f"- LP Locked/Burned\n"
                                        f"- Top Holder < 5%\n"
                                        f"- Ada Sinyal Akumulasi Paus!\n\n"
                                        f"🔗 [Chart DEXScreener](https://dexscreener.com/solana/{token_address})"
                                    )
                                    self.kirim_telegram(pesan)
            except Exception as e:
                pass
            time.sleep(20)

screener = SmartScreener()

# ================= ENDPOINT KEEP-ALIVE HACK =================
@app.route('/')
def keep_alive():
    return "Sistem Sniper Aktif dan Berjalan!", 200

# ================= ENDPOINT WEBHOOK CIELO =================
@app.route('/cielo-webhook', methods=['POST'])
def receive_cielo_signal():
    data = request.json
    wallet = data.get("wallet", "Unknown")
    token_address = data.get("token_address")
    token_symbol = data.get("token_symbol")
    
    if data.get("type") == "buy" and screener.cek_keamanan_onchain(token_address):
        pesan = (
            f"🧠 *ALERT SMART MONEY!*\n\n"
            f"Whale `{wallet[:6]}...` baru saja masuk!\n"
            f"🪙 *Koin:* {token_symbol}\n"
            f"✅ Kontrak & Holder AMAN.\n\n"
            f"🔗 [Beli di DEXScreener](https://dexscreener.com/solana/{token_address})"
        )
        screener.kirim_telegram(pesan)
    return jsonify({"status": "success"}), 200

if __name__ == "__main__":
    # Jalankan crawler di background
    crawler_thread = threading.Thread(target=screener.run_crawler, daemon=True)
    crawler_thread.start()
    
    # Ambil PORT dari server (wajib untuk Render)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)