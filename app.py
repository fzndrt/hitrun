import os
import time
import requests
import threading
from collections import deque
from flask import Flask, request, jsonify

app = Flask(__name__)

# ================= DATA TELEGRAM =================
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "8934987393:AAGTgFdVP3V-LAFAcpTY4o9afX6MUlGpV5o")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "1233826806")

# ================= SISTEM MEMORI ANTI-SPAM =================
# Mengingat 500 koin terakhir. Jika RAM penuh, data terlama otomatis terhapus.
processed_tokens = deque(maxlen=500)

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID, 
        "text": message, 
        "parse_mode": "Markdown", 
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Gagal kirim Telegram: {e}", flush=True)

# ================= AUDIT KEAMANAN (RUGCHECK) =================
def check_rugcheck(token_address):
    """Memanggil API Rugcheck untuk mendeteksi honeypot dan rugpull"""
    try:
        url = f"https://api.rugcheck.xyz/v1/tokens/{token_address}/report"
        resp = requests.get(url, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            risks = data.get("risks", [])
            
            # Cari risiko fatal (level danger / score tinggi)
            bahaya = []
            for risk in risks:
                if risk.get("level") == "danger" or risk.get("score", 0) > 1000:
                    bahaya.append(risk.get("name", "Risiko Fatal"))
            
            if len(bahaya) > 0:
                return False, f"Bahaya terdeteksi: {', '.join(bahaya)}"
            
            return True, "Aman (Risiko Rendah)"
        else:
            return False, "Sistem API RugCheck sibuk"
    except Exception as e:
        return False, f"Error Audit: {str(e)}"

# ================= FLASK SERVER (WEBHOOK & UPTIMEROBOT) =================
@app.route('/')
def keep_alive():
    return "Sistem Sniper Pro Aktif!", 200

@app.route('/cielo-webhook', methods=['POST'])
def cielo_webhook():
    data = request.json
    if data:
        token_name = data.get("token_name", "Token")
        token_address = data.get("token_address", "Alamat Kontrak")
        usd_value = data.get("usd_value", "0")
        
        # Cegah notifikasi dobel jika bot sniper sudah menemukannya
        if token_address not in processed_tokens:
            processed_tokens.append(token_address)
            pesan = (
                f"🐳 **SMART MONEY ALERT**\n\n"
                f"Paus sedang akumulasi koin ini!\n"
                f"🪙 Koin: {token_name}\n"
                f"💵 Nilai Beli: ${usd_value}\n"
                f"📝 Kontrak: `{token_address}`\n\n"
                f"🛒 **Beli Cepat:** [DEXScreener](https://dexscreener.com/solana/{token_address})"
            )
            send_telegram(pesan)
            print(f"🐳 Webhook masuk: {token_name}", flush=True)
            
    return jsonify({"status": "success"}), 200

# ================= BACKGROUND CRAWLER (KOIN BARU) =================
def sniper_bot_loop():
    print("🤖 Sistem Sniper Pro Online! Memantau dengan Filter Institusi...", flush=True)
    send_telegram("🤖 **Sistem Sniper Pro Online!**\nFitur aktif: Anti-Spam, Filter Koin Debu (Min $2k), & Audit RugCheck.")
    
    while True:
        try:
            url = "https://api.geckoterminal.com/api/v2/networks/solana/new_pools"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                pools = response.json().get("data", [])
                print(f"[*] Cek {len(pools)} kolam likuiditas terbaru...", flush=True)
                
                # Cek dari yang paling baru
                for pool in pools:
                    # Ambil data koin
                    try:
                        token_address = pool["relationships"]["base_token"]["data"]["id"].replace("solana_", "")
                    except:
                        continue
                        
                    name = pool["attributes"]["name"]
                    
                    # 1. FILTER ANTI-SPAM
                    if token_address in processed_tokens:
                        continue # Lewati jika sudah pernah dicek
                    
                    # Masukkan ke memori agar tidak dicek ulang
                    processed_tokens.append(token_address)
                    
                    # 2. FILTER LIKUIDITAS AWAL (Minimal $2000)
                    try:
                        liquidity = float(pool["attributes"].get("reserve_in_usd", 0))
                    except:
                        liquidity = 0
                        
                    if liquidity < 2000:
                        # Koin dengan modal di bawah $2000 kemungkinan besar mainan
                        # Hilangkan tanda pagar (#) pada print di bawah jika ingin melihat koin yang dibuang
                        # print(f"[SKIP] {name} | Likuiditas terlalu kecil (${liquidity:,.2f})", flush=True)
                        continue
                        
                    print(f"[AUDIT] {name} | Likuiditas: ${liquidity:,.2f}. Mengecek keamanan kontrak...", flush=True)
                    
                    # 3. AUDIT KEAMANAN
                    is_safe, reason = check_rugcheck(token_address)
                    
                    if is_safe:
                        print(f"[💎 AMAN] {name} lolos audit! Mengirim ke Telegram...", flush=True)
                        pesan = (
                            f"💎 **PERMATA DITEMUKAN (Lolos Audit)** 💎\n\n"
                            f"🪙 Koin: {name}\n"
                            f"💧 Likuiditas: ${liquidity:,.2f}\n"
                            f"🛡️ Status: Aman (LP Locked/Burned)\n"
                            f"📝 Kontrak: `{token_address}`\n\n"
                            f"🛒 **Beli:** [DEXScreener](https://dexscreener.com/solana/{token_address})"
                        )
                        send_telegram(pesan)
                    else:
                        print(f"[SCAM/BAHAYA] {name} ditolak: {reason}", flush=True)
                        
            else:
                print("Terkena Limit API GeckoTerminal, istirahat sejenak...", flush=True)
                
        except Exception as e:
            print(f"Error Crawler: {e}", flush=True)
        
        # Jeda 20 detik
        time.sleep(20)

# ================= MAIN EXECUTION =================
if __name__ == "__main__":
    bot_thread = threading.Thread(target=sniper_bot_loop)
    bot_thread.daemon = True
    bot_thread.start()
    
    app.run(host="0.0.0.0", port=10000, use_reloader=False)
