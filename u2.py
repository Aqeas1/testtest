# bot.py
# -*- coding: utf-8 -*-
# pip install aiogram==3.13.1 aiohttp pillow python-dateutil

import asyncio
import json
import logging
import os
import html
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, List, Tuple

import aiohttp
from PIL import Image
from io import BytesIO
from aiogram.types import BufferedInputFile
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery, ChatJoinRequest,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
)

MAX_TG_UPLOAD_BYTES = 48 * 1024 * 1024  # ~48 МБ

# ===================== 🔧 КОНФИГ =====================

TELEGRAM_TOKEN = "8218214415:AAGZYr6n94w9_5nXj6VBwahgqvoOhf2dKvM"
ADMIN_ID = 7510524298
REQUIRED_CHANNEL_ID = -1003235129860
REQUIRED_CHANNEL_INVITE = "https://t.me/+r3gB2jGWssQyNjVi"
LOG_CHAT_ID = -1003283949256
DB_PATH = "db.json"
DB_QUEUE_PATH = "db_queue.json"

PAY_CARD = "2204120119962344"
# 🔐 CryptoBot
CRYPTO_PAY_TOKEN = "488948:AAyPXFPlloJgKYcoKNOhaDB4CbGnBIHi2qH"
CRYPTO_API_URL = "https://pay.crypt.bot/api/"
CRYPTO_ASSETS = ["TON", "USDT", "BTC", "TRX"]


# 🎯 пресет-промпты (под себя)
GIF_1 = "The girl takes off all her clothes, exposes her breasts and remains naked, she does it all quickly"
GIF_2 = "A man's penis appears with an Erection and puts the penis in the girl's mouth, and the girl calmly takes the penis deeper into her mouth and looks at the man from below to above, the man inserts the penis deeper into the girl's mouth several times"
GIF_3 = "A man's penis appears with an Erection and puts the penis in the girl's mouth, and the girl calmly takes the penis deeper into her mouth and at this time takes off all her clothes while remaining naked and looks at the man from below to above, the man inserts the penis deeper into the girl's mouth several times"
GIF_4 = "The girl quickly takes off her pants and the frame changes to how the girl stands with cancer and the penis enters her vagina, the penis enters the girl's vagina completely, and quickly exits and inserts completely into the vagina again and so many times"

PH_1 = "Completely remove clothes, leave naked"
PH_2 = "Strip down to your underwear, lace bra and panties"
PH_3 = "Change the girl's clothes to a BDSM costume"
PH_4 = "Swap the girl's clothes for a bikini"
PH_5 = "Change the girl's clothes to a shirt and a short skirt, the shirt is translucent through which the girl's body and the nipples of the breasts are visible"
PH_6 = "Remove the clothes from the girl, add a man from behind the girl who massages the girl's breasts with his hands"
PH_7 = "Remove the clothes from the girl, the girl's hands are doing breast massage"
PH_8 = "Remove the clothes from the girl, add a towel on the girl's hips"
PH_9 = "Remove the clothes from the girl, add a towel to the girl's entire body up to her chest"
PH_10 = "Remove the clothes from the girl, Add a man with a penis near the girl's face"
PH_11 = "The girl's hands are holding up her clothes, half of her bare breasts are visible"
PH_12 = "Remove the clothes from the girl and make the girl's hand lowered to the bottom and the girl's fingers are in her clitoris"
PH_13 = "Expand the photo and make the girl's bottom in just her Nothing  and have the girl lift her legs and show them to the camera, The girl is lying on the sofa, her hand in her vagina"
PH_14 = "Change it so that the girl lies on her stomach on the bed and you can see her ass from behind, facing the camera"
PH_15 = "Change it so that the girl is lying on her stomach on the bed and you can see her ass from behind, facing the camera, and cum is pouring out of her vagina"
PH_16 = "Add cum on a girl's face"
PH_17 = "Undress the girl, make sure that she is on top of the man and the penis is in the girl's vagina"
PH_18 = "Undress the girl, make her lie on her stomach on the bed with her back arched facing the camera and take a first-person photo of a man whose penis is in the girl's vagina"
PH_19 = "Change the photo so that the girl will be naked, and between the tits of the girl in the first person there will be a dick between the tits of the girl, the girl squeezes her breasts with her hands around the penis"
PH_20 = "Change the photo so that the girl is completely naked in a bubble bath"
PH_21 = "Undress the girl, make her lie on her stomach on the bed, arching her back facing the camera, and take a first-person picture of a man whose penis is in the girl's vagina, from which sperm is pouring"
PH_22 = "Take off your clothes completely to stay naked, add cum to your chest"
PH_23 = "Change the photo so that the girl will show her legs in white stockings to the camera, she will only be wearing panties from below"
PH_24 = "Change the photo so that the girl will be lying on the couch completely naked, her arms and legs tied with ropes and her mouth taped shut"
PH_25 = "Change the image so the girl is kneeling naked and there is a gun to the girl's head in first person"
PH_26 = "Change the photo so that the girl is completely naked, and there is a man standing next to him with his hand between the girl's legs, and the man's fingers are in the girl's vagina."

PHOTO_PRESETS = [
    ("🔞Раздеть", PH_1),
    ("🩲Нижнее белье", PH_2),
    ("🍒BDSM", PH_3),
    ("👙Бикини", PH_4),
    ("👕Школьная прозрачная рубашка", PH_5),
    ("🤲Трогать грудь девушки", PH_6),
    ("👐Массаж груди(сама)", PH_7),
    ("🛁Полотенце на бедрах", PH_8),
    ("🛁Полотенце полностью", PH_9),
    ("🍆Раздеть+Мужчина рядом", PH_10),
    ("👚Приподнять одежду", PH_11),
    ("🍓Раздеть+рука в низ", PH_12),
    ("👣Раздвинуть ноги", PH_13),
    ("🍑Жопой к камере", PH_14),
    ("🍑💦Жопой к камере+сперма", PH_15),
    ("💦Сперма на лицо", PH_16),
    ("🍓🍆На члене сверху", PH_17),
    ("🍓🍆Раком", PH_18),
    ("🍒🍆Титфак", PH_19),
    ("💦🛁Пенная ванна+раздеть", PH_20),
    ("🥵💦🍆Раком+сперма", PH_21),
    ("🔞Раздеть+сперма на грудь", PH_22),
    ("👣Раздвинуть ноги+чулки", PH_23),
    ("🪢Раздеть+связанная", PH_24),
    ("🔫Раздеть+угрожать", PH_25),
    ("🔞Раздеть+трогают киску", PH_26),
]


GIF_PRESETS = [
    ("🔞Снять одежду", GIF_1),
    ("🥵Минет", GIF_2),
    ("🔥Минет+снять одежду", GIF_3),
    ("🍓Догги стайл", GIF_4),
]

# 🧠 Токены: ОТДЕЛЬНО basic/premium + фото/видео
BASIC_VIDEO_TOKENS: List[str] = [
    "pbo_pat_Msm0uY5Hi2gFW8ouejqAfo.Ei1v9DzwYojxZLqi0NfcMqwwx4JSCKxS7SdB9EtPhFK2",
    "pbo_pat_ZpB1UDHG69a5EMAvUdQSxD.CW2yFIet8W7aoXAfrrjGJZXLRsKWKAJaQQyeSRmjOwDH",
    "pbo_pat_GG3d5Zh5KKRlFgklbP89gy.AE970cSkqa1da7Cc3pUKfTfgPbdvIv7umZjGb4GBuTqH",
    "pbo_pat_qmk48mhBEG7HvLRkf02VCf.pJOJroJBvHZnVG14J5uggtPTVl17mH6nRnHF5zp1EPJW",
]

BASIC_PHOTO_TOKENS: List[str] = [
    "pbo_pat_Msm0uY5Hi2gFW8ouejqAfo.Ei1v9DzwYojxZLqi0NfcMqwwx4JSCKxS7SdB9EtPhFK2",
    "pbo_pat_ZpB1UDHG69a5EMAvUdQSxD.CW2yFIet8W7aoXAfrrjGJZXLRsKWKAJaQQyeSRmjOwDH",
    "pbo_pat_GG3d5Zh5KKRlFgklbP89gy.AE970cSkqa1da7Cc3pUKfTfgPbdvIv7umZjGb4GBuTqH",
    "pbo_pat_qmk48mhBEG7HvLRkf02VCf.pJOJroJBvHZnVG14J5uggtPTVl17mH6nRnHF5zp1EPJW",
]

PREMIUM_VIDEO_TOKENS: List[str] = [
    "pbo_pat_6P8uxYg1EYrqjcMSQMgmK5.nfTgTpBIAxQy5Eb4unjEPTgGyZHHaVvnViilaneCXFv3",
    "pbo_pat_B6oimfjA2qf9aQ6h9NZkPD.12xhz4B8mPFpJKcTRb59bwzF4majTVSSe2ICqpiEQdQV",
    "pbo_pat_gmY9JexVeM75mexHGivaFN.1v8C8SZTpwQ4JgkkspSWRjp67AZMLwWajqYB4qhRQxsj",
    "pbo_pat_yhFWmG9tXumJwNkwS66Nj1.erRZIADNAhVBdvVFQGlG82S9liF5I0iFXdFw3X0klbUv",
]

PREMIUM_PHOTO_TOKENS: List[str] = [
    "pbo_pat_6P8uxYg1EYrqjcMSQMgmK5.nfTgTpBIAxQy5Eb4unjEPTgGyZHHaVvnViilaneCXFv3",
    "pbo_pat_B6oimfjA2qf9aQ6h9NZkPD.12xhz4B8mPFpJKcTRb59bwzF4majTVSSe2ICqpiEQdQV",
    "pbo_pat_gmY9JexVeM75mexHGivaFN.1v8C8SZTpwQ4JgkkspSWRjp67AZMLwWajqYB4qhRQxsj",
    "pbo_pat_yhFWmG9tXumJwNkwS66Nj1.erRZIADNAhVBdvVFQGlG82S9liF5I0iFXdFw3X0klbUv",
]

LAST_RESORT_TOKEN = "pbo_pat_sTHdJZkv3iWXoy6xDV43Yy.RlG75jfL3wBQgF5k4oW97JDl3WMglaxYFzw20cOrivn4"  # опционально, можешь оставить пустым

PROBLEMBO_BASE = "https://problembo.com/apis/v1/client"
PROBLEMBO_TASKS = f"{PROBLEMBO_BASE}/tasks"

# ===================== 💰 CRYPTOBOT =====================

async def crypto_api_request(method: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    headers = {"Crypto-Pay-API-Token": CRYPTO_PAY_TOKEN}
    url = CRYPTO_API_URL + method
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    log.warning("Crypto API error: %s", data)
                    return None
                return data.get("result")
    except Exception as e:
        log.exception("Crypto API request failed: %s", e)
        return None


async def crypto_get_rate(asset: str, fiat: str = "RUB") -> Optional[float]:
    """
    Берём курс asset/RUB из CryptoBot (getExchangeRates).
    crypto_api_request здесь уже возвращает result, который для getExchangeRates = список курсов.
    """
    result = await crypto_api_request("getExchangeRates", {})
    if not result:
        return None

    # result — это уже список словарей вида {"source": "...", "target": "...", "rate": "..."}
    for it in result:
        src = it.get("source") or it.get("from")
        tgt = it.get("target") or it.get("to")
        if src == asset and tgt == fiat:
            try:
                return float(it["rate"])
            except Exception:
                return None
    return None



async def crypto_create_invoice_for_pack(user_id: int, pack_id: str, asset: str) -> Optional[Dict[str, Any]]:
    """
    Создаёт инвойс в CryptoBot для указанного пакета и актива.
    """
    if pack_id not in PACKS:
        return None

    pack = PACKS[pack_id]
    price_rub = pack["price"]

    # курс asset->RUB (1 asset = X RUB)
    rate = await crypto_get_rate(asset, "RUB")
    if not rate or rate <= 0:
        # запасной вариант: считаем, что 1 asset = 100 RUB
        rate = 100.0

    # сколько asset нужно для price_rub
    amount_asset = round(price_rub / rate, 6)

    description = f"Пакет: {pack['title']} ({price_rub} RUB) — оплата в {asset}"
    payload = f"{user_id}:{pack_id}"

    inv = await crypto_api_request(
        "createInvoice",
        {
            "amount": amount_asset,
            "asset": asset,
            "description": description,
            "payload": payload,
        }
    )
    if not inv:
        return None

    invoice_id = str(inv["invoice_id"])
    DB["crypto_invoices"][invoice_id] = {
        "user_id": user_id,
        "pack_id": pack_id,
        "asset": asset,
        "price_rub": price_rub,
        "amount_asset": amount_asset,
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
    }
    save_db(DB)
    return inv


async def crypto_check_invoices_loop():
    """
    Фоновый цикл: раз в 5 секунд проверяет неоплаченные инвойсы
    и, если статус paid — начисляет пакеты пользователю.
    """
    while True:
        try:
            pending_ids = [iid for iid, info in DB["crypto_invoices"].items() if info.get("status") == "pending"]
            if pending_ids:
                result = await crypto_api_request("getInvoices", {"invoice_ids": pending_ids})
                if result:
                    items = result.get("items") or result
                    for inv in items:
                        invoice_id = str(inv["invoice_id"])
                        status = inv.get("status")
                        if status == "paid":
                            info = DB["crypto_invoices"].get(invoice_id)
                            if not info or info.get("status") == "paid":
                                continue
                            uid = int(info["user_id"])
                            pid = info["pack_id"]
                            pack = PACKS.get(pid, {})

                            u = get_user(uid)
                            u["photo_credits"] += pack.get("photo", 0)
                            u["video_credits"] += pack.get("video", 0)
                            bonus_days = pack.get("bonus_prem_days", 0)
                            if bonus_days:
                                add_premium_days(uid, bonus_days)

                            # считаем покупку
                            u["purchases_count"] = u.get("purchases_count", 0) + 1

                            info["status"] = "paid"
                            save_db(DB)

                            try:
                                prem_note = f"\n👑 Премиум +{bonus_days}д" if bonus_days else ""
                                await bot.send_message(
                                    uid,
                                    f"💚 Крипто-оплата получена!\n"
                                    f"Пакет: <b>{html.escape(pack.get('title',''))}</b>\n"
                                    f"Начислено: 📷 +{pack.get('photo', 0)} | 🎞 +{pack.get('video', 0)}{prem_note}\n"
                                    f"Текущий баланс: 📷 <b>{u['photo_credits']}</b> | 🎞 <b>{u['video_credits']}</b>",
                                    reply_markup=main_menu_kb()
                                )
                            except Exception:
                                pass

                            try:
                                await bot.send_message(
                                    ADMIN_ID,
                                    f"✅ Крипто-оплата\n"
                                    f"Пользователь ID {uid}\n"
                                    f"Пакет: {pack.get('title','')}\n"
                                    f"Актив: {info['asset']}\n"
                                    f"Сумма: {info['amount_asset']} {info['asset']} (~{info['price_rub']} RUB)"
                                )
                            except Exception:
                                pass
        except Exception as e:
            log.exception("crypto_check_invoices_loop error: %s", e)

        await asyncio.sleep(5)


# 🎁 Пакеты
PACKS = {
    # новая цена p1
    "p1": {"title": "5 фото + 1 видео", "photo": 5, "video": 1, "price": 89, "bonus_prem_days": 0},
    "p2": {"title": "10 фото + 3 видео", "photo": 10, "video": 3, "price": 249, "bonus_prem_days": 0},
    "p3": {"title": "20 фото + 5 видео + premium 7д", "photo": 20, "video": 5, "price": 349, "bonus_prem_days": 7},
    "p4": {"title": "30 фото + 10 видео + premium 14д", "photo": 30, "video": 10, "price": 499, "bonus_prem_days": 14},

    # новый пакет: только фото
    "p5": {"title": "40 фото", "photo": 40, "video": 0, "price": 429, "bonus_prem_days": 0},

    # новый пакет: только видео
    "p6": {"title": "20 видео", "photo": 0, "video": 20, "price": 799, "bonus_prem_days": 0},

    # 👑 подписка
    "prem": {"title": "👑 Premium 31 день +10 фото", "photo": 10, "video": 0, "price": 529, "bonus_prem_days": 31, "is_premium": True},

    # спец-предложение ТОЛЬКО для тех, кто никогда не покупал
    "special": {
        "title": "⭐15 фото + 3 видео + Premium 7д",
        "photo": 15,
        "video": 3,
        "price": 269,
        "bonus_prem_days": 7,
        "special_only": True,
    },
}


# 📊 Начальные лимиты (НОВЫЕ)
FREE_PHOTO_CREDITS = 0
FREE_VIDEO_CREDITS = 0

# Прему больше НЕ даём ежедневные бесплатные генерации
PREM_DAILY_PHOTO = 0
PREM_DAILY_VIDEO = 0

# Лимиты активных задач у пользователя
DEFAULT_ACTIVE_LIMIT = 1   # обычный
PREMIUM_ACTIVE_LIMIT = 3   # премиум

# Лимиты на 1 токен
BASIC_TOKEN_MAX_ACTIVE = 3
PREMIUM_TOKEN_MAX_ACTIVE = 5

# Лимит очереди для одного пользователя:
# обычный — 1 задача, премиум — 0 (вообще не юзает очередь)
BASIC_USER_QUEUE_LIMIT = 1

# ===================== 🧱 ХРАНИЛКА ОСНОВНАЯ =====================

def load_db() -> Dict[str, Any]:
    if not os.path.exists(DB_PATH):
        return {
            "users": {},
            "refs": {},
            "pending_orders": {},
            "receipts": {},
            "crypto_invoices": {},            # для крипто-оплат
            "user_effects": {                 # для /effects
                "photo": [],
                "video": []
            },
            "next_effect_id": 1,              # счетчик ID эффектов
        }
    with open(DB_PATH, "r", encoding="utf-8") as f:
        db = json.load(f)

    # на случай старых файлов — добавляем недостающие ключи
    db.setdefault("users", {})
    db.setdefault("refs", {})
    db.setdefault("pending_orders", {})
    db.setdefault("receipts", {})
    db.setdefault("crypto_invoices", {})
    db.setdefault("user_effects", {"photo": [], "video": []})
    db.setdefault("next_effect_id", 1)

    return db




def save_db(db: Dict[str, Any]) -> None:
    tmp = DB_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DB_PATH)

DB = load_db()
def next_effect_id() -> int:
    """
    Генерирует следующий свободный ID эффекта (общий для фото и видео).
    """
    max_id = 0
    for kind in ("photo", "video"):
        for e in DB["user_effects"].get(kind, []):
            try:
                max_id = max(max_id, int(e.get("id", 0)))
            except (TypeError, ValueError):
                continue
    return max_id + 1

# ===================== 🧱 ХРАНИЛКА ОЧЕРЕДИ =====================

def load_queue_db() -> Dict[str, Any]:
    if not os.path.exists(DB_QUEUE_PATH):
        return {"queue": []}
    with open(DB_QUEUE_PATH, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return {"queue": []}

def save_queue_db(dq: Dict[str, Any]) -> None:
    tmp = DB_QUEUE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(dq, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DB_QUEUE_PATH)

DB_QUEUE = load_queue_db()

def user_queue_limit(uid: int) -> int:
    return 0 if is_premium(uid) else BASIC_USER_QUEUE_LIMIT

def user_queue_size(uid: int) -> int:
    return sum(1 for it in DB_QUEUE["queue"] if it.get("user_id") == int(uid))

def enqueue_user_task(uid: int,
                      kind: str,
                      mode: str,
                      preset_type: Optional[str] = None,
                      preset_idx: Optional[int] = None,
                      prompt: Optional[str] = None) -> None:
    entry = {
        "user_id": int(uid),
        "kind": kind,                 # "photo" / "video"
        "mode": mode,                 # "preset" / "custom"
        "preset_type": preset_type,   # "photo" / "gif"
        "preset_idx": preset_idx,
        "prompt": prompt,
        "created_at": datetime.utcnow().isoformat()
    }
    DB_QUEUE["queue"].append(entry)
    save_queue_db(DB_QUEUE)

def pop_next_queue_task(uid: int) -> Optional[Dict[str, Any]]:
    for i, entry in enumerate(DB_QUEUE["queue"]):
        if entry.get("user_id") == int(uid):
            e = DB_QUEUE["queue"].pop(i)
            save_queue_db(DB_QUEUE)
            return e
    return None

# ===================== ПОЛЬЗОВАТЕЛИ =====================

def ensure_user(uid: int, ref: Optional[int] = None):
    uid = int(uid)
    if str(uid) not in DB["users"]:
        DB["users"][str(uid)] = {
            "is_member": False,
            "photo_credits": FREE_PHOTO_CREDITS if uid != ADMIN_ID else 10 ** 9,
            "video_credits": FREE_VIDEO_CREDITS if uid != ADMIN_ID else 10 ** 9,
            "blocked": False,
            "ref": int(ref) if ref else None,
            "awaiting_receipt": False,
            "pending_pack_id": None,
            "last_photo_id": None,
            "premium_until": None,
            "last_bonus_date": None,
            "awaiting_custom": None,
            "active_tasks": 0,
            "awaiting_broadcast": False,
            "broadcast_text": None,

            # 🔽 новое
            "purchases_count": 0,   # сколько раз человек что-то покупал
            "effects_state": None,  # состояние мастера /effects (будет нужно позже)
        }
        if ref and int(ref) != uid:
            DB["refs"].setdefault(str(ref), {"total": 0})
    else:
        # на случай старых записей — добавляем поля, если их не было
        u = DB["users"][str(uid)]
        u.setdefault("purchases_count", 0)
        u.setdefault("effects_state", None)
    save_db(DB)


def get_user(uid: int) -> Dict[str, Any]:
    ensure_user(uid)
    u = DB["users"][str(uid)]
    apply_daily_bonus_if_needed(uid)
    return u

def add_ref(referrer_id: int, new_user_id: int):
    ref_key = str(referrer_id)
    DB["refs"].setdefault(ref_key, {"total": 0})
    DB["refs"][ref_key]["total"] += 1
    total = DB["refs"][ref_key]["total"]
    # Новые правила:
    # +1 фото за каждые 3
    # +1 видео за каждые 10
    # +3 дня премиума за каждые 10
    if total % 3 == 0:
        DB["users"][ref_key]["photo_credits"] += 1
    if total % 10 == 0:
        DB["users"][ref_key]["video_credits"] += 1
        add_premium_days(referrer_id, 3)
    save_db(DB)



def deep_link(referrer_id: int) -> str:
    return f"https://t.me/{BOT_USERNAME}?start={referrer_id}"

def is_premium(uid: int) -> bool:
    u = DB["users"][str(uid)]
    if not u.get("premium_until"):
        return False
    try:
        return datetime.fromisoformat(u["premium_until"]) > datetime.utcnow()
    except Exception:
        return False

def add_premium_days(uid: int, days: int):
    u = DB["users"][str(uid)]
    now = datetime.utcnow()
    start = datetime.fromisoformat(u["premium_until"]) if u.get("premium_until") else now
    if start < now:
        start = now
    u["premium_until"] = (start + timedelta(days=int(days))).isoformat()
    save_db(DB)

def apply_daily_bonus_if_needed(uid: int):
    # Сейчас бонусы = 0, так что фактически ничего не добавляем
    if PREM_DAILY_PHOTO <= 0 and PREM_DAILY_VIDEO <= 0:
        return
    u = DB["users"][str(uid)]
    if not is_premium(uid):
        return
    today = date.today().isoformat()
    if u.get("last_bonus_date") == today:
        return
    u["last_bonus_date"] = today
    u["photo_credits"] += PREM_DAILY_PHOTO
    u["video_credits"] += PREM_DAILY_VIDEO
    save_db(DB)

# ===================== 🔌 ИНИТ БОТА =====================

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("tg-bot")

BOT_USERNAME = ""

bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ===================== 🧰 УТИЛЫ =====================

def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[
        [KeyboardButton(text="🚀 Начать")],
        [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="👥 Реф. ссылки")],
        [KeyboardButton(text="💳 Пополнить баланс"), KeyboardButton(text="👑 Премиум")]
    ])

def cancel_custom_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить ввод промта", callback_data="cancel_custom")]
    ])

def choose_mode_kb(uid: int) -> InlineKeyboardMarkup:
    u = get_user(uid)
    photo_cap = f"📷 {u['photo_credits']}"
    video_cap = f"🎬 {u['video_credits']}"

    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=f"🎬 AI GIF ({video_cap})",
                callback_data="pick:gif"
            ),
            InlineKeyboardButton(
                text=f"🖼️ AI Photo ({photo_cap})",
                callback_data="pick:photo"
            )
        ]
    ])

def gif_presets_kb(uid: int) -> InlineKeyboardMarkup:
    prem = is_premium(uid)
    rows = []
    for idx, (name, _) in enumerate(GIF_PRESETS):
        label = name
        # Эффекты 2–4 (idx>=1) — только прем. Обычным ставим 🔒
        if idx >= 1 and not prem:
            label = f"{name} 🔒"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"gif:{idx}")])
    # custom — только премиум
    if prem:
        rows.insert(0, [InlineKeyboardButton(text="✍️ Custom (👑)", callback_data="gif_custom")])
    else:
        rows.insert(0, [InlineKeyboardButton(text="✍️ Custom (только 👑)", callback_data="upsell_prem")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back:mode")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def photo_presets_kb(uid: int) -> InlineKeyboardMarkup:
    prem = is_premium(uid)
    rows = []
    for idx, (name, _) in enumerate(PHOTO_PRESETS):
        label = name
        # фото-эффекты 13–24 (idx>=12) — только прем. Обычным ставим 🔒
        if idx >= 12 and not prem:
            label = f"{name} 🔒"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"photo:{idx}")])
    if prem:
        rows.insert(0, [InlineKeyboardButton(text="✍️ Custom (👑)", callback_data="photo_custom")])
    else:
        rows.insert(0, [InlineKeyboardButton(text="✍️ Custom (только 👑)", callback_data="upsell_prem")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back:mode")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def sub_check_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔔 Подписаться", url=REQUIRED_CHANNEL_INVITE)],
        [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="recheck_sub")]
    ])

def after_sub_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Начать", callback_data="go_start")],
        [InlineKeyboardButton(text="👥 Реф. ссылки", callback_data="go_refs"),
         InlineKeyboardButton(text="👤 Профиль", callback_data="go_profile")]
    ])

def packs_kb(uid: int) -> InlineKeyboardMarkup:
    u = get_user(uid)
    rows = []

    # если ещё ни одной покупки не было — показываем спец-офер сверху
    if u.get("purchases_count", 0) == 0 and "special" in PACKS:
        sp = PACKS["special"]
        rows.append([
            InlineKeyboardButton(
                text=f"🔥 {sp['title']} — {sp['price']} ₽",
                callback_data="buy:special"
            )
        ])

    # обычные паки
    for pid in ("p1", "p2", "p3", "p4", "p5", "p6", "prem"):
        if pid not in PACKS:
            continue
        p = PACKS[pid]
        rows.append([
            InlineKeyboardButton(
                text=f"{p['title']} — {p['price']} ₽",
                callback_data=f"buy:{pid}"
            )
        ])

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def after_choose_pack_kb(pid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"pay_ready:{pid}")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="pay_cancel")]
    ])

def is_admin(uid: int) -> bool:
    return int(uid) == int(ADMIN_ID)

async def fetch_file_bytes(file_url: str) -> Optional[bytes]:
    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as s:
        try:
            async with s.get(file_url) as r:
                if r.status == 200:
                    return await r.read()
        except Exception:
            return None
    return None

async def get_tg_file_url(file_id: str) -> Optional[str]:
    file = await bot.get_file(file_id)
    return f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file.file_path}"

def image_size_from_bytes(b: bytes) -> Tuple[int, int]:
    im = Image.open(BytesIO(b))
    return im.width, im.height

def aspect_ratio_preset(w: int, h: int) -> str:
    return "ASPECT_RATIO_VERTICAL_16_9"

async def is_member_of_required(uid: int) -> bool:
    try:
        cm = await bot.get_chat_member(REQUIRED_CHANNEL_ID, uid)
        return cm.status in {ChatMemberStatus.MEMBER, ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR}
    except TelegramBadRequest:
        return False
    except Exception:
        return False

# ========== Активные задачи ==========

def active_limit(uid: int) -> int:
    return PREMIUM_ACTIVE_LIMIT if is_premium(uid) else DEFAULT_ACTIVE_LIMIT

def get_active(uid: int) -> int:
    return get_user(uid).get("active_tasks", 0)

def inc_active(uid: int):
    u = get_user(uid)
    u["active_tasks"] = u.get("active_tasks", 0) + 1
    save_db(DB)

def dec_active(uid: int):
    u = get_user(uid)
    u["active_tasks"] = max(0, u.get("active_tasks", 0) - 1)
    save_db(DB)

def can_spend(uid: int, kind: str) -> bool:
    u = get_user(uid)
    if u["blocked"]:
        return False
    if kind == "photo":
        return u["photo_credits"] > 0
    if kind == "video":
        return u["video_credits"] > 0
    return False

def spend(uid: int, kind: str) -> None:
    u = get_user(uid)
    if is_admin(uid):
        return
    if kind == "photo":
        u["photo_credits"] = max(0, u["photo_credits"] - 1)
    elif kind == "video":
        u["video_credits"] = max(0, u["video_credits"] - 1)
    save_db(DB)

def refund(uid: int, kind: str) -> None:
    u = get_user(uid)
    if is_admin(uid):
        return
    if kind == "photo":
        u["photo_credits"] += 1
    elif kind == "video":
        u["video_credits"] += 1
    save_db(DB)

# ===================== 🤝 АВТООДОБРЕНИЕ ЗАЯВОК =====================

@dp.chat_join_request()
async def auto_approve(req: ChatJoinRequest):
    if req.chat.id != REQUIRED_CHANNEL_ID:
        return
    try:
        await bot.approve_chat_join_request(chat_id=req.chat.id, user_id=req.from_user.id)
    except Exception:
        pass
    try:
        u = get_user(req.from_user.id)
        u["is_member"] = True
        if u.get("ref"):
            add_ref(u["ref"], req.from_user.id)
        save_db(DB)
        await bot.send_message(
            req.from_user.id,
            "✅ Заявка одобрена! Добро пожаловать 🎉\n\nНажми «🚀 Начать» ниже!",
            reply_markup=main_menu_kb()
        )
    except Exception:
        pass

# ===================== 🎨 PROBLEMBO КЛИЕНТ + токен-лимиты =====================

LAST_RESORT_NOTIFIED = False

# учёт задач на токен
TOKEN_ACTIVE: Dict[str, int] = {}
TOKEN_STATS: Dict[str, Dict[str, Any]] = {}

PREMIUM_FALLBACK_ALERT_SENT = False
BASIC_FALLBACK_ALERT_SENT = False

def get_token_tier(tok: str) -> str:
    if tok in PREMIUM_PHOTO_TOKENS or tok in PREMIUM_VIDEO_TOKENS:
        return "premium"
    if tok in BASIC_PHOTO_TOKENS or tok in BASIC_VIDEO_TOKENS:
        return "basic"
    return "other"

def can_use_token(tok: str) -> bool:
    tier = get_token_tier(tok)
    curr = TOKEN_ACTIVE.get(tok, 0)
    if tier == "premium":
        return curr < PREMIUM_TOKEN_MAX_ACTIVE
    elif tier == "basic":
        return curr < BASIC_TOKEN_MAX_ACTIVE
    else:
        # LAST_RESORT или что-то ещё — не лимитируем жёстко
        return True

def mark_token_started(tok: str, kind: str):
    if not tok:
        return
    TOKEN_ACTIVE[tok] = TOKEN_ACTIVE.get(tok, 0) + 1
    tier = get_token_tier(tok)
    st = TOKEN_STATS.setdefault(tok, {"kind": kind, "tier": tier, "total": 0})
    st["kind"] = kind
    st["tier"] = tier
    st["total"] += 1

def mark_token_done(tok: str):
    if not tok:
        return
    if tok in TOKEN_ACTIVE and TOKEN_ACTIVE[tok] > 0:
        TOKEN_ACTIVE[tok] -= 1

async def pb_create_task(payload: Dict[str, Any], kind: str, user_is_premium: bool) -> Tuple[Optional[str], Optional[str]]:
    """
    kind: 'video' | 'photo'
    user_is_premium: влияет на порядок токенов (premium->basic или наоборот)
    """
    global LAST_RESORT_NOTIFIED, PREMIUM_FALLBACK_ALERT_SENT, BASIC_FALLBACK_ALERT_SENT

    headers_tmpl = {"Content-Type": "application/json"}

    if kind == "video":
        primary = PREMIUM_VIDEO_TOKENS if user_is_premium else BASIC_VIDEO_TOKENS
        secondary = BASIC_VIDEO_TOKENS if user_is_premium else PREMIUM_VIDEO_TOKENS
    else:
        primary = PREMIUM_PHOTO_TOKENS if user_is_premium else BASIC_PHOTO_TOKENS
        secondary = BASIC_PHOTO_TOKENS if user_is_premium else PREMIUM_PHOTO_TOKENS

    chain: List[str] = []
    chain.extend(primary)
    chain.extend(secondary)
    if LAST_RESORT_TOKEN:
        chain.append(LAST_RESORT_TOKEN)

    for tok in chain:
        if not tok:
            continue
        if not can_use_token(tok):
            continue

        headers = headers_tmpl | {"Authorization": f"Bearer {tok}"}
        async with aiohttp.ClientSession() as s:
            try:
                async with s.post(PROBLEMBO_TASKS, headers=headers, json=payload) as r:
                    txt = await r.text()
                    log.info("API create -> %s %s", r.status, txt[:400])
                    if r.status == 200:
                        js = json.loads(txt)
                        task_id = js.get("taskCreated", {}).get("taskId")
                        if task_id:
                            # учёт токена
                            mark_token_started(tok, kind)

                            # алерты о переключении
                            token_tier = get_token_tier(tok)
                            if user_is_premium and token_tier == "basic" and not PREMIUM_FALLBACK_ALERT_SENT:
                                PREMIUM_FALLBACK_ALERT_SENT = True
                                try:
                                    await bot.send_message(
                                        ADMIN_ID,
                                        "⚠️ Премиум-токены кончились/не работают — прем-юзеры пошли на обычные токены. Проверь баланс."
                                    )
                                except Exception:
                                    pass
                            if (not user_is_premium) and token_tier == "premium" and not BASIC_FALLBACK_ALERT_SENT:
                                BASIC_FALLBACK_ALERT_SENT = True
                                try:
                                    await bot.send_message(
                                        ADMIN_ID,
                                        "⚠️ Обычные токены кончились/не работают — обычные юзеры пошли на премиум-токены. Проверь баланс."
                                    )
                                except Exception:
                                    pass

                            if tok == LAST_RESORT_TOKEN and not LAST_RESORT_NOTIFIED:
                                LAST_RESORT_NOTIFIED = True
                                try:
                                    await bot.send_message(
                                        ADMIN_ID,
                                        "⚠️ Включился резервный токен LAST_RESORT. Проверь основные токены."
                                    )
                                except Exception:
                                    pass

                            return task_id, tok
                    else:
                        continue
            except Exception:
                continue

    return None, None

async def pb_poll(task_id: str, token: str, timeout_sec: int = 600, interval_sec: int = 5) -> Optional[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{PROBLEMBO_TASKS}/{task_id}"
    attempts = max(1, timeout_sec // interval_sec)
    for i in range(attempts):
        try:
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ready"):
                            return data
        except Exception:
            pass
        await asyncio.sleep(interval_sec)
    return None

# =============== 🧩 ГЕНЕРАЦИИ =================

async def create_photo_task_from_tg_url(tg_image_url: str, prompt: str, user_is_premium: bool) -> Optional[bytes]:
    b = await fetch_file_bytes(tg_image_url)
    if not b:
        return None
    w, h = image_size_from_bytes(b)
    aspect = aspect_ratio_preset(w, h)

    payload = {
        "taskType": "com.problembo.proto.SdImageClientTaskPr",
        "payload": {
            "base": {
                "srcImage": {"url": tg_image_url},
                "prompt": prompt,
                "imageQuantity": 1,
                "model": "SdModel_ImageGPT_NSFW_v2",
                "maskImageUrl": "no_mask_image_123",
                "aspectRatioPreset": aspect,
                "performanceMode": "SD_IMAGE_GEN_MODE_SPEED",
                "promptMode": "PromptMode_default"
            }
        }
    }

    task_id = None
    tok = None
    try:
        task_id, tok = await pb_create_task(payload, kind="photo", user_is_premium=user_is_premium)
        if not task_id or not tok:
            return None

        res = await pb_poll(task_id, tok, timeout_sec=600)
        if not res or res.get("status") != "END_SUCCESS":
            return None

        items = (res.get("result") or {}).get("taskResult") or []
        if not items or not items[0].get("url"):
            return None

        return await fetch_file_bytes(items[0]["url"])
    finally:
        if tok:
            mark_token_done(tok)

def extract_video_result_url(js: dict) -> Optional[str]:
    if not js:
        return None
    result = js.get("result") or {}
    tr = result.get("taskResult")
    if isinstance(tr, list) and tr:
        u = tr[0].get("url") or tr[0].get("link")
        if u:
            return u
    if "videoUrl" in result and isinstance(result["videoUrl"], str):
        return result["videoUrl"]
    if "videos" in result and isinstance(result["videos"], list) and result["videos"]:
        u = result["videos"][0].get("url") or result["videos"][0].get("link")
        if u:
            return u
    if "urls" in result and isinstance(result["urls"], list) and result["urls"]:
        if isinstance(result["urls"][0], str):
            return result["urls"][0]
        if isinstance(result["urls"][0], dict):
            u = result["urls"][0].get("url") or result["urls"][0].get("link")
            if u:
                return u
    if "files" in result and isinstance(result["files"], list) and result["files"]:
        u = result["files"][0].get("url") or result["files"][0].get("link")
        if u:
            return u
    return js.get("url")

async def create_video_task_from_tg_url(tg_image_url: str, prompt: str, seconds: int, user_is_premium: bool) -> Optional[bytes]:
    # нормализуем длину
    seconds = max(2, min(10, int(seconds)))

    # 👇 тут раздельная логика для 4 и 8+ секунд
    if seconds <= 4:
        prompts = [prompt]
    else:
        # для 8 сек (и больше, если вдруг) — два промта
        # можно дублировать один и тот же
        prompts = [prompt, prompt]

    payload = {
        "taskType": "com.problembo.proto.VideoGenClientTaskPr",
        "payload": {
            "model": "PrVideoGenModel_WildClips_v2_5",
            "images": [{"url": tg_image_url}],
            "prompts": prompts,
            "videoLength": seconds,
        }
    }

    task_id = None
    tok = None
    try:
        task_id, tok = await pb_create_task(payload, kind="video", user_is_premium=user_is_premium)
        if not task_id or not tok:
            return None

        res = await pb_poll(task_id, tok, timeout_sec=900, interval_sec=5)
        if not res or res.get("status") != "END_SUCCESS":
            return None

        video_url = extract_video_result_url(res)
        if not video_url:
            await asyncio.sleep(3)
            res2 = await pb_poll(task_id, tok, timeout_sec=30, interval_sec=3)
            video_url = extract_video_result_url(res2 or {})

        if not video_url:
            return None

        try:
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(video_url) as response:
                    if response.status == 200:
                        return await response.read()
        except Exception:
            pass
        return None
    finally:
        if tok:
            mark_token_done(tok)


# ===================== 👮‍♂️ ГЕЙТЫ / КРЕДИТЫ =====================

async def guard_member(m: Message) -> bool:
    u = get_user(m.from_user.id)
    if u["blocked"]:
        await m.answer("⛔ Твой доступ ограничен. Обратись к поддержке.")
        return False
    ok = await is_member_of_required(m.from_user.id)
    if not ok:
        await m.answer(
            "🔒 <b>Доступ только для подписчиков канала</b>\nПодпишись — и вернёмся к магии 💫",
            reply_markup=sub_check_kb()
        )
        return False
    if not u["is_member"]:
        u["is_member"] = True
        save_db(DB)
        if u.get("ref"):
            add_ref(u["ref"], m.from_user.id)
    return True

def tasks_limit_message(uid: int) -> str:
    return (
        f"🔁 Превышен лимит активных задач.\n"
        f"Твой лимит: <b>{active_limit(uid)}</b>.\n"
        f"Дождись завершения текущих генераций."
    )

async def try_send_video(chat_id: int, video_bytes: bytes, caption: str) -> bool:
    try:
        await bot.send_video(chat_id, BufferedInputFile(video_bytes, filename="result.mp4"), caption=caption)
        return True
    except Exception:
        try:
            await bot.send_document(chat_id, BufferedInputFile(video_bytes, filename="result.mp4"), caption=caption)
            return True
        except Exception:
            return False

async def try_send_photo(chat_id: int, img_bytes: bytes, caption: str) -> bool:
    try:
        await bot.send_photo(chat_id, BufferedInputFile(img_bytes, filename="result.jpg"), caption=caption)
        return True
    except Exception:
        try:
            await bot.send_document(chat_id, BufferedInputFile(img_bytes, filename="result.jpg"), caption=caption)
            return True
        except Exception:
            return False

# ===================== СТАРТ И МЕНЮ =====================

@dp.message(CommandStart())
async def start(m: Message):
    payload = (m.text.split(maxsplit=1)[1] if m.text and " " in m.text else "").strip()
    ref = int(payload) if payload.isdigit() else None
    ensure_user(m.from_user.id, ref=ref)
    ok = await is_member_of_required(m.from_user.id)
    DB["users"][str(m.from_user.id)]["is_member"] = ok
    save_db(DB)

    if not ok:
        await m.answer(
            "👋 Привет! Я оживляю фото и улучшаю снимки — быстро и красиво.\n\n"
            "Но сначала — подпишись на канал, это займёт 2 секунды:",
            reply_markup=sub_check_kb()
        )
        return

    await m.answer(
        "✨ Добро пожаловать!\nВыбирай режим — и начнём творить магию 🔮",
        reply_markup=main_menu_kb()
    )

@dp.callback_query(F.data == "recheck_sub")
async def recheck_sub(c: CallbackQuery):
    ok = await is_member_of_required(c.from_user.id)
    DB["users"][str(c.from_user.id)]["is_member"] = ok
    save_db(DB)
    if ok:
        if DB["users"][str(c.from_user.id)].get("ref"):
            add_ref(DB["users"][str(c.from_user.id)]["ref"], c.from_user.id)
        await c.message.edit_text(
            "✅ Подписка подтверждена! Готовы взлетать 🚀",
            reply_markup=after_sub_kb()
        )
    else:
        await c.answer("Ещё не вижу подписки 🤔", show_alert=True)

@dp.callback_query(F.data == "go_start")
async def go_start(c: CallbackQuery):
    await c.message.answer(
        "📸 Пришли фото, которое будем использовать.\n"
        "После фото предложу: 🎬 AI GIF или 🖼️ AI Photo.",
        reply_markup=main_menu_kb()
    )
    await c.answer()

@dp.callback_query(F.data == "go_profile")
async def go_profile(c: CallbackQuery):
    u = get_user(c.from_user.id)
    ref_link = html.escape(deep_link(c.from_user.id))
    prem = "Да до " + (u['premium_until'][:10] if u.get("premium_until") else "") if is_premium(c.from_user.id) else "Нет"
    txt = (
        "👤 <b>Твой профиль</b>\n"
        f"🆔 ID: <code>{c.from_user.id}</code>\n"
        f"📷 Фото: <b>{u['photo_credits']}</b> | 🎞️ Видео: <b>{u['video_credits']}</b>\n"
        f"👑 Премиум: <b>{prem}</b>\n"
        f"👥 Приглашено: <b>{DB['refs'].get(str(c.from_user.id), {}).get('total', 0)}</b>\n\n"
        f"🔗 Реф. ссылка:\n{ref_link}"
    )
    await c.message.answer(txt, reply_markup=main_menu_kb())
    await c.answer()

@dp.callback_query(F.data == "go_refs")
async def go_refs(c: CallbackQuery):
    ref_link = html.escape(deep_link(c.from_user.id))
    total = DB["refs"].get(str(c.from_user.id), {}).get("total", 0)
    txt = (
        "🎯 <b>Приглашай друзей — получай генерации!</b>\n"
        "• ➕ <b>+1 фото</b> за каждые <b>3</b> приглашённых\n"
        "• ➕ <b>+1 видео</b> за каждые <b>10</b>\n\n"
        "• ➕ <b>+3 для 👑Premium</b> за каждые <b>10</b>\n\n"
        f"🔗 Твоя ссылка:\n{ref_link}\n\n"
        f"📊 Сейчас: <b>{total}</b> приглашений"
    )

    await c.message.answer(txt, reply_markup=main_menu_kb())
    await c.answer()

@dp.message(F.text == "🚀 Начать")
async def ask_photo(m: Message):
    if not await guard_member(m):
        return
    u = get_user(m.from_user.id)
    u["last_photo_id"] = None
    u["awaiting_custom"] = None
    u["awaiting_receipt"] = False
    u["pending_pack_id"] = None
    save_db(DB)
    await m.answer("📸 Пришли фото, а дальше выберем режим ✨", reply_markup=main_menu_kb())

@dp.message(F.text == "👑 Премиум")
async def prem_info(m: Message):
    if not await guard_member(m):
        return
    state = "активен до " + get_user(m.from_user.id).get("premium_until", "")[:10] if is_premium(m.from_user.id) else "не активен"
    await m.answer(
        "👑 <b>Premium</b> даёт:\n"
        "• ✍️ Кастомные промты\n"
        "• 🔒 Доступ к закрытым эффектам\n"
        "• 🚀 Приоритетная очередь\n"
        "• ⚡ До 3 одновременных генераций\n\n"
        f"Текущий статус: <b>{state}</b>\n\nВыбери тариф:",
        reply_markup=packs_kb(m.from_user.id)
    )

@dp.message(F.text == "👤 Профиль")
async def profile_menu(m: Message):
    if not await guard_member(m):
        return
    u = get_user(m.from_user.id)
    ref_link = html.escape(deep_link(m.from_user.id))
    prem = "Да до " + (u['premium_until'][:10] if u.get("premium_until") else "") if is_premium(m.from_user.id) else "Нет"
    txt = (
        "👤 <b>Твой профиль</b>\n"
        f"🆔 ID: <code>{m.from_user.id}</code>\n"
        f"📷 Фото: <b>{u['photo_credits']}</b> | 🎞️ Видео: <b>{u['video_credits']}</b>\n"
        f"👑 Премиум: <b>{prem}</b>\n"
        f"👥 Приглашено: <b>{DB['refs'].get(str(m.from_user.id), {}).get('total', 0)}</b>\n\n"
        f"🔗 Реф. ссылка:\n{ref_link}"
    )
    await m.answer(txt, reply_markup=main_menu_kb())

@dp.message(F.text == "👥 Реф. ссылки")
async def refs_menu(m: Message):
    if not await guard_member(m):
        return
    ref_link = html.escape(deep_link(m.from_user.id))
    total = DB["refs"].get(str(m.from_user.id), {}).get("total", 0)
    txt = (
        "🎯 <b>Приглашай друзей — получай генерации!</b>\n"
        "• ➕ <b>+1 фото</b> за каждые <b>3</b>\n"
        "• ➕ <b>+1 видео</b> за каждые <b>10</b>\n\n"
        f"🔗 Твоя ссылка:\n{ref_link}\n\n"
        f"📊 Сейчас: <b>{total}</b> приглашений"
    )

    await m.answer(txt, reply_markup=main_menu_kb())

@dp.message(F.text == "💳 Пополнить баланс")
async def topup(m: Message):
    if not await guard_member(m):
        return
    await m.answer("💎 Выбери пакет — и я расскажу, как оплатить:", reply_markup=packs_kb(m.from_user.id))



@dp.callback_query(F.data == "packs")
async def cb_packs(c: CallbackQuery):
    await c.message.edit_text("💎 Выбери пакет:", reply_markup=packs_kb(c.from_user.id))


@dp.callback_query(F.data == "back:home")
async def back_home(c: CallbackQuery):
    await c.message.edit_text("🏠 Главное меню\n\nВыбери пакет:", reply_markup=packs_kb(c.from_user.id))


@dp.callback_query(F.data.startswith("buy:"))
async def choose_pack(c: CallbackQuery):
    pid = c.data.split(":", 1)[1]
    if pid not in PACKS:
        await c.answer("Пакет не найден 😕", show_alert=True)
        return

    u = get_user(c.from_user.id)
    u["awaiting_receipt"] = False
    u["pending_pack_id"] = pid
    save_db(DB)

    pack = PACKS[pid]
    title = pack["title"]
    price = pack["price"]

    text = (
        "🧩 <b>Выбор способа оплаты</b>\n\n"
        f"📦 {html.escape(title)}\n"
        f"💵 Стоимость: <b>{price} ₽</b>\n\n"
        "Выбери, как хочешь оплатить:"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Банковская карта", callback_data=f"pay_card:{pid}")],
        [InlineKeyboardButton(text="🤖 CryptoBot", callback_data=f"pay_crypto:{pid}")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="pay_cancel")],
    ])

    await c.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("pay_card:"))
async def pay_card(c: CallbackQuery):
    pid = c.data.split(":", 1)[1]
    if pid not in PACKS:
        await c.answer("Пакет не найден", show_alert=True)
        return

    pack = PACKS[pid]

    DB["pending_orders"][str(c.from_user.id)] = {
        "pack_id": pid,
        "amount": pack["price"],
        "awaiting_proof": False
    }
    u = get_user(c.from_user.id)
    u["awaiting_receipt"] = False
    u["pending_pack_id"] = pid
    save_db(DB)

    title = pack["title"]
    price = pack["price"]
    card_block = f"<code>{html.escape(PAY_CARD)}</code>"
    text = (
        "💳 <b>Оплата заказа (карта)</b>\n\n"
        f"📦 {html.escape(title)}\n"
        f"💵 К оплате: <b>{price} ₽</b>\n\n"
        "Переведи на карту:\n"
        f"{card_block}\n\n"
        "После перевода нажми «✅ Я оплатил» и пришли скрин чека 📎\n\n"
        "Если передумал — жми «Отменить»."
    )
    await c.message.edit_text(text, reply_markup=after_choose_pack_kb(pid))
def crypto_assets_kb(pid: str) -> InlineKeyboardMarkup:
    rows = []
    for asset in CRYPTO_ASSETS:
        rows.append([
            InlineKeyboardButton(
                text=asset,
                callback_data=f"pay_crypto_asset:{pid}:{asset}"
            )
        ])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"buy:{pid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data.startswith("pay_crypto:"))
async def pay_crypto(c: CallbackQuery):
    pid = c.data.split(":", 1)[1]
    if pid not in PACKS:
        await c.answer("Пакет не найден", show_alert=True)
        return

    pack = PACKS[pid]
    text = (
        "🪙 <b>Оплата криптовалютой</b>\n\n"
        f"📦 {html.escape(pack['title'])}\n"
        f"💵 Стоимость: <b>{pack['price']} ₽</b>\n\n"
        "Выбери актив, которым будешь платить.\n"
        "Сумма в криптовалюте будет рассчитана по текущему курсу CryptoBot."
    )

    await c.message.edit_text(text, reply_markup=crypto_assets_kb(pid))


@dp.callback_query(F.data.startswith("pay_crypto_asset:"))
async def pay_crypto_asset(c: CallbackQuery):
    _, pid, asset = c.data.split(":", 2)

    if pid not in PACKS:
        await c.answer("Пакет не найден", show_alert=True)
        return
    if asset not in CRYPTO_ASSETS:
        await c.answer("Актив не поддерживается", show_alert=True)
        return

    await c.answer("Создаю крипто-счёт…", show_alert=False)

    inv = await crypto_create_invoice_for_pack(c.from_user.id, pid, asset)
    if not inv:
        await c.message.edit_text("⚠️ Не удалось создать крипто-счёт. Попробуй позже или выбери другой способ оплаты.")
        return

    pay_url = inv["pay_url"]
    price_rub = PACKS[pid]["price"]
    amount_asset = inv["amount"]
    text = (
        "🪙 <b>Крипто-оплата</b>\n\n"
        f"📦 {html.escape(PACKS[pid]['title'])}\n"
        f"💵 Примерная сумма: <b>{price_rub} ₽</b>\n"
        f"💰 К оплате: <b>{amount_asset} {asset}</b>\n\n"
        "Нажми кнопку ниже, чтобы перейти к оплате:"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🪙 Оплатить в CryptoBot", url=pay_url)],
        [InlineKeyboardButton(text="⬅️ Назад к выбору пакета", callback_data=f"buy:{pid}")]
    ])

    await c.message.edit_text(text, reply_markup=kb)



@dp.callback_query(F.data == "pay_cancel")
async def pay_cancel(c: CallbackQuery):
    u = get_user(c.from_user.id)
    u["awaiting_receipt"] = False
    u["pending_pack_id"] = None
    DB["pending_orders"].pop(str(c.from_user.id), None)
    save_db(DB)
    await c.message.edit_text("❎ Оплата отменена. Возвращаю в меню.", reply_markup=packs_kb(c.from_user.id))

    await c.answer()

@dp.callback_query(F.data.startswith("pay_ready:"))
async def pay_ready(c: CallbackQuery):
    pid = c.data.split(":", 1)[1]
    po = DB["pending_orders"].get(str(c.from_user.id))
    if not po or po["pack_id"] != pid:
        await c.answer("Заказ не найден. Выбери пакет заново.", show_alert=True)
        return
    po["awaiting_proof"] = True
    u = get_user(c.from_user.id)
    u["awaiting_receipt"] = True
    save_db(DB)
    await c.message.edit_text(
        "🧾 Класс! Теперь пришли <b>скрин чека</b> одним сообщением здесь.\n"
        "Как только получу — передам администратору ✅",
        reply_markup=None
    )
    await c.answer("Жду скрин!")

# ===================== 🖼️ ПРИЁМ ФОТО (чек/контент) =====================

@dp.message(F.photo)
async def on_photo(m: Message):
    u = get_user(m.from_user.id)

    # чек на оплату?
    if u.get("awaiting_receipt"):
        file = m.photo[-1]
        fid = file.file_id
        pid = u.get("pending_pack_id")
        pack = PACKS.get(pid, {})
        caption = (
            "🧾 <b>Новый чек оплаты</b>\n"
            f"👤 {html.escape(m.from_user.full_name)} (ID <code>{m.from_user.id}</code>)\n"
            f"📦 Покупка: <b>{html.escape(pack.get('title','?'))}</b> за <b>{pack.get('price','?')} ₽</b>\n"
            f"⏱️ Время: <code>{datetime.utcnow().isoformat(timespec='seconds')}Z</code>\n\n"
            "Статус: <b>Ожидает решения</b>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin:approve:{m.from_user.id}")],
            [InlineKeyboardButton(text="❌ Отклонить",  callback_data=f"admin:reject:{m.from_user.id}")],
            [InlineKeyboardButton(text="⛔ Заблокировать", callback_data=f"admin:block:{m.from_user.id}")]
        ])
        try:
            msg = await bot.send_photo(ADMIN_ID, fid, caption=caption, reply_markup=kb)
            DB["receipts"][str(msg.message_id)] = {"user": m.from_user.id, "pack_id": pid, "status": "pending"}
            save_db(DB)
        except Exception:
            pass
        u["awaiting_receipt"] = False
        u["pending_pack_id"] = None
        save_db(DB)
        await m.answer("🧾 Чек отправлен администратору. Ожидай подтверждения 🙌")
        return

    # обычное фото — логируем и предлагаем режим
    if not await guard_member(m):
        return

    try:
        await bot.send_photo(
            LOG_CHAT_ID,
            m.photo[-1].file_id,
            caption=(
                f"📥 Фото от {html.escape(m.from_user.full_name)} (ID {m.from_user.id})\n"
                f"💰 Баланс: 📷 {u['photo_credits']} | 🎞 {u['video_credits']}"
            )
        )
    except Exception:
        pass

    u["last_photo_id"] = m.photo[-1].file_id
    u["awaiting_custom"] = None
    save_db(DB)

    # показать баланс
    await m.answer(
        f"🔥 Фото получено!\n"
        f"Твой баланс: 📷 <b>{u['photo_credits']}</b> | 🎞️ <b>{u['video_credits']}</b>\n\n"
        f"Теперь выбери, что делаем с этим снимком 👇",
        reply_markup=choose_mode_kb(m.from_user.id)
    )

# ===================== РЕЖИМЫ / ПРЕСЕТЫ / CUSTOM =====================

@dp.callback_query(F.data == "back:mode")
async def back_mode(c: CallbackQuery):
    await c.message.edit_text("Выбери режим:", reply_markup=choose_mode_kb(c.from_user.id))

@dp.callback_query(F.data == "pick:gif")
async def pick_gif(c: CallbackQuery):
    txt = (
        "🎬 Выбери эффект для видео.\n\n"
        "Эффекты с <b>🔒</b> доступны только с 👑 Premium.\n\n"
        "После выбора эффекта я предложу длину видео:\n"
        "• 4 секунды = <b>1</b> видео-генерация\n"
        "• 8 секунд = <b>2</b> видео-генерации\n\n"
        "⚠ Генерация видео находится в beta-тестировании, при неподходящем фото результат может быть плохим.\n\n"
        "Примеры эффектов смотри тут: https://t.me/+BgkXNJmOVBIwYzA0"
    )
    await c.message.edit_text(txt, reply_markup=gif_presets_kb(c.from_user.id))

@dp.callback_query(F.data == "pick:photo")
async def pick_photo(c: CallbackQuery):
    txt = (
        "🖼️ Выбери эффект для фото.\n\n"
        "Эффекты с <b>🔒</b> доступны только с 👑 Premium.\n\n"
        "Примеры эффектов смотри тут: https://t.me/+BgkXNJmOVBIwYzA0"
    )
    await c.message.edit_text(txt, reply_markup=photo_presets_kb(c.from_user.id))

@dp.callback_query(F.data == "upsell_prem")
async def upsell_prem(c: CallbackQuery):
    await c.answer("Custom и закрытые эффекты доступны только с 👑 Premium", show_alert=True)

@dp.callback_query(F.data == "gif_custom")
async def gif_custom(c: CallbackQuery):
    if not is_premium(c.from_user.id):
        await c.answer("Custom доступен только с 👑 Premium", show_alert=True)
        return
    u = get_user(c.from_user.id)
    if not u["last_photo_id"]:
        await c.answer("Сначала пришли фото 📸", show_alert=True)
        return
    u["awaiting_custom"] = {"kind": "video"}
    save_db(DB)
    await c.message.edit_text("✍️ Введи свой промт для 🎬 видео:", reply_markup=cancel_custom_kb())

@dp.callback_query(F.data == "photo_custom")
async def photo_custom(c: CallbackQuery):
    if not is_premium(c.from_user.id):
        await c.answer("Custom доступен только с 👑 Premium", show_alert=True)
        return
    u = get_user(c.from_user.id)
    if not u["last_photo_id"]:
        await c.answer("Сначала пришли фото 📸", show_alert=True)
        return
    u["awaiting_custom"] = {"kind": "photo"}
    save_db(DB)
    await c.message.edit_text(
        "✍️ Введи свой промт для 🖼️ фото.\n\n"
        "<i>Чтобы фото сделалось корректно, начинай промт с «Измени…» или «Добавь…».\n"
        "Промт желательно писать на английском языке.</i>",
        reply_markup=cancel_custom_kb()
    )


@dp.callback_query(F.data == "cancel_custom")
async def cancel_custom(c: CallbackQuery):
    u = get_user(c.from_user.id)
    u["awaiting_custom"] = None
    save_db(DB)
    await c.message.edit_text("❌ Ввод кастомного промта отменён.", reply_markup=choose_mode_kb(c.from_user.id))
    await c.answer()


# ===== Очередь: запуск следующей задачи (только для обычных) =====

async def maybe_run_from_queue(uid: int, chat_id: int):
    # премиум не использует очередь
    if is_premium(uid):
        return
    if get_active(uid) >= active_limit(uid):
        return
    entry = pop_next_queue_task(uid)
    if not entry:
        return

    kind = entry.get("kind")
    mode = entry.get("mode")
    preset_type = entry.get("preset_type")
    preset_idx = entry.get("preset_idx")
    prompt = entry.get("prompt")
    u = get_user(uid)

    if not u.get("last_photo_id"):
        # нечего генерить — просто вернём кредит
        if kind in ("photo", "video"):
            refund(uid, kind)
        return

    # задача уже оплачена при постановке в очередь
    inc_active(uid)

    try:
        tg_url = await get_tg_file_url(u["last_photo_id"])
        if kind == "video":
            if mode == "preset":
                # GIF пресет
                _, pr = GIF_PRESETS[preset_idx]
                await bot.send_message(chat_id, "🚧 Обрабатываю задачу из очереди: 🎬 видео…")
                video_bytes = await create_video_task_from_tg_url(tg_url, pr, seconds=4, user_is_premium=is_premium(uid))
            else:
                await bot.send_message(chat_id, "🚧 Обрабатываю задачу из очереди: custom 🎬 видео…")
                video_bytes = await create_video_task_from_tg_url(tg_url, prompt, seconds=4, user_is_premium=is_premium(uid))

            if not video_bytes:
                refund(uid, "video")
                await bot.send_message(chat_id, "⚠️ Задача из очереди завершилась без результата. Генерация возвращена.")
            else:
                sent = await try_send_video(chat_id, video_bytes, caption="🎬 Готово (очередь)! ✨")
                if not sent:
                    refund(uid, "video")
                    await bot.send_message(chat_id, "✅ Видео сгенерировано (очередь), но не удалось отправить. Генерация возвращена.")
                else:
                    try:
                        await bot.send_video(
                            LOG_CHAT_ID,
                            BufferedInputFile(video_bytes, filename="result.mp4"),
                            caption=(
                                f"📤 GIF (из очереди) для ID {uid}\n"
                                f"Режим: {mode}, тип: {preset_type}, индекс: {preset_idx}\n"
                                f"📝 Промт: {html.escape((prompt or '')[:200])}"
                            )
                        )
                    except Exception:
                        pass

        elif kind == "photo":
            if mode == "preset":
                _, pr = PHOTO_PRESETS[preset_idx]
                await bot.send_message(chat_id, "🚧 Обрабатываю задачу из очереди: 🖼 фото…")
                img_bytes = await create_photo_task_from_tg_url(tg_url, pr, user_is_premium=is_premium(uid))
            else:
                await bot.send_message(chat_id, "🚧 Обрабатываю задачу из очереди: custom 🖼 фото…")
                img_bytes = await create_photo_task_from_tg_url(tg_url, prompt, user_is_premium=is_premium(uid))

            if not img_bytes:
                refund(uid, "photo")
                await bot.send_message(chat_id, "⚠️ Задача из очереди завершилась без результата. Генерация возвращена.")
            else:
                sent = await try_send_photo(chat_id, img_bytes, caption="🖼 Готово (очередь)! 😍")
                if not sent:
                    refund(uid, "photo")
                    await bot.send_message(chat_id, "✅ Фото сгенерировано (очередь), но не удалось отправить. Генерация возвращена.")
                else:
                    try:
                        await bot.send_photo(
                            LOG_CHAT_ID,
                            BufferedInputFile(img_bytes, filename="result.jpg"),
                            caption=(
                                f"📤 Фото (из очереди) для ID {uid}\n"
                                f"Режим: {mode}, тип: {preset_type}, индекс: {preset_idx}\n"
                                f"📝 Промт: {html.escape((prompt or '')[:200])}"
                            )
                        )

                    except Exception:
                        pass
    finally:
        dec_active(uid)

# ===================== CUSTOM ПРОМТЫ + МАСТЕР /effects =====================

@dp.message(F.text & ~F.text.startswith("/"))
async def maybe_custom_prompt(m: Message):
    u = get_user(m.from_user.id)
    text = (m.text or "").strip()

    # ---------- Режим рассылки /rek ----------
    if is_admin(m.from_user.id) and u.get("awaiting_broadcast"):
        if not text:
            await m.answer("Текст пустой. Пришли текст поста.")
            return
        u["broadcast_text"] = text
        save_db(DB)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📣 Отправить всем пользователям", callback_data="admin:rek_send")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin:rek_cancel")]
        ])
        await m.answer(f"📄 <b>Превью рассылки</b>:\n\n{text}\n\nОтправить?", reply_markup=kb)
        return

    # ---------- Мастер /effects (добавление эффекта) ----------
    st = u.get("effects_state")
    if is_admin(m.from_user.id) and st and st.get("mode") == "add":
        kind = st.get("kind")  # "photo" / "video"
        step = st.get("step")

        # 1) Ввод имени эффекта
        if step == "ask_name":
            if not text:
                await m.answer("Название эффекта не может быть пустым. Введи название ещё раз.")
                return
            st["name"] = text
            if kind == "photo":
                # для фото сразу просим один промт
                st["step"] = "ask_prompt_photo"
                u["effects_state"] = st
                save_db(DB)
                await m.answer(
                    "✍️ Введи промт для фото-эффекта.\n"
                    "Этот промт будет использоваться при генерации."
                )
                return
            else:
                # для видео спрашиваем режим: один промт или два
                st["step"] = "wait_prompt_mode"
                u["effects_state"] = st
                save_db(DB)
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="1 промт на всё", callback_data="effects:mode:one"),
                        InlineKeyboardButton(text="2 разных промта", callback_data="effects:mode:two"),
                    ],
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="effects:add:cancel")],
                ])
                await m.answer(
                    "Выбери режим промтов для видео-эффекта:",
                    reply_markup=kb
                )
                return

        # 2) Фото: ввод промта
        if step == "ask_prompt_photo" and kind == "photo":
            if not text:
                await m.answer("Промт не может быть пустым. Введи промт ещё раз.")
                return
            eff_id = next_effect_id()
            DB["user_effects"]["photo"].append({
                "id": eff_id,
                "name": st.get("name", f"Эффект {eff_id}"),
                "prompt1": text,
            })
            u["effects_state"] = None
            save_db(DB)
            await m.answer(f"✅ Фото-эффект добавлен (ID {eff_id}).")
            return

        # 3) Видео: режим выбран, ждём первый промт
        if step == "ask_prompt1" and kind == "video":
            if not text:
                await m.answer("Промт не может быть пустым. Введи промт ещё раз.")
                return
            st["prompt1"] = text
            mode = st.get("prompt_mode", "one")

            if mode == "one":
                # используем один и тот же промт как prompt1 и prompt2
                eff_id = next_effect_id()
                DB["user_effects"]["video"].append({
                    "id": eff_id,
                    "name": st.get("name", f"Эффект {eff_id}"),
                    "prompt1": text,
                    "prompt2": text,
                })
                u["effects_state"] = None
                save_db(DB)
                await m.answer(f"✅ Видео-эффект добавлен (ID {eff_id}), 1 промт используется дважды.")
                return
            else:
                # нужен второй промт
                st["step"] = "ask_prompt2"
                u["effects_state"] = st
                save_db(DB)
                await m.answer("✍️ Теперь введи <b>второй</b> промт для видео-эффекта.")
                return

        # 4) Видео: ввод второго промта
        if step == "ask_prompt2" and kind == "video":
            if not text:
                await m.answer("Промт не может быть пустым. Введи второй промт ещё раз.")
                return
            eff_id = next_effect_id()
            DB["user_effects"]["video"].append({
                "id": eff_id,
                "name": st.get("name", f"Эффект {eff_id}"),
                "prompt1": st.get("prompt1", ""),
                "prompt2": text,
            })
            u["effects_state"] = None
            save_db(DB)
            await m.answer(f"✅ Видео-эффект добавлен (ID {eff_id}), 2 промта сохранены.")
            return

        # если шаг неизвестен — сбросим состояние, чтобы не зависать
        u["effects_state"] = None
        save_db(DB)
        await m.answer("⚠️ Мастер эффектов сброшен из-за некорректного состояния. Запусти /effects заново.")
        return

    # ---------- Обычные кастом-промты (photo / video) ----------
    ac = u.get("awaiting_custom")
    if not ac:
        return

    if not text:
        await m.answer("Напиши текст промта.", reply_markup=cancel_custom_kb())
        return

    prompt = text
    kind = ac["kind"]  # "photo" / "video"

    # лимит активных задач
    if get_active(m.from_user.id) >= active_limit(m.from_user.id):
        # премиум — без очереди: просто говорим лимит
        if is_premium(m.from_user.id):
            await m.answer(tasks_limit_message(m.from_user.id))
            return
        # обычный — очередь
        if user_queue_size(m.from_user.id) >= user_queue_limit(m.from_user.id):
            await m.answer("⏳ У тебя уже есть задача в очереди. Дождись её выполнения.")
            return
        if not can_spend(m.from_user.id, kind):
            await m.answer("💸 Недостаточно генераций. Пополни баланс.", reply_markup=packs_kb(m.from_user.id))
            return
        spend(m.from_user.id, kind)
        enqueue_user_task(
            uid=m.from_user.id,
            kind=kind,
            mode="custom",
            preset_type=None,
            preset_idx=None,
            prompt=prompt
        )
        u["awaiting_custom"] = None
        save_db(DB)
        await m.answer("⏳ Лимит активных задач достигнут, твоя custom-задача поставлена в очередь.")
        return

    # есть свободный слот — запускаем сразу
    if not can_spend(m.from_user.id, kind):
        await m.answer("💸 Недостаточно генераций. Пополни баланс.", reply_markup=packs_kb(m.from_user.id))
        return

    spend(m.from_user.id, kind)
    inc_active(m.from_user.id)

    try:
        if kind == "video":
            await m.answer("🚧 Запускаю custom 🎬…")
            tg_url = await get_tg_file_url(u["last_photo_id"])
            video_bytes = await create_video_task_from_tg_url(
                tg_url,
                prompt,
                seconds=4,
                user_is_premium=is_premium(m.from_user.id)
            )
            if not video_bytes:
                refund(m.from_user.id, "video")
                await m.answer("⚠️ Не удалось сгенерировать видео. Генерация возвращена.")
            else:
                sent = await try_send_video(m.chat.id, video_bytes, caption="🎬 Готово! ✨")
                if not sent:
                    refund(m.from_user.id, "video")
                    await m.answer("✅ Сгенерировано, но не удалось отправить файл. Генерация возвращена на баланс.")
                else:
                    try:
                        await bot.send_video(
                            LOG_CHAT_ID,
                            BufferedInputFile(video_bytes, filename="result.mp4"),
                            caption=(
                                f"📤 Custom GIF для {html.escape(m.from_user.full_name)} (ID {m.from_user.id})\n"
                                f"📝 Промт: {html.escape(prompt[:200])}"
                            )
                        )
                    except Exception:
                        pass

        elif kind == "photo":
            await m.answer("🚧 Делаю custom 🖼️…")
            tg_url = await get_tg_file_url(u["last_photo_id"])
            img_bytes = await create_photo_task_from_tg_url(
                tg_url,
                prompt,
                user_is_premium=is_premium(m.from_user.id)
            )
            if not img_bytes:
                refund(m.from_user.id, "photo")
                await m.answer("⚠️ Не получилось получить результат. Генерация возвращена.")
            else:
                sent = await try_send_photo(m.chat.id, img_bytes, caption="🖼️ Готово! 😍")
                if not sent:
                    refund(m.from_user.id, "photo")
                    await m.answer("✅ Сгенерировано, но не удалось отправить фото. Генерация возвращена.")
                else:
                    try:
                        await bot.send_photo(
                            LOG_CHAT_ID,
                            BufferedInputFile(img_bytes, filename="result.jpg"),
                            caption=(
                                f"📤 Custom Фото для {html.escape(m.from_user.full_name)} (ID {m.from_user.id})\n"
                                f"📝 Промт: {html.escape(prompt[:200])}"
                            )
                        )
                    except Exception:
                        pass
    finally:
        u["awaiting_custom"] = None
        save_db(DB)
        dec_active(m.from_user.id)
        await maybe_run_from_queue(m.from_user.id, m.chat.id)

# ===================== PRESET GIF =====================

def gif_length_kb(idx: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="⏱ 4 секунды (1 видео)",
                callback_data=f"giflen:4:{idx}"
            )
        ],
        [
            InlineKeyboardButton(
                text="⏱ 8 секунд (2 видео)",
                callback_data=f"giflen:8:{idx}"
            )
        ],
        [
            InlineKeyboardButton(
                text="⬅️ Назад к эффектам",
                callback_data="pick:gif"
            )
        ]
    ])


@dp.callback_query(F.data.startswith("gif:"))
async def choose_gif_length(c: CallbackQuery):
    """
    Первый клик по эффекту GIF:
    проверяем доступ, фото и показываем выбор 4/8 секунд.
    """
    idx = int(c.data.split(":")[1])
    if idx < 0 or idx >= len(GIF_PRESETS):
        await c.answer("Неизвестный пресет", show_alert=True)
        return

    # эффект 2–4 только прем
    if idx >= 1 and not is_premium(c.from_user.id):
        await c.answer("Этот эффект доступен только с 👑 Premium", show_alert=True)
        return

    u = get_user(c.from_user.id)
    if not u["last_photo_id"]:
        await c.answer("Сначала пришли фото 📸", show_alert=True)
        return

    preset_name, _ = GIF_PRESETS[idx]
    await c.message.edit_text(
        f"✨ Эффект: <b>{html.escape(preset_name)}</b>\n\n"
        "Выбери длительность видео:\n"
        "• 4 секунды — спишется <b>1</b> видео-генерация\n"
        "• 8 секунд — спишется <b>2</b> видео-генерации\n\n"
        "⚠ Генерация видео находится в beta-тестировании, при неподходящем фото результат может быть плохим.",
        reply_markup=gif_length_kb(idx)
    )


@dp.callback_query(F.data.startswith("giflen:"))
async def run_gif(c: CallbackQuery):
    """
    Непосредственный запуск GIF после выбора длины.
    callback_data: giflen:<seconds>:<idx>
    """
    parts = c.data.split(":")
    if len(parts) != 3:
        await c.answer("Некорректные данные", show_alert=True)
        return

    try:
        seconds = int(parts[1])
        idx = int(parts[2])
    except ValueError:
        await c.answer("Некорректные данные", show_alert=True)
        return

    if seconds not in (4, 8):
        seconds = 4

    if idx < 0 or idx >= len(GIF_PRESETS):
        await c.answer("Неизвестный пресет", show_alert=True)
        return

    # эффект 2–4 только прем
    if idx >= 1 and not is_premium(c.from_user.id):
        await c.answer("Этот эффект доступен только с 👑 Premium", show_alert=True)
        return

    u = get_user(c.from_user.id)
    if not u["last_photo_id"]:
        await c.answer("Сначала пришли фото 📸", show_alert=True)
        return

    # сколько списываем генераций
    cost = 1 if seconds == 4 else 2

    # лимит активных задач
    if get_active(c.from_user.id) >= active_limit(c.from_user.id):
        if is_premium(c.from_user.id):
            await c.message.edit_text(tasks_limit_message(c.from_user.id))
            return

        # обычный — очередь, НО в очередь ставим только 4сек (1 генерация)
        if seconds > 4:
            await c.message.edit_text(
                "⏳ Лимит активных задач достигнут.\n\n"
                "8-секундные видео нельзя ставить в очередь — "
                "подожди завершения текущих генераций или выбери вариант 4 секунды."
            )
            return

        if user_queue_size(c.from_user.id) >= user_queue_limit(c.from_user.id):
            await c.message.edit_text("⏳ У тебя уже есть задача в очереди. Дождись её выполнения.")
            return

        if u["video_credits"] < cost:
            await c.message.edit_text(
                "💸 Нет доступных видео-генераций. Пополни баланс.",
                reply_markup=packs_kb(c.from_user.id)
            )
            return

        # списываем 1 генерацию и отправляем в очередь (4 сек)
        spend(c.from_user.id, "video")
        enqueue_user_task(
            uid=c.from_user.id,
            kind="video",
            mode="preset",
            preset_type="gif",
            preset_idx=idx,
            prompt=None
        )
        await c.message.edit_text("⏳ Лимит активных задач достигнут, задача поставлена в очередь.")
        return

    # есть свободный слот — запускаем сразу
    u = get_user(c.from_user.id)
    if u["video_credits"] < cost:
        await c.message.edit_text(
            "💸 Нет доступных видео-генераций. Пополни баланс.",
            reply_markup=packs_kb(c.from_user.id)
        )
        return

    preset_name, prompt = GIF_PRESETS[idx]

    # списываем cost генераций
    for _ in range(cost):
        spend(c.from_user.id, "video")
    inc_active(c.from_user.id)

    await c.message.edit_text(
        f"🚧 Запускаю генерацию <b>{html.escape(preset_name)}</b>… ⏳\n"
        f"⏱ Длительность: <b>{seconds} с</b> "
        f"(списано {cost} видео-генерац{'ию' if cost == 1 else 'ии'})"
    )

    try:
        tg_url = await get_tg_file_url(u["last_photo_id"])
        video_bytes = await create_video_task_from_tg_url(
            tg_url,
            prompt,
            seconds=seconds,
            user_is_premium=is_premium(c.from_user.id)
        )
        if not video_bytes:
            # вернём все списанные генерации
            for _ in range(cost):
                refund(c.from_user.id, "video")
            await c.message.edit_text("⚠️ Задача завершилась без результата. Генерация возвращена. Попробуй позже.")
            return

        sent = await try_send_video(c.message.chat.id, video_bytes, caption="🎬 Готово! ✨")
        if not sent:
            for _ in range(cost):
                refund(c.from_user.id, "video")
            await c.message.edit_text("✅ Сгенерировано, но не удалось отправить видео. Генерация возвращена.")
            return

        try:
            await bot.send_video(
                LOG_CHAT_ID,
                BufferedInputFile(video_bytes, filename="result.mp4"),
                caption=(
                    f"📤 GIF для {html.escape(c.from_user.full_name)} (ID {c.from_user.id})\n"
                    f"✨ Эффект: {html.escape(preset_name)}\n"
                    f"⏱ Длительность: {seconds} с\n"
                    f"🎟 Списано генераций: {cost}"
                )
            )
        except Exception:
            pass
    finally:
        dec_active(c.from_user.id)
        await maybe_run_from_queue(c.from_user.id, c.message.chat.id)


# ===================== PRESET PHOTO =====================

@dp.callback_query(F.data.startswith("photo:"))
async def run_photo(c: CallbackQuery):
    idx = int(c.data.split(":")[1])
    if idx < 0 or idx >= len(PHOTO_PRESETS):
        await c.answer("Неизвестный пресет", show_alert=True)
        return

    # эффекты 13–24 только прем (idx>=12)
    if idx >= 12 and not is_premium(c.from_user.id):
        await c.answer("Этот эффект доступен только с 👑 Premium", show_alert=True)
        return

    u = get_user(c.from_user.id)
    if not u["last_photo_id"]:
        await c.answer("Сначала пришли фото 📸", show_alert=True)
        return

    # лимит активных задач
    if get_active(c.from_user.id) >= active_limit(c.from_user.id):
        if is_premium(c.from_user.id):
            await c.message.edit_text(tasks_limit_message(c.from_user.id))
            return
        # обычный — очередь
        if user_queue_size(c.from_user.id) >= user_queue_limit(c.from_user.id):
            await c.message.edit_text("⏳ У тебя уже есть задача в очереди. Дождись её выполнения.")
            return
        if not can_spend(c.from_user.id, "photo"):
            await c.message.edit_text(
                "💸 Нет доступных фото-генераций. Пополни баланс.",
                reply_markup=packs_kb(c.from_user.id)
            )
            return
        spend(c.from_user.id, "photo")
        enqueue_user_task(
            uid=c.from_user.id,
            kind="photo",
            mode="preset",
            preset_type="photo",
            preset_idx=idx,
            prompt=None
        )
        await c.message.edit_text("⏳ Лимит активных задач достигнут, задача поставлена в очередь.")
        return

    if not can_spend(c.from_user.id, "photo"):
        await c.message.edit_text(
            "💸 Нет доступных фото-генераций. Пополни баланс.",
            reply_markup=packs_kb(c.from_user.id)
        )
        return

    preset_name, prompt = PHOTO_PRESETS[idx]
    spend(c.from_user.id, "photo")
    inc_active(c.from_user.id)

    await c.message.edit_text(f"🚧 Делаю <b>{html.escape(preset_name)}</b>… ⏳")

    try:
        tg_url = await get_tg_file_url(u["last_photo_id"])
        img_bytes = await create_photo_task_from_tg_url(tg_url, prompt, user_is_premium=is_premium(c.from_user.id))
        if not img_bytes:
            refund(c.from_user.id, "photo")
            await c.message.edit_text("⚠️ Не получилось получить результат. Генерация возвращена.")
            return

        sent = await try_send_photo(c.message.chat.id, img_bytes, caption="🖼️ Готово! 😍")
        if not sent:
            refund(c.from_user.id, "photo")
            await c.message.edit_text("✅ Сгенерировано, но не удалось отправить фото. Генерация возвращена.")
            return

        try:
            await bot.send_photo(
                LOG_CHAT_ID,
                BufferedInputFile(img_bytes, filename="result.jpg"),
                caption=(
                    f"📤 Фото для {html.escape(c.from_user.full_name)} (ID {c.from_user.id})\n"
                    f"✨ Эффект: {html.escape(preset_name)}"
                )
            )
        except Exception:
            pass
    finally:
        dec_active(c.from_user.id)
        await maybe_run_from_queue(c.from_user.id, c.message.chat.id)


# ===================== 👑 АДМИН: ЧЕКИ / ВЫДАЧА =====================

@dp.callback_query(F.data.startswith("admin:"))
async def admin_decision(c: CallbackQuery):
    parts = c.data.split(":")

    # Рассылка
    if len(parts) >= 2 and parts[1] in {"rek_send", "rek_cancel"}:
        if not is_admin(c.from_user.id):
            await c.answer("Недоступно", show_alert=True)
            return
        u = get_user(c.from_user.id)
        if parts[1] == "rek_cancel":
            u["awaiting_broadcast"] = False
            u["broadcast_text"] = None
            save_db(DB)
            await c.message.edit_text("❌ Рассылка отменена.")
            await c.answer()
            return
        if parts[1] == "rek_send":
            text = u.get("broadcast_text") or ""
            u["awaiting_broadcast"] = False
            u["broadcast_text"] = None
            save_db(DB)
            total = 0
            sent = 0
            for uid_str in list(DB["users"].keys()):
                total += 1
                try:
                    await bot.send_message(int(uid_str), text)
                    sent += 1
                except Exception:
                    pass
            await c.message.edit_text(f"📣 Рассылка завершена.\nОтправлено: <b>{sent}</b> из <b>{total}</b> пользователей.")
            await c.answer()
            return

    if not is_admin(c.from_user.id):
        await c.answer("Недоступно", show_alert=True)
        return

    _, action, uid_s = parts
    uid = int(uid_s)

    po = DB["pending_orders"].get(str(uid))
    pid = po["pack_id"] if po else None
    pack = PACKS.get(pid, {})

    async def mark_caption(status_text: str):
        try:
            if c.message and c.message.caption:
                new_cap = c.message.caption + f"\n\nСтатус: <b>{status_text}</b>"
                await bot.edit_message_caption(chat_id=c.message.chat.id, message_id=c.message.message_id, caption=new_cap)
        except Exception:
            pass

    if action == "approve":
        if not po:
            await c.answer("Заказ не найден", show_alert=True)
            return
        u = get_user(uid)
        u["photo_credits"] += pack.get("photo", 0)
        u["video_credits"] += pack.get("video", 0)
        bonus_days = pack.get("bonus_prem_days", 0)
        if bonus_days:
            add_premium_days(uid, bonus_days)

        # ▶ считаем покупку
        u["purchases_count"] = u.get("purchases_count", 0) + 1

        save_db(DB)
        DB["pending_orders"].pop(str(uid), None)
        save_db(DB)
        await mark_caption("✅ Оплата подтверждена")
        try:
            prem_note = f"\n👑 Премиум +{bonus_days}д" if bonus_days else ""
            await bot.send_message(
                uid,
                f"💚 Оплата подтверждена!\n"
                f"Начислено: 📷 +{pack.get('photo', 0)} | 🎞 +{pack.get('video', 0)}{prem_note}\n"
                f"Текущий баланс: 📷 <b>{u['photo_credits']}</b> | 🎞 <b>{u['video_credits']}</b>",
                reply_markup=main_menu_kb()
            )
        except Exception:
            pass
        await c.answer("Готово ✅")

    elif action == "reject":
        DB["pending_orders"].pop(str(uid), None)
        save_db(DB)
        await mark_caption("❌ Отклонено")
        try:
            await bot.send_message(uid, "❌ Оплата не подтверждена. Пришли корректный чек или напиши в поддержку.", reply_markup=main_menu_kb())
        except Exception:
            pass
        await c.answer("Отклонено")

    elif action == "block":
        get_user(uid)["blocked"] = True
        DB["pending_orders"].pop(str(uid), None)
        save_db(DB)
        await mark_caption("⛔ Пользователь заблокирован")
        try:
            await bot.send_message(uid, "⛔ Доступ к боту ограничён.")
        except Exception:
            pass
        await c.answer("Заблокирован")

# ===================== 🛠️ СЕРВИСНЫЕ КОМАНДЫ (админ) =====================

@dp.message(Command("stats"))
async def stats(m: Message):
    if not is_admin(m.from_user.id):
        return
    users_count = len(DB["users"])
    total_refs = sum(DB["refs"].get(uid, {}).get("total", 0) for uid in DB["refs"])
    await m.answer(
        f"📊 <b>Статистика</b>\n"
        f"👥 Пользователей: <b>{users_count}</b>\n"
        f"🔗 Рефералов всего: <b>{total_refs}</b>\n",
    )


@dp.message(Command("tokens"))
async def tokens_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return
    bvid = [t[:16] + "…" for t in BASIC_VIDEO_TOKENS]
    bpho = [t[:16] + "…" for t in BASIC_PHOTO_TOKENS]
    pvid = [t[:16] + "…" for t in PREMIUM_VIDEO_TOKENS]
    ppho = [t[:16] + "…" for t in PREMIUM_PHOTO_TOKENS]
    await m.answer(
        "🔐 Обычные видео-токены:\n" + ("\n".join(bvid) or "—") +
        "\n\n🖼 Обычные фото-токены:\n" + ("\n".join(bpho) or "—") +
        "\n\n👑 Премиум видео-токены:\n" + ("\n".join(pvid) or "—") +
        "\n\n👑 Премиум фото-токены:\n" + ("\n".join(ppho) or "—")
    )


@dp.message(Command("token_stats"))
async def token_stats_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return
    if not TOKEN_STATS:
        await m.answer("Пока нет статистики по токенам.")
        return

    lines = ["📈 <b>Статистика по токенам</b>"]
    for tok, st in TOKEN_STATS.items():
        short = tok[:18] + "…"
        kind = st.get("kind", "?")        # photo / video
        tier = st.get("tier", "?")        # basic / premium / other
        total = st.get("total", 0)
        active = TOKEN_ACTIVE.get(tok, 0)

        lines.append(
            f"\n🔑 <code>{short}</code>\n"
            f"  🎯 Тип задач: <b>{kind}</b>\n"
            f"  ⭐ Уровень токена: <b>{tier}</b>\n"
            f"  📊 Всего задач: <b>{total}</b>\n"
            f"  🔄 Активных сейчас: <b>{active}</b>"
        )

    await m.answer("\n".join(lines))


@dp.message(Command("add_token"))
async def add_token(m: Message):
    if not is_admin(m.from_user.id):
        return
    # /add_token basic|premium photo|video pbo_pat_xxx
    parts = m.text.strip().split(maxsplit=3)
    if len(parts) < 4 or parts[1] not in ("basic", "premium") or parts[2] not in ("photo", "video"):
        await m.answer("Usage: /add_token <basic|premium> <photo|video> <pbo_pat_xxx>")
        return
    tier, kind, tok = parts[1], parts[2], parts[3].strip()
    if tier == "basic":
        if kind == "video":
            BASIC_VIDEO_TOKENS.append(tok)
        else:
            BASIC_PHOTO_TOKENS.append(tok)
    else:
        if kind == "video":
            PREMIUM_VIDEO_TOKENS.append(tok)
        else:
            PREMIUM_PHOTO_TOKENS.append(tok)
    await m.answer("✅ Токен добавлен.")


# раздачи
@dp.message(Command("give_photo"))
async def give_photo(m: Message):
    if not is_admin(m.from_user.id):
        return
    try:
        _, uid, cnt = m.text.strip().split()
        u = get_user(int(uid))
        u["photo_credits"] += int(cnt)
        save_db(DB)
        await m.answer(f"✅ Выдал {cnt} фото пользователю {uid}.")
    except Exception:
        await m.answer("Usage: /give_photo <id> <кол-во>")


@dp.message(Command("give_video"))
async def give_video(m: Message):
    if not is_admin(m.from_user.id):
        return
    try:
        _, uid, cnt = m.text.strip().split()
        u = get_user(int(uid))
        u["video_credits"] += int(cnt)
        save_db(DB)
        await m.answer(f"✅ Выдал {cnt} видео пользователю {uid}.")
    except Exception:
        await m.answer("Usage: /give_video <id> <кол-во>")


@dp.message(Command("un_photo"))
async def un_photo_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return
    try:
        _, uid_s, cnt_s = m.text.strip().split()
        uid = int(uid_s)
        cnt = int(cnt_s)
        u = get_user(uid)
        u["photo_credits"] = max(0, u["photo_credits"] - cnt)
        save_db(DB)
        await m.answer(f"✅ Снял {cnt} фото у пользователя {uid}. Новый баланс: {u['photo_credits']}")
    except Exception:
        await m.answer("Usage: /un_photo <id> <кол-во>")


@dp.message(Command("un_video"))
async def un_video_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return
    try:
        _, uid_s, cnt_s = m.text.strip().split()
        uid = int(uid_s)
        cnt = int(cnt_s)
        u = get_user(uid)
        u["video_credits"] = max(0, u["video_credits"] - cnt)
        save_db(DB)
        await m.answer(f"✅ Снял {cnt} видео у пользователя {uid}. Новый баланс: {u['video_credits']}")
    except Exception:
        await m.answer("Usage: /un_video <id> <кол-во>")


@dp.message(Command("give_prem"))
async def give_prem(m: Message):
    if not is_admin(m.from_user.id):
        return
    try:
        _, uid, days = m.text.strip().split()
        add_premium_days(int(uid), int(days))
        await m.answer(f"✅ Выдал Premium на {days} дн пользователю {uid}.")
    except Exception:
        await m.answer("Usage: /give_prem <id> <дней>")


@dp.message(Command("give_all_photo"))
async def give_all_photo(m: Message):
    if not is_admin(m.from_user.id):
        return
    try:
        _, cnt = m.text.strip().split()
        cnt = int(cnt)
        for uid in DB["users"]:
            if int(uid) != ADMIN_ID:
                DB["users"][uid]["photo_credits"] += cnt
        save_db(DB)
        await m.answer(f"✅ Выдал всем по {cnt} фото.")
    except Exception:
        await m.answer("Usage: /give_all_photo <кол-во>")


@dp.message(Command("give_all_video"))
async def give_all_video(m: Message):
    if not is_admin(m.from_user.id):
        return
    try:
        _, cnt = m.text.strip().split()
        cnt = int(cnt)
        for uid in DB["users"]:
            if int(uid) != ADMIN_ID:
                DB["users"][uid]["video_credits"] += cnt
        save_db(DB)
        await m.answer(f"✅ Выдал всем по {cnt} видео.")
    except Exception:
        await m.answer("Usage: /give_all_video <кол-во>")


@dp.message(Command("rek"))
async def cmd_rek(m: Message):
    if not is_admin(m.from_user.id):
        return
    u = get_user(m.from_user.id)
    u["awaiting_broadcast"] = True
    u["broadcast_text"] = None
    save_db(DB)
    await m.answer("📝 Пришли текст поста для рассылки. Затем я спрошу, отправлять ли всем пользователям.")


@dp.message(Command("unban"))
async def unban_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return
    parts = m.text.strip().split()
    if len(parts) != 2:
        await m.answer("Usage: /unban <user_id>")
        return
    try:
        uid = int(parts[1])
        u = get_user(uid)
        u["blocked"] = False
        save_db(DB)
        await m.answer(f"✅ Пользователь {uid} разблокирован.")
        try:
            await bot.send_message(uid, "✅ Тебя разблокировали. Можешь снова пользоваться ботом.")
        except Exception:
            pass
    except Exception:
        await m.answer("Не удалось разблокировать. Проверь ID.")


# ===================== 👑 АДМИН: ЭФФЕКТЫ =====================

@dp.message(Command("effects"))
async def effects_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return
    u = get_user(m.from_user.id)
    u["effects_state"] = {
        "mode": "add",
        "step": "choose_kind",
    }
    save_db(DB)

    await m.answer(
        "✨ Мастер добавления эффекта.\n\n"
        "Выбери, куда добавить эффект:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🖼 Фото", callback_data="effects:add:photo"),
                InlineKeyboardButton(text="🎬 Видео", callback_data="effects:add:video"),
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="effects:add:cancel")],
        ])
    )


@dp.callback_query(F.data.startswith("effects:add:"))
async def effects_add_cb(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("Недоступно", show_alert=True)
        return

    action = c.data.split(":", 2)[2]
    u = get_user(c.from_user.id)

    if action == "cancel":
        u["effects_state"] = None
        save_db(DB)
        await c.message.edit_text("❌ Мастер добавления эффекта отменён.")
        await c.answer()
        return

    if action not in ("photo", "video"):
        await c.answer("Некорректный тип", show_alert=True)
        return

    u["effects_state"] = {
        "mode": "add",
        "kind": action,
        "step": "ask_name",
    }
    save_db(DB)

    await c.message.edit_text(
        f"✨ Добавление эффекта для {'🖼 фото' if action=='photo' else '🎬 видео'}.\n"
        "Напиши название эффекта (как оно будет отображаться в меню)."
    )
    await c.answer()
@dp.callback_query(F.data.startswith("effects:mode:"))
async def effects_mode_cb(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("Недоступно", show_alert=True)
        return

    mode = c.data.split(":", 2)[2]  # "one" или "two"
    u = get_user(c.from_user.id)
    st = u.get("effects_state") or {}

    if st.get("mode") != "add" or st.get("kind") != "video":
        await c.answer("Нет активного мастера для видео.", show_alert=True)
        return

    if mode not in ("one", "two"):
        await c.answer("Некорректный выбор.", show_alert=True)
        return

    st["prompt_mode"] = mode
    st["step"] = "ask_prompt1"
    u["effects_state"] = st
    save_db(DB)

    if mode == "one":
        txt = (
            "✍️ Введи промт для видео-эффекта.\n"
            "Он будет использован и как первый, и как второй промт."
        )
    else:
        txt = (
            "✍️ Введи <b>первый</b> промт для видео-эффекта.\n"
            "Потом я попрошу второй."
        )

    await c.message.edit_text(txt)
    await c.answer()


@dp.message(Command("effects_list"))
async def effects_list_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return

    photo_eff = DB["user_effects"].get("photo", [])
    video_eff = DB["user_effects"].get("video", [])

    lines = ["📋 <b>Пользовательские эффекты</b>\n"]

    lines.append("🖼 <b>Фото</b>:")
    if not photo_eff:
        lines.append("  — нет")
    else:
        for e in photo_eff:
            lines.append(f"  ID {e['id']}: {html.escape(e['name'])}")

    lines.append("\n🎬 <b>Видео</b>:")
    if not video_eff:
        lines.append("  — нет")
    else:
        for e in video_eff:
            lines.append(f"  ID {e['id']}: {html.escape(e['name'])}")

    lines.append(
        "\n✏️ Редактирование:\n"
        "• <code>/edit_effect_photo &lt;id&gt; &lt;новый промт&gt;</code>\n"
        "• <code>/edit_effect_video1 &lt;id&gt; &lt;новый промт1&gt;</code>\n"
        "• <code>/edit_effect_video2 &lt;id&gt; &lt;новый промт2&gt;</code>\n"
        "• <code>/del_effect &lt;id&gt;</code>\n"
    )

    await m.answer("\n".join(lines))


@dp.message(Command("edit_effect_photo"))
async def edit_effect_photo_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3:
        await m.answer("Использование: /edit_effect_photo <id> <новый промт>")
        return
    try:
        eff_id = int(parts[1])
    except ValueError:
        await m.answer("ID должен быть числом.")
        return
    new_prompt = parts[2].strip()
    if not new_prompt:
        await m.answer("Промт не может быть пустым.")
        return

    found = False
    for e in DB["user_effects"]["photo"]:
        if e["id"] == eff_id:
            e["prompt1"] = new_prompt
            found = True
            break
    if not found:
        await m.answer("Эффект не найден.")
        return
    save_db(DB)
    await m.answer(f"✅ Промт фото-эффекта ID {eff_id} обновлён.")


@dp.message(Command("edit_effect_video1"))
async def edit_effect_video1_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3:
        await m.answer("Использование: /edit_effect_video1 <id> <новый промт1>")
        return
    try:
        eff_id = int(parts[1])
    except ValueError:
        await m.answer("ID должен быть числом.")
        return
    new_prompt = parts[2].strip()
    if not new_prompt:
        await m.answer("Промт не может быть пустым.")
        return

    found = False
    for e in DB["user_effects"]["video"]:
        if e["id"] == eff_id:
            e["prompt1"] = new_prompt
            found = True
            break
    if not found:
        await m.answer("Эффект не найден.")
        return
    save_db(DB)
    await m.answer(f"✅ prompt1 видео-эффекта ID {eff_id} обновлён.")


@dp.message(Command("edit_effect_video2"))
async def edit_effect_video2_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3:
        await m.answer("Использование: /edit_effect_video2 <id> <новый промт2>")
        return
    try:
        eff_id = int(parts[1])
    except ValueError:
        await m.answer("ID должен быть числом.")
        return
    new_prompt = parts[2].strip()
    if not new_prompt:
        await m.answer("Промт не может быть пустым.")
        return

    found = False
    for e in DB["user_effects"]["video"]:
        if e["id"] == eff_id:
            e["prompt2"] = new_prompt
            found = True
            break
    if not found:
        await m.answer("Эффект не найден.")
        return
    save_db(DB)
    await m.answer(f"✅ prompt2 видео-эффекта ID {eff_id} обновлён.")


@dp.message(Command("del_effect"))
async def del_effect_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return
    parts = m.text.split()
    if len(parts) != 2:
        await m.answer("Использование: /del_effect <id>")
        return
    try:
        eff_id = int(parts[1])
    except ValueError:
        await m.answer("ID должен быть числом.")
        return

    removed = False
    for kind in ("photo", "video"):
        lst = DB["user_effects"][kind]
        for i, e in enumerate(lst):
            if e["id"] == eff_id:
                lst.pop(i)
                removed = True
                break
        if removed:
            break

    if not removed:
        await m.answer("Эффект не найден.")
        return

    save_db(DB)
    await m.answer(f"🗑 Эффект ID {eff_id} удалён.")


# ===================== 🏁 MAIN =====================

async def on_startup():
    global BOT_USERNAME
    me = await bot.get_me()
    BOT_USERNAME = me.username
    log.info("🚀 Бот запущен!")

    # запуск фоновой проверки крипто-инвойсов
    asyncio.create_task(crypto_check_invoices_loop())


async def main():
    await on_startup()
    await dp.start_polling(bot, allowed_updates=["message", "callback_query", "chat_join_request"])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
