#!/usr/bin/env python3
"""
🔥 AutoTech AI News Bot - Production Edition v3.1 (With Dashboard)
====================================================================
With FREE Image Generation + Backfill + Branding + Web Dashboard
Deployed on Render.com | Groq API (Free Tier) | Multi-Key Rotation
Cron: cron-job.org (every minute) | Uptime: UptimeRobot (every 10 min)

Environment Variables:
 GROQ_API_KEYS - Comma-separated Groq keys
 TELEGRAM_BOT_TOKEN - Telegram bot token
 TELEGRAM_CHANNEL_ID - e.g. @PulseAI_ir
 CRON_SECRET - Secret token for /cron endpoint
 RENDER_DISK_PATH - /opt/render/project/src/data
 ENABLE_IMAGES - true/false (default: true)
 IMAGE_SOURCE - og|ai|both (default: both)
 BACKFILL_MODE - true/false (default: false)
"""

import os
import re
import json
import time
import hashlib
import sqlite3
import logging
import threading
import feedparser
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from urllib.parse import quote
from dateutil import parser as date_parser

from flask import Flask, jsonify, request, render_template_string

# ═══════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════
class Config:
    GROQ_API_KEYS = [k.strip() for k in os.getenv("GROQ_API_KEYS", "").split(",") if k.strip()]
    GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-70b-versatile")
    GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")

    CRON_SECRET = os.getenv("CRON_SECRET", "change-me-please")

    RENDER_DISK_PATH = os.getenv("RENDER_DISK_PATH", "./data")
    DB_PATH = os.path.join(RENDER_DISK_PATH, "news.db")

    POST_INTERVAL_MINUTES = int(os.getenv("POST_INTERVAL_MINUTES", "30"))
    MAX_NEWS_PER_RUN = int(os.getenv("MAX_NEWS_PER_RUN", "3"))
    SUMMARY_MAX_CHARS = int(os.getenv("SUMMARY_MAX_CHARS", "900"))
    MAX_RETRIES = 3
    RETRY_DELAY_BASE = 2

    ENABLE_IMAGES = os.getenv("ENABLE_IMAGES", "true").lower() == "true"
    IMAGE_SOURCE = os.getenv("IMAGE_SOURCE", "both")
    BACKFILL_MODE = os.getenv("BACKFILL_MODE", "false").lower() == "true"
    BACKFILL_START_DATE = os.getenv("BACKFILL_START_DATE", "2026-01-01")
    BACKFILL_END_DATE = os.getenv("BACKFILL_END_DATE", "2026-08-15")
    BACKFILL_DELAY_SECONDS = int(os.getenv("BACKFILL_DELAY_SECONDS", "5"))

# ═══════════════════════════════════════════════════════════════
# RSS Sources
# ═══════════════════════════════════════════════════════════════
RSS_SOURCES = {
    "TechCrunch": "https://techcrunch.com/feed/",
    "The Verge": "https://www.theverge.com/rss/index.xml",
    "Ars Technica": "https://feeds.arstechnica.com/arstechnica/index",
    "MIT Tech Review": "https://www.technologyreview.com/feed/",
    "VentureBeat": "https://venturebeat.com/feed/",
    "Wired": "https://www.wired.com/feed/rss",
    "OpenAI Blog": "https://openai.com/blog/rss.xml",
    "AI News": "https://www.artificialintelligence-news.com/feed/",
    "HuggingFace Blog": "https://huggingface.co/blog/feed.xml",
    "Google AI Blog": "https://ai.googleblog.com/feeds/posts/default",
    "Machine Learning Mastery": "https://machinelearningmastery.com/feed/",
    "Towards Data Science": "https://towardsdatascience.com/feed",
}

AI_TECH_KEYWORDS = [
    "artificial intelligence", "machine learning", "deep learning", "neural",
    "llm", "large language model", "gpt", "chatbot", "generative ai", "ai model",
    "openai", "anthropic", "google gemini", "claude", "midjourney", "stable diffusion",
    "robotics", "automation", "nvidia", "tesla", "spacex", "quantum",
    "blockchain", "crypto", "cybersecurity", "cloud", "edge computing",
    "5g", "iot", "virtual reality", "augmented reality", "metaverse",
    "startup", "venture capital", "funding", "ipo", "acquisition",
    "python", "tensorflow", "pytorch", "hugging face", "data science",
    "llama", "mistral", "groq", "api", "sdk", "framework", "benchmark",
    "gpu", "tpu", "inference", "training", "fine-tuning", "rag",
    "computer vision", "nlp", "natural language", "multimodal",
]

# ═══════════════════════════════════════════════════════════════
# Database
# ═══════════════════════════════════════════════════════════════
class NewsDatabase:
    def __init__(self):
        os.makedirs(Config.RENDER_DISK_PATH, exist_ok=True)
        self.db_path = Config.DB_PATH
        self.lock = threading.Lock()
        self.init_db()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        with self._connect() as conn:
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS sent_news (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_hash TEXT UNIQUE NOT NULL,
                    title TEXT, source TEXT, url TEXT,
                    image_url TEXT, image_source TEXT,
                    impact_level TEXT, time_to_impact TEXT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS system_state (
                    key TEXT PRIMARY KEY, value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def is_duplicate(self, url: str) -> bool:
        h = hashlib.md5(url.encode()).hexdigest()
        with self.lock, self._connect() as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM sent_news WHERE news_hash=?", (h,))
            return c.fetchone() is not None

    def mark_as_sent(self, url: str, title: str, source: str,
                     image_url: str = "", image_source: str = "",
                     impact_level: str = "", time_to_impact: str = ""):
        h = hashlib.md5(url.encode()).hexdigest()
        with self.lock, self._connect() as conn:
            c = conn.cursor()
            c.execute(
                """INSERT OR IGNORE INTO sent_news
                (news_hash,title,source,url,image_url,image_source,impact_level,time_to_impact)
                VALUES (?,?,?,?,?,?,?,?)""",
                (h, title, source, url, image_url, image_source, impact_level, time_to_impact)
            )
            conn.commit()

    def get_last_run(self) -> Optional[datetime]:
        with self.lock, self._connect() as conn:
            c = conn.cursor()
            c.execute("SELECT value FROM system_state WHERE key='last_run'")
            row = c.fetchone()
            return datetime.fromisoformat(row["value"]) if row else None

    def set_last_run(self, dt: datetime):
        with self.lock, self._connect() as conn:
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO system_state (key,value) VALUES (?,?)", ("last_run", dt.isoformat()))
            conn.commit()

    def get_stats(self) -> Dict:
        with self.lock, self._connect() as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) as total FROM sent_news")
            total = c.fetchone()["total"]
            c.execute("SELECT COUNT(*) as today FROM sent_news WHERE date(sent_at)=date('now')")
            today = c.fetchone()["today"]
            c.execute("SELECT source, COUNT(*) as cnt FROM sent_news GROUP BY source ORDER BY cnt DESC LIMIT 5")
            top = [dict(r) for r in c.fetchall()]
            c.execute("SELECT COUNT(*) as with_img FROM sent_news WHERE image_url!=''")
            with_img = c.fetchone()["with_img"]
            c.execute("SELECT impact_level, COUNT(*) as cnt FROM sent_news WHERE impact_level!='' GROUP BY impact_level")
            impacts = [dict(r) for r in c.fetchall()]
            return {
                "total_sent": total, "today_sent": today,
                "with_image": with_img, "top_sources": top,
                "impact_distribution": impacts
            }

    def cleanup_old(self, days: int = 60):
        with self.lock, self._connect() as conn:
            c = conn.cursor()
            cutoff = (datetime.now() - timedelta(days=days)).isoformat()
            c.execute("DELETE FROM sent_news WHERE sent_at < ?", (cutoff,))
            conn.commit()
            logger.info(f"🧹 Cleaned {c.rowcount} old records")

    def get_recent(self, limit: int = 20):
        with self.lock, self._connect() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT title, source, url, image_url, image_source, impact_level, time_to_impact, sent_at
                FROM sent_news ORDER BY sent_at DESC LIMIT ?
            """, (limit,))
            return [dict(r) for r in c.fetchall()]

# ═══════════════════════════════════════════════════════════════
# Groq Key Manager
# ═══════════════════════════════════════════════════════════════
class GroqKeyManager:
    def __init__(self, keys: List[str]):
        self.keys = keys
        self.current_index = 0
        self.failed_keys = set()
        self.last_used = {k: 0 for k in keys}
        self.lock = threading.Lock()
        if not keys:
            logger.error("❌ No Groq API keys provided!")

    def get_key(self) -> Optional[str]:
        with self.lock:
            if not self.keys:
                logger.error("❌ GROQ_API_KEYS is not configured")
                return None

            available = [k for k in self.keys if k not in self.failed_keys]
            if not available:
                logger.warning("⚠️ All Groq keys failed! Resetting...")
                self.failed_keys.clear()
                available = self.keys

            if not available:
                logger.error("❌ No Groq key is currently available")
                return None

            available.sort(key=lambda k: self.last_used.get(k, 0))
            key = available[0]
            self.last_used[key] = time.time()
            return key

    def mark_failed(self, key: str):
        with self.lock:
            self.failed_keys.add(key)
            logger.warning(f"🔴 Groq key failed ({len(self.failed_keys)}/{len(self.keys)})")

    def mark_success(self, key: str):
        with self.lock:
            if key in self.failed_keys:
                self.failed_keys.remove(key)
                logger.info("🟢 Groq key recovered")

# ═══════════════════════════════════════════════════════════════
# AI Processor with Branding Features
# ═══════════════════════════════════════════════════════════════
class AIProcessor:
    def __init__(self, key_manager: GroqKeyManager):
        self.key_manager = key_manager
        self.session = requests.Session()

    def _call_groq(self, prompt: str, max_retries: int = Config.MAX_RETRIES) -> Optional[str]:
        for attempt in range(max_retries):
            key = self.key_manager.get_key()
            if not key:
                return None
            headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
            payload = {
                "model": Config.GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": "You are an expert Persian tech news editor."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.7, "max_tokens": 900
            }
            try:
                resp = self.session.post(Config.GROQ_URL, headers=headers, json=payload, timeout=30)
                if resp.status_code == 429:
                    logger.warning(f"⏳ Groq rate limit (attempt {attempt+1})")
                    self.key_manager.mark_failed(key)
                    time.sleep(Config.RETRY_DELAY_BASE ** attempt)
                    continue
                resp.raise_for_status()
                result = resp.json()
                self.key_manager.mark_success(key)
                return result["choices"][0]["message"]["content"].strip()
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Groq request error: {e}")
                time.sleep(Config.RETRY_DELAY_BASE ** attempt)
            except Exception as e:
                logger.error(f"❌ Groq unexpected error: {e}")
                return None
        return None

    def analyze_news(self, title: str, content: str) -> Optional[Dict]:
        prompt = f"""You are a professional tech news editor for a premium Persian AI news channel called "پالس هوش".

TASK: Analyze the following tech/AI news and produce a complete branded post in Persian.

RULES:
- Write in natural, engaging Persian
- Maximum {Config.SUMMARY_MAX_CHARS} characters for the main text
- Use relevant emojis
- Tone: professional, insider, confident

OUTPUT FORMAT (exactly follow this structure):

IMPACT_LEVEL: [🔴 Critical OR 🟡 Major OR 🟢 Awareness]
TIME_TO_IMPACT: [⚡ Immediate OR 📅 This Month OR 🔮 Future]
HEADLINE: [Catchy Persian headline]
SUMMARY: [2-3 sentence summary in Persian]
WHY_IT_MATTERS: [1 sentence: what this means for the reader]

ORIGINAL TITLE: {title}
CONTENT: {content[:3000]}

Return ONLY the formatted output above."""

        result = self._call_groq(prompt)
        if not result:
            return None

        parsed = {"impact_level": "", "time_to_impact": "", "headline": "",
                  "summary": "", "why_it_matters": ""}

        for line in result.split("\n"):
            line = line.strip()
            if line.startswith("IMPACT_LEVEL:"):
                parsed["impact_level"] = line.replace("IMPACT_LEVEL:", "").strip()
            elif line.startswith("TIME_TO_IMPACT:"):
                parsed["time_to_impact"] = line.replace("TIME_TO_IMPACT:", "").strip()
            elif line.startswith("HEADLINE:"):
                parsed["headline"] = line.replace("HEADLINE:", "").strip()
            elif line.startswith("SUMMARY:"):
                parsed["summary"] = line.replace("SUMMARY:", "").strip()
            elif line.startswith("WHY_IT_MATTERS:"):
                parsed["why_it_matters"] = line.replace("WHY_IT_MATTERS:", "").strip()

        return parsed if parsed["headline"] else None

    def format_post(self, analysis: Dict, link: str) -> str:
        text = f"""{analysis.get('impact_level', '')} | {analysis.get('time_to_impact', '')}

🎯 {analysis.get('headline', '')}

📋 {analysis.get('summary', '')}

💡 {analysis.get('why_it_matters', '')}

🔗 لینک خبر کامل در کپشن

#AI #TechNews #پالس_هوش"""
        return text.strip()

    def generate_image_prompt(self, title: str, summary: str) -> Optional[str]:
        prompt = f"""Create a short, vivid English image generation prompt (max 15 words) for this tech/AI news.
Make it descriptive and visual. No text/words in the image concept.

Title: {title}
Summary: {summary[:500]}

Output ONLY the prompt, nothing else."""
        result = self._call_groq(prompt)
        if result:
            result = result.strip().strip('"').strip("'")
            if len(result) > 200:
                result = result[:200]
            return result

# ═══════════════════════════════════════════════════════════════
# Image Service - FREE
# ═══════════════════════════════════════════════════════════════
class ImageService:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

    def get_og_image(self, url: str) -> Optional[str]:
        try:
            logger.info(f"🖼️ Fetching OG image from {url[:60]}...")
            resp = self.session.get(url, timeout=15, allow_redirects=True)
            resp.raise_for_status()
            html = resp.text

            og_match = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
            if og_match:
                img_url = og_match.group(1)
                if img_url.startswith("//"):
                    img_url = "https:" + img_url
                elif img_url.startswith("/"):
                    from urllib.parse import urlparse
                    parsed = urlparse(url)
                    img_url = f"{parsed.scheme}://{parsed.netloc}{img_url}"
                logger.info(f"✅ Found OG image")
                return img_url

            tw_match = re.search(r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
            if tw_match:
                img_url = tw_match.group(1)
                if img_url.startswith("//"):
                    img_url = "https:" + img_url
                logger.info(f"✅ Found Twitter image")
                return img_url

            return None
        except Exception as e:
            logger.warning(f"⚠️ OG image failed: {e}")
            return None

    def generate_ai_image(self, prompt: str, seed: int = None) -> str:
        if seed is None:
            seed = hash(prompt) % 100000
        clean_prompt = re.sub(r'[^\w\s,-]', '', prompt).strip()
        encoded = quote(clean_prompt[:500])
        url = f"https://image.pollinations.ai/prompt/{encoded}?width=1024&height=1024&seed={seed}&nologo=true&enhance=true"
        logger.info(f"🎨 Generating AI image via Pollinations...")
        return url

    def get_image_for_article(self, article_url: str, article_title: str,
                              ai_prompt: Optional[str] = None) -> Tuple[Optional[str], str]:
        if not Config.ENABLE_IMAGES:
            return None, "none"

        if Config.IMAGE_SOURCE in ("og", "both"):
            og_img = self.get_og_image(article_url)
            if og_img:
                return og_img, "og"

        if Config.IMAGE_SOURCE in ("ai", "both") and ai_prompt:
            ai_img = self.generate_ai_image(ai_prompt)
            return ai_img, "ai"

        return None, "none"

# ═══════════════════════════════════════════════════════════════
# News Aggregator with Date Filtering
# ═══════════════════════════════════════════════════════════════
class NewsAggregator:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })

    def parse_date(self, date_str: str) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            return date_parser.parse(date_str)
        except:
            return None

    def fetch_feed(self, source_name: str, url: str) -> List[Dict]:
        try:
            logger.info(f"📡 Fetching {source_name}...")
            feed = feedparser.parse(url)
            entries = []
            for entry in feed.entries[:20]:
                entries.append({
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "summary": self._clean_html(entry.get("summary", entry.get("description", ""))),
                    "published": entry.get("published", ""),
                    "published_parsed": self.parse_date(entry.get("published", "")),
                    "source": source_name,
                })
            return entries
        except Exception as e:
            logger.error(f"❌ Error fetching {source_name}: {e}")
            return []

    def _clean_html(self, raw: str) -> str:
        clean = re.sub(r"<[^>]+>", " ", raw)
        clean = re.sub(r"\s+", " ", clean).strip()
        return clean

    def is_relevant(self, title: str, summary: str) -> bool:
        text = (title + " " + summary).lower()
        return any(kw.lower() in text for kw in AI_TECH_KEYWORDS)

    def filter_by_date(self, entries: List[Dict], start: datetime, end: datetime) -> List[Dict]:
        filtered = []
        start_day = start.date()
        end_day = end.date()

        for entry in entries:
            pub_date = entry.get("published_parsed")
            if pub_date:
                if start_day <= pub_date.date() <= end_day:
                    filtered.append(entry)
            else:
                filtered.append(entry)
        return filtered

    def collect_all(self, date_filter: Optional[Tuple[datetime, datetime]] = None) -> List[Dict]:
        all_news = []
        for name, url in RSS_SOURCES.items():
            entries = self.fetch_feed(name, url)
            for entry in entries:
                if self.is_relevant(entry["title"], entry["summary"]):
                    all_news.append(entry)
            time.sleep(0.5)

        if date_filter:
            start, end = date_filter
            all_news = self.filter_by_date(all_news, start, end)
            logger.info(f"📅 Date filter applied: {len(all_news)} articles in range")

        return all_news

# ═══════════════════════════════════════════════════════════════
# Telegram Publisher
# ═══════════════════════════════════════════════════════════════
class TelegramPublisher:
    def __init__(self):
        self.token = Config.TELEGRAM_BOT_TOKEN
        self.channel = Config.TELEGRAM_CHANNEL_ID
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.session = requests.Session()

    def send_text(self, text: str, link: str) -> bool:
        if not self.token or not self.channel:
            logger.error("❌ Telegram credentials missing")
            return False

        full_text = f"{text}\n\n🔗 {link}"
        if len(full_text) > 4096:
            full_text = full_text[:4090] + "..."

        try:
            resp = self.session.post(
                f"{self.base_url}/sendMessage",
                json={"chat_id": self.channel, "text": full_text, "disable_web_page_preview": False},
                timeout=30
            )
            resp.raise_for_status()
            result = resp.json()
            if result.get("ok"):
                logger.info("✅ Text message sent")
                return True
            logger.error(f"❌ Telegram API error: {result}")
            return False
        except Exception as e:
            logger.error(f"❌ Telegram text send error: {e}")
            return False

    def send_photo(self, photo_url: str, caption: str, link: str) -> bool:
        if not self.token or not self.channel:
            return False

        full_caption = f"{caption}\n\n🔗 {link}"
        if len(full_caption) > 1024:
            full_caption = full_caption[:1018] + "..."

        try:
            resp = self.session.post(
                f"{self.base_url}/sendPhoto",
                json={"chat_id": self.channel, "photo": photo_url, "caption": full_caption},
                timeout=45
            )
            resp.raise_for_status()
            result = resp.json()
            if result.get("ok"):
                logger.info(f"✅ Photo message sent")
                return True
            logger.warning(f"⚠️ Photo send failed: {result}. Falling back to text.")
            return self.send_text(caption, link)
        except Exception as e:
            logger.error(f"❌ Telegram photo send error: {e}")
            return self.send_text(caption, link)

    def send(self, text: str, link: str, image_url: Optional[str] = None) -> bool:
        if image_url and Config.ENABLE_IMAGES:
            return self.send_photo(image_url, text, link)
        return self.send_text(text, link)

# ═══════════════════════════════════════════════════════════════
# Main Bot
# ═══════════════════════════════════════════════════════════════
class NewsBot:
    def __init__(self):
        self.db = NewsDatabase()
        self.groq_keys = GroqKeyManager(Config.GROQ_API_KEYS)
        self.ai = AIProcessor(self.groq_keys)
        self.aggregator = NewsAggregator()
        self.publisher = TelegramPublisher()
        self.image_service = ImageService()
        self.running = False

    def should_run(self) -> bool:
        last = self.db.get_last_run()
        if not last:
            return True
        minutes_since = (datetime.now() - last).total_seconds() / 60
        return minutes_since >= Config.POST_INTERVAL_MINUTES

    def validate_runtime_config(self) -> List[str]:
        errors = []
        if not Config.GROQ_API_KEYS:
            errors.append("GROQ_API_KEYS تنظیم نشده")
        if not Config.TELEGRAM_BOT_TOKEN:
            errors.append("TELEGRAM_BOT_TOKEN تنظیم نشده")
        if not Config.TELEGRAM_CHANNEL_ID:
            errors.append("TELEGRAM_CHANNEL_ID تنظیم نشده")
        return errors

    def process_article(self, article: Dict) -> Tuple[bool, str, str]:
        logger.info(f"📝 Processing: {article['title'][:70]}...")

        analysis = self.ai.analyze_news(article["title"], article["summary"])
        if not analysis:
            return False, "AI analysis failed", "none"

        text = self.ai.format_post(analysis, article["link"])

        img_prompt = None
        if Config.IMAGE_SOURCE in ("ai", "both"):
            img_prompt = self.ai.generate_image_prompt(article["title"], article["summary"])

        image_url, img_source = self.image_service.get_image_for_article(
            article["link"], article["title"], img_prompt
        )

        success = self.publisher.send(text, article["link"], image_url)

        if success:
            self.db.mark_as_sent(
                article["link"], article["title"], article["source"],
                image_url or "", img_source,
                analysis.get("impact_level", ""), analysis.get("time_to_impact", "")
            )
            return True, "", img_source
        else:
            return False, "Telegram send failed", img_source

    def run_cycle(self, date_filter: Optional[Tuple[datetime, datetime]] = None,
                  backfill_mode: bool = False,
                  ignore_interval: bool = False) -> Dict:
        if self.running:
            return {"status": "already_running"}

        start_time = datetime.now()
        results = {"sent": 0, "errors": [], "duration_sec": 0,
                   "images": {"og": 0, "ai": 0, "none": 0},
                   "mode": "backfill" if backfill_mode else "normal"}

        config_errors = self.validate_runtime_config()
        if config_errors:
            results["errors"] = config_errors
            results["status"] = "error"
            return results

        self.running = True

        try:
            if not backfill_mode and not ignore_interval and not self.should_run():
                last = self.db.get_last_run()
                logger.info(f"⏳ Skipping. Last run: {last}. Interval: {Config.POST_INTERVAL_MINUTES}min")
                results["status"] = "skipped_interval"
                return results

            mode_str = "BACKFILL" if backfill_mode else "NORMAL"
            logger.info(f"🚀 Starting {mode_str} cycle...")

            all_news = self.aggregator.collect_all(date_filter=date_filter)
            logger.info(f"📊 Found {len(all_news)} relevant articles")

            new_news = [n for n in all_news if not self.db.is_duplicate(n["link"])]
            logger.info(f"🆕 {len(new_news)} new articles")

            to_process = new_news[:Config.MAX_NEWS_PER_RUN]

            for article in to_process:
                success, error, img_source = self.process_article(article)

                results["images"][img_source] = results["images"].get(img_source, 0) + 1

                if success:
                    results["sent"] += 1
                    if backfill_mode:
                        time.sleep(Config.BACKFILL_DELAY_SECONDS)
                    else:
                        time.sleep(3)
                else:
                    results["errors"].append(f"{error}: {article['link']}")

            if not backfill_mode:
                self.db.set_last_run(datetime.now())
                if datetime.now().day == 1:
                    self.db.cleanup_old(days=60)

            if results["errors"] and results["sent"] == 0:
                results["status"] = "error"
            elif results["errors"]:
                results["status"] = "partial_success"
            else:
                results["status"] = "success"

        except Exception as e:
            logger.exception("❌ Cycle error")
            results["errors"].append(str(e))
            results["status"] = "error"
        finally:
            self.running = False
            results["duration_sec"] = round((datetime.now() - start_time).total_seconds(), 2)
            logger.info(f"🏁 Cycle complete: {results}")

        return results

# ═══════════════════════════════════════════════════════════════
# Dashboard HTML Template
# ═══════════════════════════════════════════════════════════════
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>پالس هوش | داشبورد مدیریت</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
            color: #fff;
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        header {
            text-align: center;
            padding: 30px 0;
            border-bottom: 1px solid rgba(255,255,255,0.1);
            margin-bottom: 30px;
        }
        header h1 { font-size: 2.5rem; margin-bottom: 10px; }
        header p { color: #aaa; font-size: 1.1rem; }
        .status-bar {
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            justify-content: center;
            margin-bottom: 30px;
        }
        .status-pill {
            background: rgba(255,255,255,0.08);
            border: 1px solid rgba(255,255,255,0.15);
            border-radius: 50px;
            padding: 10px 25px;
            font-size: 0.9rem;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .status-pill .dot {
            width: 10px; height: 10px;
            border-radius: 50%;
            background: #4ade80;
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.4; }
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .card {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 16px;
            padding: 25px;
            backdrop-filter: blur(10px);
        }
        .card h3 {
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: #a78bfa;
            margin-bottom: 15px;
        }
        .card .value {
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 5px;
        }
        .card .label { color: #aaa; font-size: 0.9rem; }
        .action-section {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 16px;
            padding: 30px;
            margin-bottom: 30px;
        }
        .action-section h2 {
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .form-group {
            margin-bottom: 20px;
        }
        .form-group label {
            display: block;
            margin-bottom: 8px;
            color: #ccc;
            font-size: 0.95rem;
        }
        .form-group input {
            width: 100%;
            max-width: 300px;
            padding: 12px 16px;
            border-radius: 10px;
            border: 1px solid rgba(255,255,255,0.2);
            background: rgba(0,0,0,0.3);
            color: #fff;
            font-size: 1rem;
            font-family: inherit;
        }
        .form-group input:focus {
            outline: none;
            border-color: #a78bfa;
        }
        .btn {
            padding: 14px 32px;
            border-radius: 12px;
            border: none;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            font-family: inherit;
        }
        .btn-primary {
            background: linear-gradient(135deg, #667eea, #764ba2);
            color: #fff;
        }
        .btn-primary:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 30px rgba(102, 126, 234, 0.4);
        }
        .btn-danger {
            background: linear-gradient(135deg, #f093fb, #f5576c);
            color: #fff;
            margin-right: 10px;
        }
        .btn-danger:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 30px rgba(245, 87, 108, 0.4);
        }
        .btn-success {
            background: linear-gradient(135deg, #4ade80, #22c55e);
            color: #000;
            margin-right: 10px;
        }
        .btn-success:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 30px rgba(74, 222, 128, 0.4);
        }
        .result-box {
            margin-top: 20px;
            padding: 20px;
            border-radius: 12px;
            background: rgba(0,0,0,0.3);
            border: 1px solid rgba(255,255,255,0.1);
            display: none;
            white-space: pre-wrap;
            font-family: monospace;
            font-size: 0.85rem;
            max-height: 300px;
            overflow-y: auto;
        }
        .result-box.show { display: block; }
        .result-box.success { border-color: #4ade80; }
        .result-box.error { border-color: #f5576c; }
        .recent-news {
            margin-top: 30px;
        }
        .recent-news h2 {
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .news-item {
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 12px;
            padding: 15px 20px;
            margin-bottom: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
        }
        .news-item .title {
            font-weight: 600;
            flex: 1;
            min-width: 200px;
        }
        .news-item .meta {
            color: #888;
            font-size: 0.8rem;
        }
        .news-item .badge {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 600;
        }
        .badge-og { background: rgba(59, 130, 246, 0.2); color: #60a5fa; }
        .badge-ai { background: rgba(168, 85, 247, 0.2); color: #c084fc; }
        .badge-none { background: rgba(107, 114, 128, 0.2); color: #9ca3af; }
        .env-section {
            margin-top: 30px;
        }
        .env-section h2 {
            margin-bottom: 20px;
        }
        .env-table {
            width: 100%;
            border-collapse: collapse;
        }
        .env-table th, .env-table td {
            padding: 12px 16px;
            text-align: right;
            border-bottom: 1px solid rgba(255,255,255,0.08);
        }
        .env-table th {
            color: #a78bfa;
            font-weight: 600;
            font-size: 0.85rem;
            text-transform: uppercase;
        }
        .env-table td { color: #ccc; font-size: 0.9rem; }
        .env-table code {
            background: rgba(0,0,0,0.4);
            padding: 4px 8px;
            border-radius: 6px;
            font-family: monospace;
            font-size: 0.85rem;
        }
        .loading {
            display: inline-block;
            width: 18px; height: 18px;
            border: 2px solid rgba(255,255,255,0.3);
            border-top-color: #fff;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
            margin-right: 8px;
            vertical-align: middle;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
        .hidden { display: none; }
        @media (max-width: 600px) {
            header h1 { font-size: 1.8rem; }
            .card .value { font-size: 2rem; }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>⚡ پالس هوش</h1>
            <p>داشبورد مدیریت ربات اخبار AI و تکنولوژی</p>
        </header>

        <div class="status-bar">
            <div class="status-pill">
                <span class="dot"></span>
                <span>سرویس فعال</span>
            </div>
            <div class="status-pill">
                <span>📅</span>
                <span id="last-run">در حال بارگذاری...</span>
            </div>
            <div class="status-pill">
                <span>🔑</span>
                <span id="groq-status">در حال بارگذاری...</span>
            </div>
            <div class="status-pill">
                <span>🖼️</span>
                <span id="img-status">در حال بارگذاری...</span>
            </div>
        </div>

        <div class="grid" id="stats-grid">
            <div class="card">
                <h3>📊 کل ارسال شده</h3>
                <div class="value" id="stat-total">-</div>
                <div class="label">خبر از شروع</div>
            </div>
            <div class="card">
                <h3>📅 امروز</h3>
                <div class="value" id="stat-today">-</div>
                <div class="label">خبر ارسال شده</div>
            </div>
            <div class="card">
                <h3>🖼️ با تصویر</h3>
                <div class="value" id="stat-img">-</div>
                <div class="label">درصد تصویردار</div>
            </div>
            <div class="card">
                <h3>⏱️ فاصله ارسال</h3>
                <div class="value" id="stat-interval">-</div>
                <div class="label">دقیقه</div>
            </div>
        </div>

        <div class="action-section">
            <h2>🚀 اجرای دستی</h2>
            <div style="display:flex; gap:10px; flex-wrap:wrap;">
                <button class="btn btn-success" onclick="runNormal()">
                    <span id="btn-normal-text">▶️ اجرای عادی</span>
                </button>
                <button class="btn btn-primary" onclick="runForce()">
                    <span id="btn-force-text">⚡ اجرای اجباری</span>
                </button>
            </div>
            <div class="result-box" id="normal-result"></div>
        </div>

        <div class="action-section">
            <h2>📚 ارسال اخبار قدیمی (Backfill)</h2>
            <p style="color:#aaa; margin-bottom:20px;">
                با این قابلیت می‌توانید اخبار مربوط به یک بازه تاریخی خاص را جستجو و ارسال کنید.
                مثلاً اخبار ماه ژانویه ۲۰۲۶.
            </p>
            <div class="form-group">
                <label>📅 تاریخ شروع (YYYY-MM-DD)</label>
                <input type="text" id="backfill-start" value="2026-01-01" placeholder="2026-01-01">
            </div>
            <div class="form-group">
                <label>📅 تاریخ پایان (YYYY-MM-DD)</label>
                <input type="text" id="backfill-end" value="2026-08-15" placeholder="2026-08-15">
            </div>
            <div class="form-group">
                <label>🔐 رمز عبور Cron Secret</label>
                <input type="password" id="backfill-secret" placeholder="رمز عبور را وارد کنید...">
            </div>
            <button class="btn btn-danger" onclick="runBackfill()">
                <span id="btn-backfill-text">📚 اجرای Backfill</span>
            </button>
            <div class="result-box" id="backfill-result"></div>
        </div>

        <div class="recent-news">
            <h2>📰 آخرین اخبار ارسال شده</h2>
            <div id="recent-list">
                <p style="color:#888; text-align:center; padding:20px;">در حال بارگذاری...</p>
            </div>
        </div>

        <div class="env-section">
            <h2>⚙️ متغیرهای محیطی فعال</h2>
            <table class="env-table">
                <thead>
                    <tr>
                        <th>نام متغیر</th>
                        <th>مقدار فعلی</th>
                        <th>توضیحات</th>
                    </tr>
                </thead>
                <tbody id="env-body">
                </tbody>
            </table>
        </div>

        <footer style="text-align:center; padding:40px 0 20px; color:#666; font-size:0.85rem;">
            <p>پالس هوش v3.1 | ساخته شده با ❤️ برای جامعه فارسی‌زبان تکنولوژی</p>
        </footer>
    </div>

    <script>
        async function fetchStats() {
            try {
                const res = await fetch('/health');
                const data = await res.json();
                document.getElementById('stat-total').textContent = data.stats.total_sent;
                document.getElementById('stat-today').textContent = data.stats.today_sent;
                const imgPct = data.stats.total_sent > 0
                    ? Math.round((data.stats.with_image / data.stats.total_sent) * 100) + '%'
                    : '0%';
                document.getElementById('stat-img').textContent = imgPct;
                document.getElementById('stat-interval').textContent = data.interval || '30';
                document.getElementById('groq-status').textContent = `Groq: ${data.groq_keys_available}/${data.groq_keys_total} کلید فعال`;
                document.getElementById('img-status').textContent = data.images_enabled ? `تصویر: ${data.image_source}` : 'تصویر: غیرفعال';
            } catch(e) { console.error(e); }
        }

        async function fetchLastRun() {
            try {
                const res = await fetch('/stats');
                const data = await res.json();
                document.getElementById('last-run').textContent = 'آخرین اجرا: بررسی شد';
            } catch(e) { console.error(e); }
        }

        async function fetchRecent() {
            try {
                const res = await fetch('/recent');
                const data = await res.json();
                const container = document.getElementById('recent-list');
                if (!data.recent || data.recent.length === 0) {
                    container.innerHTML = '<p style="color:#888; text-align:center; padding:20px;">هنوز خبری ارسال نشده</p>';
                    return;
                }
                container.innerHTML = data.recent.map(n => {
                    const badgeClass = n.image_source === 'og' ? 'badge-og' : n.image_source === 'ai' ? 'badge-ai' : 'badge-none';
                    const badgeText = n.image_source === 'og' ? 'OG تصویر' : n.image_source === 'ai' ? 'AI تصویر' : 'بدون تصویر';
                    return `<div class="news-item">
                        <div class="title">${n.title}</div>
                        <div class="meta">${n.source} | ${n.sent_at}</div>
                        <span class="badge ${badgeClass}">${badgeText}</span>
                    </div>`;
                }).join('');
            } catch(e) { console.error(e); }
        }

        async function fetchEnv() {
            try {
                const res = await fetch('/env-info');
                const data = await res.json();
                const tbody = document.getElementById('env-body');
                tbody.innerHTML = data.env_vars.map(v => `
                    <tr>
                        <td><code>${v.name}</code></td>
                        <td><code>${v.value}</code></td>
                        <td>${v.desc}</td>
                    </tr>
                `).join('');
            } catch(e) { console.error(e); }
        }

        function showResult(id, text, isError) {
            const el = document.getElementById(id);
            el.textContent = text;
            el.className = 'result-box show ' + (isError ? 'error' : 'success');
        }

        async function runNormal() {
            const btn = document.getElementById('btn-normal-text');
            btn.innerHTML = '<span class="loading"></span> در حال اجرا...';
            try {
                const res = await fetch('/cron?secret=' + encodeURIComponent(document.getElementById('backfill-secret').value));
                const data = await res.json();
                showResult('normal-result', JSON.stringify(data, null, 2), data.status === 'error');
                fetchStats();
                fetchRecent();
            } catch(e) {
                showResult('normal-result', '❌ خطا: ' + e.message, true);
            }
            btn.innerHTML = '▶️ اجرای عادی';
        }

        async function runForce() {
            const btn = document.getElementById('btn-force-text');
            btn.innerHTML = '<span class="loading"></span> در حال اجرا...';
            try {
                const res = await fetch('/force-run?secret=' + encodeURIComponent(document.getElementById('backfill-secret').value));
                const data = await res.json();
                showResult('normal-result', JSON.stringify(data, null, 2), data.status === 'error');
                fetchStats();
                fetchRecent();
            } catch(e) {
                showResult('normal-result', '❌ خطا: ' + e.message, true);
            }
            btn.innerHTML = '⚡ اجرای اجباری';
        }

        async function runBackfill() {
            const secret = document.getElementById('backfill-secret').value;
            const start = document.getElementById('backfill-start').value;
            const end = document.getElementById('backfill-end').value;
            if (!secret) { alert('لطفاً رمز عبور Cron Secret را وارد کنید'); return; }

            const btn = document.getElementById('btn-backfill-text');
            btn.innerHTML = '<span class="loading"></span> در حال جستجوی اخبار...';
            try {
                const url = `/backfill?secret=${encodeURIComponent(secret)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`;
                const res = await fetch(url);
                const data = await res.json();
                showResult('backfill-result', JSON.stringify(data, null, 2), data.status === 'error');
                fetchStats();
                fetchRecent();
            } catch(e) {
                showResult('backfill-result', '❌ خطا: ' + e.message, true);
            }
            btn.innerHTML = '📚 اجرای Backfill';
        }

        fetchStats();
        fetchLastRun();
        fetchRecent();
        fetchEnv();
        setInterval(fetchStats, 30000);
        setInterval(fetchRecent, 60000);
    </script>
</body>
</html>
"""

# ═══════════════════════════════════════════════════════════════
# Flask App
# ═══════════════════════════════════════════════════════════════
app = Flask(__name__)
bot = NewsBot()

@app.route("/")
def index():
    return jsonify({
        "service": "AutoTech AI News Bot v3.1",
        "brand": "پالس هوش | PulseAI",
        "status": "running",
        "dashboard": "/dashboard",
        "features": ["groq_rotation", "free_images", "og_extraction", "ai_generation",
                       "impact_scoring", "backfill", "branded_content", "web_dashboard"],
        "time": datetime.now().isoformat(),
    })

@app.route("/dashboard")
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@app.route("/health")
def health():
    stats = bot.db.get_stats()
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "stats": stats,
        "interval": Config.POST_INTERVAL_MINUTES,
        "groq_keys_total": len(Config.GROQ_API_KEYS),
        "groq_keys_available": len(Config.GROQ_API_KEYS) - len(bot.groq_keys.failed_keys),
        "images_enabled": Config.ENABLE_IMAGES,
        "image_source": Config.IMAGE_SOURCE,
    })

@app.route("/cron", methods=["POST", "GET"])
def cron():
    provided = request.args.get("secret") or request.headers.get("X-Cron-Secret")
    if provided != Config.CRON_SECRET:
        logger.warning(f"🚫 Unauthorized cron from {request.remote_addr}")
        return jsonify({"error": "unauthorized"}), 403
    result = bot.run_cycle()
    return jsonify(result)

@app.route("/backfill", methods=["POST", "GET"])
def backfill():
    provided = request.args.get("secret") or request.headers.get("X-Cron-Secret")
    if provided != Config.CRON_SECRET:
        return jsonify({"error": "unauthorized"}), 403

    start_str = request.args.get("start", Config.BACKFILL_START_DATE)
    end_str = request.args.get("end", Config.BACKFILL_END_DATE)

    try:
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

    logger.info(f"📚 Backfill requested: {start_date.date()} to {end_date.date()}")
    result = bot.run_cycle(date_filter=(start_date, end_date), backfill_mode=True)
    return jsonify(result)

@app.route("/stats")
def stats():
    return jsonify(bot.db.get_stats())

@app.route("/recent")
def recent():
    return jsonify({"recent": bot.db.get_recent(limit=20)})

@app.route("/force-run")
def force_run():
    provided = request.args.get("secret") or request.headers.get("X-Cron-Secret")
    if provided != Config.CRON_SECRET:
        return jsonify({"error": "unauthorized"}), 403
    result = bot.run_cycle(ignore_interval=True)
    return jsonify(result)

@app.route("/env-info")
def env_info():
    """Return safe env var info for dashboard display"""
    env_vars = [
        {"name": "GROQ_API_KEYS", "value": f"{len(Config.GROQ_API_KEYS)} کلید تنظیم شده", "desc": "کلیدهای API Groq (با کاما جدا شوند)"},
        {"name": "GROQ_MODEL", "value": Config.GROQ_MODEL, "desc": "مدل Groq (پیش‌فرض: llama-3.1-70b-versatile)"},
        {"name": "TELEGRAM_BOT_TOKEN", "value": "✅ تنظیم شده" if Config.TELEGRAM_BOT_TOKEN else "❌ تنظیم نشده", "desc": "توکن ربات تلگرام از @BotFather"},
        {"name": "TELEGRAM_CHANNEL_ID", "value": Config.TELEGRAM_CHANNEL_ID or "❌ تنظیم نشده", "desc": "آیدی کانال مثل @PulseAI_ir"},
        {"name": "CRON_SECRET", "value": "✅ تنظیم شده" if Config.CRON_SECRET != "change-me-please" else "⚠️ پیش‌فرض (ناامن)", "desc": "رمز محافظت endpoint ها"},
        {"name": "RENDER_DISK_PATH", "value": Config.RENDER_DISK_PATH, "desc": "مسیر دیسک برای دیتابیس"},
        {"name": "POST_INTERVAL_MINUTES", "value": str(Config.POST_INTERVAL_MINUTES), "desc": "فاصله زمانی بین ارسال‌ها (دقیقه)"},
        {"name": "MAX_NEWS_PER_RUN", "value": str(Config.MAX_NEWS_PER_RUN), "desc": "حداکثر خبر در هر اجرا"},
        {"name": "ENABLE_IMAGES", "value": str(Config.ENABLE_IMAGES), "desc": "فعال‌سازی تصاویر (true/false)"},
        {"name": "IMAGE_SOURCE", "value": Config.IMAGE_SOURCE, "desc": "منبع تصویر: og / ai / both"},
        {"name": "BACKFILL_DELAY_SECONDS", "value": str(Config.BACKFILL_DELAY_SECONDS), "desc": "تأخیر بین ارسال در backfill (ثانیه)"},
    ]
    return jsonify({"env_vars": env_vars})

# ═══════════════════════════════════════════════════════════════
# Entry Point
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
