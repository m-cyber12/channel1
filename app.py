#!/usr/bin/env python3
"""
🔥 AutoTech AI News Bot - Production Edition v3.0 (Branded)
============================================================
With FREE Image Generation + Backfill + Branding Features
Deployed on Render.com | Groq API (Free Tier) | Multi-Key Rotation
Cron: cron-job.org (every minute) | Uptime: UptimeRobot (every 10 min)

Environment Variables:
  GROQ_API_KEYS        - Comma-separated Groq keys
  TELEGRAM_BOT_TOKEN   - Telegram bot token
  TELEGRAM_CHANNEL_ID  - e.g. @PulseAI_ir
  CRON_SECRET          - Secret token for /cron endpoint
  RENDER_DISK_PATH     - /opt/render/project/src/data
  ENABLE_IMAGES        - true/false (default: true)
  IMAGE_SOURCE         - og|ai|both (default: both)
  BACKFILL_MODE        - true/false (default: false)
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

from flask import Flask, jsonify, request

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
            available = [k for k in self.keys if k not in self.failed_keys]
            if not available:
                logger.warning("⚠️ All Groq keys failed! Resetting...")
                self.failed_keys.clear()
                available = self.keys
            available.sort(key=lambda k: self.last_used[k])
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
        """Generate branded content with impact analysis"""
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

        # Parse the response
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
        """Format the final Telegram post"""
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

            og_match = re.search(r'<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']', html, re.IGNORECASE)
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

            tw_match = re.search(r'<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']', html, re.IGNORECASE)
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
        """Parse various RSS date formats"""
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
        """Filter entries by date range"""
        filtered = []
        for entry in entries:
            pub_date = entry.get("published_parsed")
            if pub_date:
                if start <= pub_date <= end:
                    filtered.append(entry)
            else:
                # If no date, include it (fallback)
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
                json={"chat_id": self.channel, "text": full_text, "parse_mode": "HTML", "disable_web_page_preview": False},
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
                json={"chat_id": self.channel, "photo": photo_url, "caption": full_caption, "parse_mode": "HTML"},
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

    def process_article(self, article: Dict) -> Tuple[bool, str]:
        """Process a single article. Returns (success, error_msg)"""
        logger.info(f"📝 Processing: {article['title'][:70]}...")

        # AI Analysis with branding
        analysis = self.ai.analyze_news(article["title"], article["summary"])
        if not analysis:
            return False, "AI analysis failed"

        text = self.ai.format_post(analysis, article["link"])

        # Generate image prompt
        img_prompt = None
        if Config.IMAGE_SOURCE in ("ai", "both"):
            img_prompt = self.ai.generate_image_prompt(article["title"], article["summary"])

        # Get image
        image_url, img_source = self.image_service.get_image_for_article(
            article["link"], article["title"], img_prompt
        )

        # Send to Telegram
        success = self.publisher.send(text, article["link"], image_url)

        if success:
            self.db.mark_as_sent(
                article["link"], article["title"], article["source"],
                image_url or "", img_source,
                analysis.get("impact_level", ""), analysis.get("time_to_impact", "")
            )
            return True, ""
        else:
            return False, "Telegram send failed"

    def run_cycle(self, date_filter: Optional[Tuple[datetime, datetime]] = None, 
                  backfill_mode: bool = False) -> Dict:
        if self.running:
            return {"status": "already_running"}

        self.running = True
        start_time = datetime.now()
        results = {"sent": 0, "errors": [], "duration_sec": 0, 
                   "images": {"og": 0, "ai": 0, "none": 0}, "mode": "normal"}

        try:
            if not backfill_mode and not self.should_run():
                last = self.db.get_last_run()
                logger.info(f"⏳ Skipping. Last run: {last}. Interval: {Config.POST_INTERVAL_MINUTES}min")
                results["status"] = "skipped_interval"
                return results

            mode_str = "BACKFILL" if backfill_mode else "NORMAL"
            logger.info(f"🚀 Starting {mode_str} cycle...")
            results["mode"] = mode_str.lower()

            all_news = self.aggregator.collect_all(date_filter=date_filter)
            logger.info(f"📊 Found {len(all_news)} relevant articles")

            new_news = [n for n in all_news if not self.db.is_duplicate(n["link"])]
            logger.info(f"🆕 {len(new_news)} new articles")

            to_process = new_news[:Config.MAX_NEWS_PER_RUN]

            for article in to_process:
                success, error = self.process_article(article)

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
# Flask App
# ═══════════════════════════════════════════════════════════════
app = Flask(__name__)
bot = NewsBot()

@app.route("/")
def index():
    return jsonify({
        "service": "AutoTech AI News Bot v3.0",
        "brand": "پالس هوش | PulseAI",
        "status": "running",
        "features": ["groq_rotation", "free_images", "og_extraction", "ai_generation", 
                     "impact_scoring", "backfill", "branded_content"],
        "time": datetime.now().isoformat(),
    })

@app.route("/health")
def health():
    stats = bot.db.get_stats()
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "stats": stats,
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
    """Backfill endpoint - send historical news from date range"""
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

@app.route("/force-run")
def force_run():
    provided = request.args.get("secret")
    if provided != Config.CRON_SECRET:
        return jsonify({"error": "unauthorized"}), 403
    result = bot.run_cycle()
    return jsonify(result)

# ═══════════════════════════════════════════════════════════════
# Entry Point
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
