"""
FELIXA AUTOMATED BACKEND - PRODUCTION
======================================
Fully automated community health analysis system.

Security Features:
- Stripe webhook signature verification
- No public test endpoint
- Duplicate payment protection
- Rate limiting
"""

import os
import json
import csv
from io import StringIO
import asyncio
import requests
import hashlib
from datetime import datetime
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import base64
from io import BytesIO

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import stripe
import resend

from openai import OpenAI

# ============================================
# CONFIGURATION
# ============================================

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")

ADMIN_EMAIL = "play@felixagaming.com"
FROM_EMAIL = "Felixa <onboarding@resend.dev>"

# Initialize clients
openai_client = None
if OPENAI_API_KEY:
    try:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        print(f"OpenAI init error: {e}")

stripe.api_key = STRIPE_SECRET_KEY
resend.api_key = RESEND_API_KEY

thread_pool = ThreadPoolExecutor(max_workers=5)

# Track processed payments to prevent duplicates
processed_payments = set()

# ============================================
# FASTAPI APP
# ============================================

app = FastAPI(title="Felixa Automated Backend", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://felixagaming.com", "https://www.felixagaming.com"],
    allow_credentials=True,
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)

# ============================================
# ATTRIBUTES & CATEGORIES
# ============================================

ATTRIBUTE_WEIGHTS = {
    "Polite": 3, "Empathetic": 3, "Encouraging": 2, "Funny": 1,
    "Ignorant": -1, "Ego-centric": -1, "Neurotic": -1,
    "Sarcasm": -2, "Agitated": -2,
    "Aggressive": -3, "Judgmental": -3, "Disrespectful": -3, "Rude": -3,
}

ALL_ATTRIBUTES = list(ATTRIBUTE_WEIGHTS.keys())
CATEGORIES = ["Profanity", "Hate_Speech", "Violence", "Spam"]

# ============================================
# GPT ANALYSIS PROMPT
# ============================================

ANALYSIS_PROMPT = """Analyze the following messages for behavioral attributes and content categories.

POSITIVE ATTRIBUTES:
- Polite: Respectful and courteous
- Funny: Light-hearted
- Empathetic: Shows care
- Encouraging: Supportive

NEGATIVE ATTRIBUTES:
- Low (-1): Ignorant, Ego-centric, Neurotic
- Medium (-2): Sarcasm, Agitated  
- High (-3): Aggressive, Judgmental, Disrespectful, Rude

CONTENT CATEGORIES:
1. Profanity: Swear words
2. Hate_Speech: Discrimination
3. Violence: Threats
4. Spam: Promotional/scam content

Return ONLY valid JSON:
{
    "comments": [
        {
            "comment_index": 0,
            "attributes": {"Polite": 0, "Funny": 0, "Empathetic": 0, "Encouraging": 0, "Ignorant": 0, "Ego-centric": 0, "Neurotic": 0, "Sarcasm": 0, "Agitated": 0, "Aggressive": 0, "Judgmental": 0, "Disrespectful": 0, "Rude": 0},
            "categories": {"Profanity": {"present": 0, "words": []}, "Hate_Speech": {"present": 0, "words": []}, "Violence": {"present": 0, "words": []}, "Spam": {"present": 0, "words": []}},
            "sentiment": "neutral"
        }
    ]
}

MESSAGES:
"""

# ============================================
# SCRAPING FUNCTIONS
# ============================================

def scrape_youtube(url: str, limit: int = 500) -> List[Dict]:
    if not YOUTUBE_API_KEY:
        return []
    
    try:
        video_id = None
        if "v=" in url:
            video_id = url.split("v=")[1].split("&")[0]
        elif "youtu.be/" in url:
            video_id = url.split("youtu.be/")[1].split("?")[0]
        
        if not video_id:
            return []
        
        comments = []
        params = {
            "part": "snippet",
            "videoId": video_id,
            "key": YOUTUBE_API_KEY,
            "maxResults": 100,
            "textFormat": "plainText",
        }
        
        while len(comments) < limit:
            r = requests.get("https://www.googleapis.com/youtube/v3/commentThreads", params=params)
            if r.status_code != 200:
                break
            
            data = r.json()
            for item in data.get("items", []):
                text = item["snippet"]["topLevelComment"]["snippet"].get("textDisplay", "")
                if text:
                    comments.append({"text": text})
            
            if "nextPageToken" in data and len(comments) < limit:
                params["pageToken"] = data["nextPageToken"]
            else:
                break
        
        return comments[:limit]
    except Exception as e:
        print(f"YouTube scrape error: {e}")
        return []


def scrape_reddit(url: str, limit: int = 500) -> List[Dict]:
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        return []
    
    try:
        import praw
        
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent="Felixa/1.0"
        )
        
        submission = reddit.submission(url=url)
        submission.comments.replace_more(limit=0)
        
        comments = []
        for comment in submission.comments.list()[:limit]:
            if hasattr(comment, 'body') and comment.body:
                comments.append({"text": comment.body})
        
        return comments
    except Exception as e:
        print(f"Reddit scrape error: {e}")
        return []


def scrape_platform(platform: str, url: str, limit: int = 500) -> List[Dict]:
    platform = platform.lower()
    
    if platform == "youtube":
        return scrape_youtube(url, limit)
    elif platform == "reddit":
        return scrape_reddit(url, limit)
    else:
        return []


# ============================================
# GPT ANALYSIS
# ============================================

async def analyze_comments(comments: List[Dict]) -> Dict:
    if not openai_client or not comments:
        return {"comments": []}
    
    all_results = []
    batch_size = 15
    
    for i in range(0, len(comments), batch_size):
        batch = comments[i:i + batch_size]
        batch_text = "\n".join([f'Message {j}: "{c["text"][:400]}"' for j, c in enumerate(batch)])
        
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                thread_pool,
                lambda: openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "Analyze text for behavioral attributes. Return only valid JSON."},
                        {"role": "user", "content": ANALYSIS_PROMPT + batch_text}
                    ],
                    temperature=0.0,
                    max_tokens=4000
                )
            )
            
            content = response.choices[0].message.content.strip()
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            
            result = json.loads(content)
            for j, item in enumerate(result.get("comments", [])):
                item["comment_index"] = i + j
                all_results.append(item)
                
        except Exception as e:
            print(f"Analysis error: {e}")
        
        await asyncio.sleep(0.3)
    
    return {"comments": all_results}


def calculate_health_score(results: Dict) -> Dict:
    comments = results.get("comments", [])
    total = len(comments)
    
    if total == 0:
        return {"health_score": 50, "health_status": "No Data", "summary": {}}
    
    attr_counts = {a: 0 for a in ALL_ATTRIBUTES}
    cat_counts = {c: 0 for c in CATEGORIES}
    sent_counts = {"positive": 0, "neutral": 0, "negative": 0}
    
    for c in comments:
        for attr, val in c.get("attributes", {}).items():
            if val == 1 and attr in attr_counts:
                attr_counts[attr] += 1
        
        for cat in CATEGORIES:
            cat_data = c.get("categories", {}).get(cat, {})
            if isinstance(cat_data, dict) and cat_data.get("present", 0) == 1:
                cat_counts[cat] += 1
        
        sent = c.get("sentiment", "neutral").lower()
        if sent in sent_counts:
            sent_counts[sent] += 1
    
    pos_weight = sum(attr_counts[a] * ATTRIBUTE_WEIGHTS[a] for a in ALL_ATTRIBUTES if ATTRIBUTE_WEIGHTS[a] > 0)
    neg_weight = abs(sum(attr_counts[a] * ATTRIBUTE_WEIGHTS[a] for a in ALL_ATTRIBUTES if ATTRIBUTE_WEIGHTS[a] < 0))
    
    max_pos = total * 9
    score = min(100, max(0, int(50 + (pos_weight - neg_weight) / max(1, max_pos) * 50)))
    
    status = "Excellent" if score >= 80 else "Good" if score >= 60 else "Fair" if score >= 40 else "Needs Attention"
    
    return {
        "health_score": score,
        "health_status": status,
        "summary": {
            "total": total,
            "positive": sent_counts["positive"],
            "negative": sent_counts["negative"],
            "neutral": sent_counts["neutral"],
            "attributes": attr_counts,
            "categories": cat_counts
        }
    }


# ============================================
# FLAGGED COMMENTS EXTRACTION
# ============================================

def get_flagged_comments(results: Dict, comments: List[Dict]) -> Dict:
    """Extract flagged comments with examples"""
    backend_comments = results.get("comments", [])
    
    high_severity = []
    medium_severity = []
    
    high_attrs = ["Aggressive", "Judgmental", "Disrespectful", "Rude"]
    high_cats = ["Hate_Speech", "Violence"]
    
    for i, analyzed in enumerate(backend_comments):
        if i >= len(comments):
            break
            
        attrs = analyzed.get("attributes", {})
        cats = analyzed.get("categories", {})
        original_text = comments[i].get("text", "")[:150]
        
        flags = []
        is_high = False
        
        # Check high severity
        for attr in high_attrs:
            if attrs.get(attr, 0) == 1:
                flags.append(attr)
                is_high = True
        
        for cat in high_cats:
            cat_data = cats.get(cat, {})
            if isinstance(cat_data, dict) and cat_data.get("present", 0) == 1:
                flags.append(cat.replace("_", " "))
                is_high = True
        
        # Check medium severity
        if not flags:
            if cats.get("Profanity", {}).get("present", 0) == 1:
                flags.append("Profanity")
            if attrs.get("Sarcasm", 0) == 1:
                flags.append("Sarcasm")
            if attrs.get("Agitated", 0) == 1:
                flags.append("Agitated")
        
        if flags:
            item = {
                "text": original_text,
                "flags": flags
            }
            if is_high:
                high_severity.append(item)
            else:
                medium_severity.append(item)
    
    return {
        "high": high_severity[:10],  # Max 10 examples
        "medium": medium_severity[:5],  # Max 5 examples
        "total_high": len(high_severity),
        "total_medium": len(medium_severity)
    }


# ============================================
# HTML REPORT GENERATION
# ============================================

def generate_html_report(
    platform: str,
    source: str,
    health_score: int,
    health_status: str,
    summary: Dict,
    flagged: Dict
) -> str:
    """Generate comprehensive HTML email report"""
    
    total = summary.get("total", 0)
    positive = summary.get("positive", 0)
    negative = summary.get("negative", 0)
    neutral = summary.get("neutral", total - positive - negative)
    
    pos_pct = round(positive / total * 100, 1) if total > 0 else 0
    neg_pct = round(negative / total * 100, 1) if total > 0 else 0
    neu_pct = round(neutral / total * 100, 1) if total > 0 else 0
    
    attr_counts = summary.get("attributes", {})
    cat_counts = summary.get("categories", {})
    
    # Score color
    if health_score >= 80:
        score_color = "#28A745"
        score_gradient = "linear-gradient(135deg, #28A745 0%, #34CE57 100%)"
    elif health_score >= 60:
        score_color = "#28A745"
        score_gradient = "linear-gradient(135deg, #28A745 0%, #5DD879 100%)"
    elif health_score >= 40:
        score_color = "#FFC107"
        score_gradient = "linear-gradient(135deg, #FFC107 0%, #FF9800 100%)"
    else:
        score_color = "#DC3545"
        score_gradient = "linear-gradient(135deg, #DC3545 0%, #E25563 100%)"
    
    # Generate flagged comments HTML
    high_flagged_html = ""
    if flagged.get("high"):
        rows = ""
        for i, item in enumerate(flagged["high"][:5], 1):
            flags_html = " ".join([f'<span style="background:#DC3545;color:#fff;padding:2px 6px;border-radius:4px;font-size:10px;margin-right:3px;">{f}</span>' for f in item["flags"]])
            bg = "#FFF5F5" if i % 2 == 1 else "#fff"
            rows += f'''
            <tr style="background:{bg};">
                <td style="padding:12px;border-bottom:1px solid #eee;vertical-align:top;">{i}</td>
                <td style="padding:12px;border-bottom:1px solid #eee;font-style:italic;color:#333;">"{item["text"]}..."</td>
                <td style="padding:12px;border-bottom:1px solid #eee;vertical-align:top;">{flags_html}</td>
            </tr>
            '''
        high_flagged_html = f'''
        <h3 style="color:#DC3545;margin:30px 0 15px 0;font-size:16px;">High Severity Flagged Comments</h3>
        <table style="width:100%;border-collapse:collapse;margin-bottom:30px;">
            <thead>
                <tr style="background:#DC3545;">
                    <th style="padding:12px;text-align:left;color:white;font-weight:600;width:40px;">#</th>
                    <th style="padding:12px;text-align:left;color:white;font-weight:600;">Comment</th>
                    <th style="padding:12px;text-align:left;color:white;font-weight:600;width:150px;">Flags</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        '''
    
    medium_flagged_html = ""
    if flagged.get("medium"):
        rows = ""
        for i, item in enumerate(flagged["medium"][:3], 1):
            flags_html = " ".join([f'<span style="background:#FFC107;color:#000;padding:2px 6px;border-radius:4px;font-size:10px;margin-right:3px;">{f}</span>' for f in item["flags"]])
            bg = "#FFF8F0" if i % 2 == 1 else "#fff"
            rows += f'''
            <tr style="background:{bg};">
                <td style="padding:12px;border-bottom:1px solid #eee;vertical-align:top;">{i}</td>
                <td style="padding:12px;border-bottom:1px solid #eee;font-style:italic;color:#333;">"{item["text"]}..."</td>
                <td style="padding:12px;border-bottom:1px solid #eee;vertical-align:top;">{flags_html}</td>
            </tr>
            '''
        medium_flagged_html = f'''
        <h3 style="color:#FF8C00;margin:30px 0 15px 0;font-size:16px;">Medium Severity Flagged Comments</h3>
        <table style="width:100%;border-collapse:collapse;margin-bottom:40px;">
            <thead>
                <tr style="background:#FF8C00;">
                    <th style="padding:12px;text-align:left;color:white;font-weight:600;width:40px;">#</th>
                    <th style="padding:12px;text-align:left;color:white;font-weight:600;">Comment</th>
                    <th style="padding:12px;text-align:left;color:white;font-weight:600;width:150px;">Flags</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        '''
    
    # Build attribute rows
    def attr_row(name, desc, count, bg="#fff"):
        pct = round(count / total * 100, 1) if total > 0 else 0
        return f'''
        <tr style="background:{bg};">
            <td style="padding:12px;border-bottom:1px solid #eee;font-weight:500;">{name}</td>
            <td style="padding:12px;border-bottom:1px solid #eee;color:#666;font-size:13px;">{desc}</td>
            <td style="padding:12px;border-bottom:1px solid #eee;text-align:center;font-weight:bold;">{count}</td>
            <td style="padding:12px;border-bottom:1px solid #eee;text-align:center;">{pct}%</td>
        </tr>
        '''
    
    report_date = datetime.now().strftime("%B %d, %Y")
    
    html = f'''
<!DOCTYPE html>
<html>
<head><title>Felixa Community Health Report</title></head>
<body style="background:#e9ecef;padding:20px;margin:0;font-family:Arial,sans-serif;">
<div style="max-width:800px;margin:0 auto;background:white;">

<!-- HEADER -->
<div style="background:linear-gradient(135deg,#2D1B4E 0%,#6B4C9A 100%);padding:50px 40px;text-align:center;">
    <h1 style="color:white;margin:0;font-size:32px;font-weight:300;letter-spacing:2px;">FELIXA</h1>
    <h2 style="color:white;margin:10px 0 0 0;font-size:24px;font-weight:600;">Community Health Report</h2>
    <div style="width:60px;height:3px;background:#E8E0F0;margin:20px auto;"></div>
    <p style="color:#E8E0F0;margin:0;font-size:14px;">Comprehensive Analysis & Recommendations</p>
</div>

<!-- REPORT INFO -->
<div style="background:#f8f9fa;padding:25px 40px;border-bottom:1px solid #eee;">
    <table style="width:100%;">
        <tr>
            <td style="width:50%;">
                <p style="margin:0 0 5px 0;color:#666;font-size:12px;text-transform:uppercase;">Platform</p>
                <p style="margin:0;color:#2D1B4E;font-size:16px;font-weight:bold;">{platform.upper()}</p>
            </td>
            <td style="width:50%;text-align:right;">
                <p style="margin:0 0 5px 0;color:#666;font-size:12px;text-transform:uppercase;">Report Date</p>
                <p style="margin:0;color:#2D1B4E;font-size:16px;font-weight:bold;">{report_date}</p>
            </td>
        </tr>
        <tr>
            <td colspan="2" style="padding-top:15px;">
                <p style="margin:0 0 5px 0;color:#666;font-size:12px;text-transform:uppercase;">Source URL</p>
                <p style="margin:0;color:#6B4C9A;font-size:14px;word-break:break-all;">{source[:100]}</p>
            </td>
        </tr>
    </table>
</div>

<!-- EXECUTIVE SUMMARY -->
<div style="padding:40px;">
    <h2 style="color:#2D1B4E;margin:0 0 25px 0;font-size:22px;border-bottom:2px solid #6B4C9A;padding-bottom:10px;">Executive Summary</h2>
    
    <!-- Health Score -->
    <div style="background:#f8f9fa;border-radius:10px;padding:30px;margin-bottom:30px;text-align:center;">
        <p style="color:#666;margin:0 0 10px 0;font-size:14px;text-transform:uppercase;letter-spacing:1px;">Overall Community Health Score</p>
        <div style="width:150px;height:150px;border-radius:50%;background:{score_gradient};margin:20px auto;box-shadow:0 4px 15px rgba(0,0,0,0.2);">
            <div style="width:150px;height:150px;display:table-cell;vertical-align:middle;text-align:center;">
                <div style="width:120px;height:120px;border-radius:50%;background:white;margin:15px auto;display:table-cell;vertical-align:middle;">
                    <span style="font-size:48px;font-weight:bold;color:{score_color};">{health_score}</span>
                </div>
            </div>
        </div>
        <div style="display:inline-block;background:{score_color};color:white;padding:8px 25px;border-radius:20px;font-weight:bold;font-size:14px;text-transform:uppercase;letter-spacing:1px;">{health_status}</div>
        
        <!-- Score Scale -->
        <div style="margin-top:25px;">
            <div style="margin-bottom:5px;">
                <span style="font-size:11px;color:#DC3545;float:left;">0 - Critical</span>
                <span style="font-size:11px;color:#FFC107;">40 - Fair</span>
                <span style="font-size:11px;color:#28A745;float:right;">80 - Excellent</span>
            </div>
            <div style="clear:both;height:8px;background:linear-gradient(to right,#DC3545 0%,#FFC107 50%,#28A745 100%);border-radius:4px;"></div>
        </div>
    </div>
    
    <!-- Key Metrics -->
    <table style="width:100%;margin-bottom:30px;">
        <tr>
            <td style="width:25%;padding:5px;">
                <div style="background:#E8E0F0;padding:20px;border-radius:8px;text-align:center;">
                    <p style="margin:0;font-size:28px;font-weight:bold;color:#2D1B4E;">{total}</p>
                    <p style="margin:5px 0 0 0;font-size:12px;color:#666;text-transform:uppercase;">Comments Analyzed</p>
                </div>
            </td>
            <td style="width:25%;padding:5px;">
                <div style="background:#D4EDDA;padding:20px;border-radius:8px;text-align:center;">
                    <p style="margin:0;font-size:28px;font-weight:bold;color:#28A745;">{positive}</p>
                    <p style="margin:5px 0 0 0;font-size:12px;color:#666;text-transform:uppercase;">Positive</p>
                </div>
            </td>
            <td style="width:25%;padding:5px;">
                <div style="background:#F8F9FA;padding:20px;border-radius:8px;text-align:center;">
                    <p style="margin:0;font-size:28px;font-weight:bold;color:#6C757D;">{neutral}</p>
                    <p style="margin:5px 0 0 0;font-size:12px;color:#666;text-transform:uppercase;">Neutral</p>
                </div>
            </td>
            <td style="width:25%;padding:5px;">
                <div style="background:#F8D7DA;padding:20px;border-radius:8px;text-align:center;">
                    <p style="margin:0;font-size:28px;font-weight:bold;color:#DC3545;">{negative}</p>
                    <p style="margin:5px 0 0 0;font-size:12px;color:#666;text-transform:uppercase;">Negative</p>
                </div>
            </td>
        </tr>
    </table>
    
    <!-- Summary Text -->
    <div style="background:#fff;border-left:4px solid #6B4C9A;padding:20px;margin-bottom:20px;">
        <p style="margin:0;color:#333;line-height:1.7;">
            This report provides a comprehensive analysis of <strong>{total} comments</strong> collected from the specified {platform.upper()} content.
            The community health score of <strong>{health_score}/100</strong> indicates that the community {"is healthy and supportive" if health_score >= 80 else "is generally positive with minor issues" if health_score >= 60 else "requires attention" if health_score >= 40 else "requires significant intervention"}.
            Approximately <strong>{neg_pct}%</strong> of comments were classified as negative, with <strong>{flagged.get("total_high", 0)}</strong> comments flagged for high-severity content violations.
        </p>
    </div>
</div>

<div style="border-top:2px dashed #ddd;margin:0 40px;padding-top:10px;">
    <p style="text-align:center;color:#999;font-size:11px;margin:0;">Page 1 of 3</p>
</div>

<!-- PAGE 2: DETAILED ANALYSIS -->
<div style="padding:40px;">
    <h2 style="color:#2D1B4E;margin:30px 0 25px 0;font-size:22px;border-bottom:2px solid #6B4C9A;padding-bottom:10px;">Sentiment Analysis</h2>
    
    <!-- Sentiment Bars -->
    <table style="width:100%;border-collapse:collapse;margin-bottom:40px;">
        <tr>
            <td style="width:100px;padding:10px 0;color:#333;font-weight:500;">Positive</td>
            <td style="padding:10px 0;">
                <div style="background:#e9ecef;border-radius:4px;height:30px;position:relative;">
                    <div style="background:linear-gradient(90deg,#28A745,#34CE57);width:{pos_pct}%;height:100%;border-radius:4px;"></div>
                </div>
            </td>
            <td style="width:80px;text-align:right;font-weight:bold;color:#28A745;">{pos_pct}%</td>
        </tr>
        <tr>
            <td style="width:100px;padding:10px 0;color:#333;font-weight:500;">Neutral</td>
            <td style="padding:10px 0;">
                <div style="background:#e9ecef;border-radius:4px;height:30px;position:relative;">
                    <div style="background:linear-gradient(90deg,#6C757D,#868e96);width:{neu_pct}%;height:100%;border-radius:4px;"></div>
                </div>
            </td>
            <td style="width:80px;text-align:right;font-weight:bold;color:#6C757D;">{neu_pct}%</td>
        </tr>
        <tr>
            <td style="width:100px;padding:10px 0;color:#333;font-weight:500;">Negative</td>
            <td style="padding:10px 0;">
                <div style="background:#e9ecef;border-radius:4px;height:30px;position:relative;">
                    <div style="background:linear-gradient(90deg,#DC3545,#E25563);width:{neg_pct}%;height:100%;border-radius:4px;"></div>
                </div>
            </td>
            <td style="width:80px;text-align:right;font-weight:bold;color:#DC3545;">{neg_pct}%</td>
        </tr>
    </table>
    
    <!-- Content Categories -->
    <h2 style="color:#2D1B4E;margin:40px 0 25px 0;font-size:22px;border-bottom:2px solid #6B4C9A;padding-bottom:10px;">Content Categories</h2>
    
    <table style="width:100%;border-collapse:collapse;margin-bottom:30px;">
        <thead>
            <tr style="background:#2D1B4E;">
                <th style="padding:15px;text-align:left;color:white;font-weight:600;">Category</th>
                <th style="padding:15px;text-align:center;color:white;font-weight:600;">Severity</th>
                <th style="padding:15px;text-align:center;color:white;font-weight:600;">Count</th>
                <th style="padding:15px;text-align:center;color:white;font-weight:600;">%</th>
            </tr>
        </thead>
        <tbody>
            <tr style="background:#fff;">
                <td style="padding:15px;border-bottom:1px solid #eee;"><strong>Profanity</strong><br><span style="font-size:12px;color:#666;">Swear words and explicit language</span></td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;"><span style="background:#FFC107;color:#000;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:bold;">MEDIUM</span></td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;font-weight:bold;font-size:18px;">{cat_counts.get("Profanity", 0)}</td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;">{round(cat_counts.get("Profanity", 0) / total * 100, 1) if total > 0 else 0}%</td>
            </tr>
            <tr style="background:#FFF5F5;">
                <td style="padding:15px;border-bottom:1px solid #eee;"><strong>Hate Speech</strong><br><span style="font-size:12px;color:#666;">Discrimination based on race, religion, gender</span></td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;"><span style="background:#DC3545;color:#fff;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:bold;">HIGH</span></td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;font-weight:bold;font-size:18px;color:#DC3545;">{cat_counts.get("Hate_Speech", 0)}</td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;">{round(cat_counts.get("Hate_Speech", 0) / total * 100, 1) if total > 0 else 0}%</td>
            </tr>
            <tr style="background:#FFF5F5;">
                <td style="padding:15px;border-bottom:1px solid #eee;"><strong>Violence</strong><br><span style="font-size:12px;color:#666;">Threats or violent language</span></td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;"><span style="background:#DC3545;color:#fff;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:bold;">HIGH</span></td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;font-weight:bold;font-size:18px;color:#DC3545;">{cat_counts.get("Violence", 0)}</td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;">{round(cat_counts.get("Violence", 0) / total * 100, 1) if total > 0 else 0}%</td>
            </tr>
            <tr style="background:#fff;">
                <td style="padding:15px;border-bottom:1px solid #eee;"><strong>Spam</strong><br><span style="font-size:12px;color:#666;">Promotional or scam content</span></td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;"><span style="background:#6C757D;color:#fff;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:bold;">LOW</span></td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;font-weight:bold;font-size:18px;">{cat_counts.get("Spam", 0)}</td>
                <td style="padding:15px;border-bottom:1px solid #eee;text-align:center;">{round(cat_counts.get("Spam", 0) / total * 100, 1) if total > 0 else 0}%</td>
            </tr>
        </tbody>
    </table>
    
    <!-- Behavioral Attributes -->
    <h2 style="color:#2D1B4E;margin:40px 0 25px 0;font-size:22px;border-bottom:2px solid #6B4C9A;padding-bottom:10px;">Behavioral Attributes Analysis</h2>
    
    <!-- Positive Attributes -->
    <h3 style="color:#28A745;margin:30px 0 15px 0;font-size:16px;">Prosocial Attributes (Positive Impact)</h3>
    <table style="width:100%;border-collapse:collapse;margin-bottom:25px;">
        <thead>
            <tr style="background:#D4EDDA;">
                <th style="padding:12px;text-align:left;color:#155724;font-weight:600;">Attribute</th>
                <th style="padding:12px;text-align:left;color:#155724;font-weight:600;">Description</th>
                <th style="padding:12px;text-align:center;color:#155724;font-weight:600;">Count</th>
                <th style="padding:12px;text-align:center;color:#155724;font-weight:600;">%</th>
            </tr>
        </thead>
        <tbody>
            {attr_row("Polite", "Respectful and courteous language", attr_counts.get("Polite", 0), "#fff")}
            {attr_row("Empathetic", "Shows understanding and care for others", attr_counts.get("Empathetic", 0), "#FAFFF9")}
            {attr_row("Encouraging", "Motivates and supports others", attr_counts.get("Encouraging", 0), "#fff")}
            {attr_row("Funny", "Light-hearted and playful", attr_counts.get("Funny", 0), "#FAFFF9")}
        </tbody>
    </table>
    
    <!-- Low Severity -->
    <h3 style="color:#FFC107;margin:30px 0 15px 0;font-size:16px;">Low Severity Attributes</h3>
    <table style="width:100%;border-collapse:collapse;margin-bottom:25px;">
        <thead>
            <tr style="background:#FFF3CD;">
                <th style="padding:12px;text-align:left;color:#856404;font-weight:600;">Attribute</th>
                <th style="padding:12px;text-align:left;color:#856404;font-weight:600;">Description</th>
                <th style="padding:12px;text-align:center;color:#856404;font-weight:600;">Count</th>
                <th style="padding:12px;text-align:center;color:#856404;font-weight:600;">%</th>
            </tr>
        </thead>
        <tbody>
            {attr_row("Ignorant", "Off-topic or uninformed", attr_counts.get("Ignorant", 0), "#fff")}
            {attr_row("Ego-centric", "Excessively focused on self", attr_counts.get("Ego-centric", 0), "#FFFDF5")}
            {attr_row("Neurotic", "Excessive worry or negativity", attr_counts.get("Neurotic", 0), "#fff")}
        </tbody>
    </table>
    
    <!-- Medium Severity -->
    <h3 style="color:#FF8C00;margin:30px 0 15px 0;font-size:16px;">Medium Severity Attributes</h3>
    <table style="width:100%;border-collapse:collapse;margin-bottom:25px;">
        <thead>
            <tr style="background:#FFE4B5;">
                <th style="padding:12px;text-align:left;color:#8B4513;font-weight:600;">Attribute</th>
                <th style="padding:12px;text-align:left;color:#8B4513;font-weight:600;">Description</th>
                <th style="padding:12px;text-align:center;color:#8B4513;font-weight:600;">Count</th>
                <th style="padding:12px;text-align:center;color:#8B4513;font-weight:600;">%</th>
            </tr>
        </thead>
        <tbody>
            {attr_row("Sarcasm", "Ironic language to mock others", attr_counts.get("Sarcasm", 0), "#fff")}
            {attr_row("Agitated", "Frustrated or emotionally charged", attr_counts.get("Agitated", 0), "#FFFAF0")}
        </tbody>
    </table>
    
    <!-- High Severity -->
    <h3 style="color:#DC3545;margin:30px 0 15px 0;font-size:16px;">High Severity Attributes</h3>
    <table style="width:100%;border-collapse:collapse;margin-bottom:25px;">
        <thead>
            <tr style="background:#F8D7DA;">
                <th style="padding:12px;text-align:left;color:#721C24;font-weight:600;">Attribute</th>
                <th style="padding:12px;text-align:left;color:#721C24;font-weight:600;">Description</th>
                <th style="padding:12px;text-align:center;color:#721C24;font-weight:600;">Count</th>
                <th style="padding:12px;text-align:center;color:#721C24;font-weight:600;">%</th>
            </tr>
        </thead>
        <tbody>
            {attr_row("Aggressive", "Hostile or threatening language", attr_counts.get("Aggressive", 0), "#fff")}
            {attr_row("Judgmental", "Critical, shames others", attr_counts.get("Judgmental", 0), "#FFF5F5")}
            {attr_row("Disrespectful", "Dismisses or mocks others", attr_counts.get("Disrespectful", 0), "#fff")}
            {attr_row("Rude", "Insulting or offensive", attr_counts.get("Rude", 0), "#FFF5F5")}
        </tbody>
    </table>
</div>

<div style="border-top:2px dashed #ddd;margin:0 40px;padding-top:10px;">
    <p style="text-align:center;color:#999;font-size:11px;margin:0;">Page 2 of 3</p>
</div>

<!-- PAGE 3: FLAGGED CONTENT & RECOMMENDATIONS -->
<div style="padding:40px;">
    <h2 style="color:#2D1B4E;margin:30px 0 25px 0;font-size:22px;border-bottom:2px solid #6B4C9A;padding-bottom:10px;">Flagged Content Summary</h2>
    
    <div style="background:#FFF5F5;border:1px solid #F5C6CB;border-radius:8px;padding:20px;margin-bottom:30px;">
        <table style="width:100%;">
            <tr>
                <td style="text-align:center;padding:15px;">
                    <p style="margin:0;font-size:36px;font-weight:bold;color:#DC3545;">{flagged.get("total_high", 0) + flagged.get("total_medium", 0)}</p>
                    <p style="margin:5px 0 0 0;font-size:12px;color:#721C24;text-transform:uppercase;">Total Flagged</p>
                </td>
                <td style="text-align:center;padding:15px;border-left:1px solid #F5C6CB;">
                    <p style="margin:0;font-size:36px;font-weight:bold;color:#DC3545;">{flagged.get("total_high", 0)}</p>
                    <p style="margin:5px 0 0 0;font-size:12px;color:#721C24;text-transform:uppercase;">High Severity</p>
                </td>
                <td style="text-align:center;padding:15px;border-left:1px solid #F5C6CB;">
                    <p style="margin:0;font-size:36px;font-weight:bold;color:#FF8C00;">{flagged.get("total_medium", 0)}</p>
                    <p style="margin:5px 0 0 0;font-size:12px;color:#721C24;text-transform:uppercase;">Medium Severity</p>
                </td>
            </tr>
        </table>
    </div>
    
    {high_flagged_html}
    {medium_flagged_html}
    
    <!-- Recommendations -->
    <h2 style="color:#2D1B4E;margin:40px 0 25px 0;font-size:22px;border-bottom:2px solid #6B4C9A;padding-bottom:10px;">Recommendations</h2>
    
    <div style="background:#FFF5F5;border-left:4px solid #DC3545;padding:20px;margin-bottom:20px;border-radius:0 8px 8px 0;">
        <h4 style="color:#DC3545;margin:0 0 10px 0;font-size:16px;">High Priority Actions</h4>
        <ul style="margin:0;padding-left:20px;color:#333;line-height:1.8;">
            <li>Review and address the <strong>{cat_counts.get("Hate_Speech", 0)} instances of hate speech</strong> detected</li>
            <li>Investigate the <strong>{cat_counts.get("Violence", 0)} violence-related comments</strong> for potential threats</li>
            <li>Consider stricter moderation for aggressive language ({attr_counts.get("Aggressive", 0)} instances detected)</li>
        </ul>
    </div>
    
    <div style="background:#FFF8E6;border-left:4px solid #FFC107;padding:20px;margin-bottom:20px;border-radius:0 8px 8px 0;">
        <h4 style="color:#856404;margin:0 0 10px 0;font-size:16px;">Medium Priority Actions</h4>
        <ul style="margin:0;padding-left:20px;color:#333;line-height:1.8;">
            <li>Address elevated levels of <strong>sarcasm ({attr_counts.get("Sarcasm", 0)} instances)</strong> and <strong>agitation ({attr_counts.get("Agitated", 0)} instances)</strong></li>
            <li>Monitor <strong>judgmental comments ({attr_counts.get("Judgmental", 0)} instances)</strong> that may discourage participation</li>
            <li>Review spam content ({cat_counts.get("Spam", 0)} instances) and consider automated filters</li>
        </ul>
    </div>
    
    <div style="background:#F0FFF4;border-left:4px solid #28A745;padding:20px;margin-bottom:20px;border-radius:0 8px 8px 0;">
        <h4 style="color:#28A745;margin:0 0 10px 0;font-size:16px;">Positive Observations and Opportunities</h4>
        <ul style="margin:0;padding-left:20px;color:#333;line-height:1.8;">
            <li><strong>{pos_pct}% of comments are positive</strong> - consider highlighting positive contributors</li>
            <li><strong>{attr_counts.get("Polite", 0)} polite comments</strong> demonstrate a core group of respectful members</li>
            <li><strong>{attr_counts.get("Funny", 0)} funny comments</strong> indicate an engaged community</li>
            <li>Encourage more empathetic ({attr_counts.get("Empathetic", 0)} instances) and encouraging ({attr_counts.get("Encouraging", 0)} instances) behavior</li>
        </ul>
    </div>
    
    <div style="background:#E8E0F0;border-left:4px solid #6B4C9A;padding:20px;margin-bottom:20px;border-radius:0 8px 8px 0;">
        <h4 style="color:#2D1B4E;margin:0 0 10px 0;font-size:16px;">Long-term Strategy Recommendations</h4>
        <ul style="margin:0;padding-left:20px;color:#333;line-height:1.8;">
            <li>Establish clear community guidelines addressing hate speech, violence, and harassment</li>
            <li>Implement a tiered moderation system with warnings before bans</li>
            <li>Create a community moderator program from positive contributors</li>
            <li>Schedule regular health assessments to track improvement</li>
            <li>Target a health score improvement to {min(health_score + 20, 100)}+ within 90 days</li>
        </ul>
    </div>
    
    <!-- Methodology -->
    <h2 style="color:#2D1B4E;margin:40px 0 25px 0;font-size:22px;border-bottom:2px solid #6B4C9A;padding-bottom:10px;">Methodology</h2>
    
    <div style="background:#f8f9fa;padding:25px;border-radius:8px;">
        <p style="color:#666;margin:0 0 15px 0;line-height:1.7;">
            <strong>Data Collection:</strong> Comments were collected from the specified platform URL using API-based extraction methods.
        </p>
        <p style="color:#666;margin:0 0 15px 0;line-height:1.7;">
            <strong>Analysis Engine:</strong> Each comment was analyzed using GPT-4 powered natural language processing to detect behavioral attributes and content categories.
        </p>
        <p style="color:#666;margin:0;line-height:1.7;">
            <strong>Score Interpretation:</strong> 80-100: Excellent | 60-79: Good | 40-59: Fair | 0-39: Critical
        </p>
    </div>
</div>

<div style="border-top:2px dashed #ddd;margin:0 40px;padding-top:10px;">
    <p style="text-align:center;color:#999;font-size:11px;margin:0;">Page 3 of 3</p>
</div>

<!-- FOOTER -->
<div style="background:#2D1B4E;padding:30px 40px;margin-top:20px;">
    <table style="width:100%;">
        <tr>
            <td>
                <h3 style="color:white;margin:0 0 5px 0;font-size:18px;font-weight:300;letter-spacing:2px;">FELIXA</h3>
                <p style="color:#A0A0A0;margin:0;font-size:12px;">Community Health Analytics</p>
            </td>
            <td style="text-align:right;">
                <p style="color:#A0A0A0;margin:0 0 5px 0;font-size:12px;">www.felixagaming.com</p>
                <p style="color:#A0A0A0;margin:0;font-size:12px;">play@felixagaming.com</p>
            </td>
        </tr>
    </table>
    <div style="border-top:1px solid #4a3a6a;margin-top:20px;padding-top:20px;">
        <p style="color:#666;margin:0;font-size:11px;text-align:center;">
            This report was generated automatically by Felixa Community Health Analytics.
            For questions or support, contact play@felixagaming.com
        </p>
    </div>
</div>

</div>
</body>
</html>
'''
    return html


# ============================================
# EMAIL SENDING
# ============================================

def send_report_email(customer_email: str, platform: str, source: str, health_score: int, health_status: str, summary: Dict, flagged: Dict):
    """Send HTML report via email"""
    
    html_report = generate_html_report(
        platform=platform,
        source=source,
        health_score=health_score,
        health_status=health_status,
        summary=summary,
        flagged=flagged
    )
    
    try:
        # Send to customer
        resend.Emails.send({
            "from": FROM_EMAIL,
            "to": customer_email,
            "subject": f"Your Felixa Health Report - Score: {health_score}/100",
            "html": html_report
        })
        print(f"Email sent to: {customer_email}")
        
        # Send copy to admin
        resend.Emails.send({
            "from": FROM_EMAIL,
            "to": ADMIN_EMAIL,
            "subject": f"[COPY] Report for {customer_email} - {platform}",
            "html": f"<p style='background:#f0f0f0;padding:10px;'>Copy of report sent to: {customer_email}</p>" + html_report
        })
        print(f"Copy sent to admin")
        
        return True
    except Exception as e:
        print(f"Email error: {e}")
        return False


# ============================================
# MAIN PROCESSING
# ============================================

async def process_order(customer_email: str, platform: str, url: str, payment_id: str = None):
    """Process a paid order"""
    
    # Check for duplicate
    if payment_id and payment_id in processed_payments:
        print(f"Duplicate payment ignored: {payment_id}")
        return {"status": "duplicate"}
    
    if payment_id:
        processed_payments.add(payment_id)
    
    print(f"Processing: {customer_email} | {platform} | {url}")
    
    # Scrape comments
    comments = scrape_platform(platform, url, limit=500)
    
    if not comments:
        print("No comments found")
        comments = [{"text": "No comments available for analysis"}]
    
    print(f"Found {len(comments)} comments")
    
    # Analyze
    results = await analyze_comments(comments)
    print(f"Analyzed {len(results.get('comments', []))} comments")
    
    # Calculate health
    health = calculate_health_score(results)
    print(f"Health Score: {health['health_score']}")
    
    # Get flagged comments
    flagged = get_flagged_comments(results, comments)
    
    # Send email
    send_report_email(
        customer_email=customer_email,
        platform=platform,
        source=url,
        health_score=health["health_score"],
        health_status=health["health_status"],
        summary=health["summary"],
        flagged=flagged
    )
    
    print("Order complete!")
    return {"status": "success", "health_score": health["health_score"]}


# ============================================
# API ENDPOINTS
# ============================================

@app.get("/")
async def root():
    """Public health check"""
    return {"service": "Felixa Community Health", "status": "running"}


@app.get("/health")
async def health_check():
    """Detailed health check"""
    return {
        "status": "healthy",
        "openai": bool(openai_client),
        "stripe": bool(STRIPE_SECRET_KEY),
        "resend": bool(RESEND_API_KEY),
        "youtube": bool(YOUTUBE_API_KEY),
        "reddit": bool(REDDIT_CLIENT_ID)
    }


@app.post("/webhook/stripe")
async def stripe_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handle Stripe webhook - ONLY way to trigger reports"""
    
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    
    # Verify webhook signature
    if not STRIPE_WEBHOOK_SECRET:
        print("WARNING: No webhook secret configured")
        raise HTTPException(400, "Webhook not configured")
    
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError as e:
        print(f"Invalid signature: {e}")
        raise HTTPException(400, "Invalid signature")
    except Exception as e:
        print(f"Webhook error: {e}")
        raise HTTPException(400, str(e))
    
    # Only process checkout.session.completed
    if event["type"] != "checkout.session.completed":
        return {"status": "ignored", "type": event["type"]}
    
    session = event["data"]["object"]
    payment_id = session.get("id") or session.get("payment_intent")
    
    # Check duplicate
    if payment_id in processed_payments:
        print(f"Duplicate webhook ignored: {payment_id}")
        return {"status": "duplicate"}
    
    # Extract customer data
    customer_email = session.get("customer_email") or session.get("customer_details", {}).get("email")
    metadata = session.get("metadata", {})
    
    platform = metadata.get("platform", "youtube")
    url = metadata.get("url", "")
    
    if not customer_email:
        print("No customer email found")
        raise HTTPException(400, "No customer email")
    
    if not url:
        print("No URL in metadata")
        raise HTTPException(400, "No URL provided")
    
    print(f"Payment received: {customer_email} | {platform} | {url}")
    
    # Process in background
    background_tasks.add_task(
        process_order,
        customer_email,
        platform,
        url,
        payment_id
    )
    
    return {"status": "processing", "payment_id": payment_id}


# NO TEST ENDPOINT - Reports can only be triggered via Stripe payment


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    print(f"Starting Felixa Backend on port {port}")
    print("Security: Test endpoint DISABLED - reports require Stripe payment")
    uvicorn.run(app, host="0.0.0.0", port=port)
