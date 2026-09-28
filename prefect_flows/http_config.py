"""Shared HTTP settings for every scraper/adapter (single place to change the contact address)."""
import os

_CONTACT = os.environ.get("SCRAPER_CONTACT", "grirasirin@gmail.com")
USER_AGENT = f"PoliticalSpeechesBot/1.0 (+research project; contact: {_CONTACT})"
