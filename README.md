# 🏡 Property24 Africa Scraper

> **Unlock Africa's largest real estate dataset — no API, no manual exports, no hassle.**

Property24 is the #1 property portal across Southern and East Africa. This actor gives you instant, structured access to tens of thousands of active for-sale listings across 7 countries — ready to download as JSON, CSV, or Excel in minutes.

---

## 🌍 What You Get

Run once. Get a full country's worth of property listings delivered straight to your dataset.

| Country | Listings | Time |
|---|---|---|
| 🇿🇦 **South Africa** | ~46,000 listings | ~44 min |
| 🇳🇦 **Namibia** | ~7,400 listings | ~5 min |
| 🇰🇪 **Kenya** | ~7,200 listings | ~7 min |
| 🇿🇼 **Zimbabwe** | ~730 listings | ~2 min |
| 🇿🇲 **Zambia** | ~610 listings | ~3 min |
| 🇧🇼 **Botswana** | ~520 listings | ~2 min |
| 🇳🇬 **Nigeria** | ~110 listings | ~2 min |

---

## 📊 Sample Output

Here's what a typical record looks like:

```json
{
  "listing_url": "https://www.property24.com/for-sale/sandton/johannesburg/gauteng/3456789",
  "title": "3 Bedroom House For Sale in Sandton",
  "price": "R 4 200 000",
  "suburb": "Sandton",
  "city": "Johannesburg",
  "province": "Gauteng",
  "property_type": "House",
  "listing_type": "For Sale",
  "bedrooms": "3",
  "bathrooms": "2",
  "parking": "2",
  "floor_size": "210 m²",
  "erf_size": "600 m²",
  "image": "https://images.prop24.com/...",
  "country": "south_africa",
  "date_scraped": "2026-03-09T10:00:00Z"
}
```

Every record includes **17 fields** — price, size, location, property type, images, and a timestamp so you always know how fresh the data is.

---

## 💼 What Can You Build With This?

Whether you're a developer, analyst, or business — this data opens doors:

**📈 Market Research & Analytics**
Track asking prices by suburb, monitor inventory levels, identify up-and-coming neighbourhoods, and benchmark price-per-m² across cities.

**🏦 Financial & Investment Analysis**
Banks, insurers, and investment funds use property data to model risk, assess collateral values, and spot market trends before they hit the headlines.

**🔍 PropTech Products**
Power a price estimator, build a property comparison app, or feed a listing aggregator — all without scraping manually or paying for an expensive data subscription.

**📰 Journalism & Academic Research**
Housing affordability, urban migration, economic inequality — the data is all there, if you know how to pull it.

**🤝 Lead Generation**
Build targeted databases of active listings for estate agencies, bond originators, or moving companies.

---

## ⚙️ How To Use It

**Step 1 — Pick your country**

```json
{
  "country": "south_africa"
}
```

Available options: `south_africa` · `namibia` · `kenya` · `zimbabwe` · `zambia` · `botswana` · `nigeria`

**Step 2 — Hit Start**

Listings stream into your dataset in real time as pages are scraped. You can watch the count go up live.

**Step 3 — Export your data**

Download as **JSON**, **CSV**, or **Excel** with one click. Pipe it into your database, spreadsheet, or BI tool of choice.

That's it. No API keys. No browser automation. No setup headaches.

---

## 🛠️ Under The Hood

- Built with **Python + httpx** — fast async HTTP, no heavy browser overhead
- Uses **Apify residential proxies** for reliable, unblocked access
- Covers **provinces → cities → listings** in a fully parallel pipeline
- Records stream to the dataset as they're found — no waiting for the full run to finish
- Each record includes a `date_scraped` UTC timestamp for freshness tracking
- Deduplication built in — no duplicate listings even across overlapping city areas

---

## 📝 Notes

- Covers **for-sale listings** only (rental mode coming soon)
- South Africa covers all 9 provinces and ~194 cities
- Other countries cover all major urban areas where Property24 is active
- Ghana is currently unavailable due to site-level access restrictions
- Listing counts fluctuate as properties sell and new ones are listed — run regularly for fresh data

## ⚠️ Legal

This actor collects publicly available listing data. Use responsibly and ensure compliance with applicable laws in your jurisdiction.

---

*Made with ❤️ from Windhoek, Namibia 🇳🇦 — because African real estate data should be accessible to everyone.*