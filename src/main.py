"""
Property24 scraper — multi-country, fast httpx pipeline

Architecture:
  Phase 1 — Playwright ONLY: render homepage to get province links  (~10s, 1 page)
  Phase 2 — httpx async:     province pages -> city links           (~5s,  concurrency 20)
  Phase 3 — httpx async:     city pages + pagination -> parse tiles (~4-5 min, concurrency 30)
                              Tiles parsed IN-PLACE — no Phase 4 needed.

Fields per listing (from tiles): listing_url, title, price, suburb, city, province,
  bedrooms, bathrooms, parking, floor_size, erf_size, property_type, listing_type, image.

Playwright is used ONLY for the homepage because it needs JS to render the nav.
Every other page is plain server-rendered HTML — no browser needed.

Supported countries (configured via Actor input):
  - Namibia   (property24.co.na)
  - South Africa (property24.com)
  - Zambia    (property24.co.zm)
  - Zimbabwe  (property24.co.zw)
  - Botswana  (property24.co.bw)
  - Mozambique (property24.co.mz)
  - Tanzania  (property24.co.tz)
  - Ghana     (property24.com.gh)
  - Kenya     (property24.co.ke)
  - Nigeria   (property24.com.ng)
"""

from apify import Actor
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import asyncio
import httpx
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Country config
# ---------------------------------------------------------------------------

COUNTRIES = {
    "namibia":      "https://www.property24.co.na",
    "south_africa": "https://www.property24.com",
    "zambia":       "https://www.property24.co.zm",
    "zimbabwe":     "https://www.property24.co.zw",
    "botswana":     "https://www.property24.co.bw",
    "mozambique":   "https://www.property24.co.mz",
    "tanzania":     "https://www.property24.co.tz",
    "ghana":        "https://www.property24.com.gh",
    "kenya":        "https://www.property24.co.ke",
    "nigeria":      "https://www.property24.com.ng",
}

# Hardcoded province/region paths per country.
# These are stable top-level browse pages — no homepage scraping needed.
PROVINCE_PATHS = {
    "namibia": [
        "/property-for-sale-in-erongo-p135",
        "/property-for-sale-in-khomas-p139",
        "/property-for-sale-in-otjozondjupa-p141",
        "/property-for-sale-in-oshana-p140",
        "/property-for-sale-in-hardap-p136",
        "/property-for-sale-in-karas-p137",
        "/property-for-sale-in-kunene-p161",
        "/property-for-sale-in-omaheke-p162",
        "/property-for-sale-in-oshikoto-p166",
        "/property-for-sale-in-kavango-east-p138",
        "/property-for-sale-in-zambezi-p134",
        "/property-for-sale-in-ohangwena-p163",
        "/property-for-sale-in-omusati-p165",
        "/property-for-sale-in-kavango-west-p167",
    ],
    "south_africa": [
        "/for-sale/western-cape/9",
        "/for-sale/gauteng/1",
        "/for-sale/durban/kwazulu-natal/578",
        "/for-sale/eastern-cape/2",
        "/for-sale/limpopo/6",
        "/for-sale/mpumalanga/5",
        "/for-sale/north-west/8",
        "/for-sale/free-state/3",
        "/for-sale/northern-cape/7",
    ],
    "kenya": [
        "/property-for-sale-in-nairobi-c1890",
        "/property-for-sale-in-kiambu-c1851",
        "/property-for-sale-in-kajiado-c1836",
        "/property-for-sale-in-mombasa-c1887",
        "/property-for-sale-in-kilifi-c1852",
        "/property-for-sale-in-nakuru-c1896",
        "/property-for-sale-in-kisumu-c1862",
        "/property-for-sale-in-machakos-p66",
    ],
    "ghana": [
        "/property-for-sale-in-greater-accra",
        "/property-for-sale-in-ashanti",
        "/property-for-sale-in-western",
        "/property-for-sale-in-central",
        "/property-for-sale-in-eastern",
        "/property-for-sale-in-northern",
    ],
    "nigeria": [
        "/property-for-sale-in-lagos-p37",   # 942 listings
        "/property-for-sale-in-abuja-p20",   # 187 listings
        "/property-for-sale-in-oyo-p48",     # 90 listings
        "/property-for-sale-in-ogun-p49",    # 18 listings
    ],
    "zambia": [
        "/property-for-sale-in-lusaka-c2327",       # 1,417 listings
        "/property-for-sale-in-copperbelt-p122",    # Kitwe, Ndola, Chingola
        "/property-for-sale-in-southern-p158",      # Livingstone, Mazabuka
        "/property-for-sale-in-central-p152",       # Kabwe, Chisamba
    ],
    "zimbabwe": [
        "/property-for-sale-in-harare-c1729",
        "/property-for-sale-in-bulawayo-c1730",
    ],
    "botswana": [
        "/property-for-sale-in-gaborone-c2250",
        "/property-for-sale-in-mogoditshane-c2234",
    ],
    "tanzania": [
        "/property-for-sale-in-dar-es-salaam",
        "/property-for-sale-in-arusha",
        "/property-for-sale-in-mwanza",
        "/property-for-sale-in-zanzibar",
    ],
    "mozambique": [
        "/property-for-sale-in-maputo",
        "/property-for-sale-in-beira",
        "/property-for-sale-in-nampula",
    ],
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ---------------------------------------------------------------------------
# Scrape metadata (set once at start of each run)
# ---------------------------------------------------------------------------

_SCRAPE_DATE: str = ""
_SCRAPE_COUNTRY: str = ""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def abs_url(base: str, href: str):
    if not href:
        return None
    return urljoin(base, href)


def get_next_page(soup: BeautifulSoup, base: str):
    nxt = soup.select_one('a[rel="next"]')
    if nxt and nxt.get("href"):
        return abs_url(base, nxt["href"])
    for a in soup.select('a[href*="Page="]'):
        txt = a.get_text(strip=True).lower()
        if any(m in txt for m in ["next", ">", "»"]):
            return abs_url(base, a["href"])
    return None


def parse_tiles_from_page(soup: BeautifulSoup, base: str, city_url: str = "") -> list:
    """Extract listing records directly from search result tiles — no detail page needed."""
    records = []
    tiles = (
        soup.select('[class*="p24_regularTile"]') or
        soup.select('[class*="p24_tileContainer"]') or
        soup.select('[class*="js_listingTile"]')
    )

    # Try to extract city/province from the URL slug
    city_name = ""
    province_name = ""
    if city_url:
        slug = city_url.split("?")[0].rstrip("/").split("/")[-1]
        # e.g. "property-for-sale-in-windhoek-c2268" -> "windhoek"
        m = re.search(r"property-for-sale-in-(.+?)(?:-c\d+|-p\d+)?$", slug)
        if m:
            city_name = m.group(1).replace("-", " ").title()

    for tile in tiles:
        # URL
        link = tile.find("a", href=True)
        if not link:
            continue
        url = abs_url(base, link["href"])
        if not url:
            continue
        slug = url.split("/")[-1]
        if re.search(r"-(c|p)\d+", slug):
            continue  # skip pagination/province links

        def tile_txt(*selectors):
            for sel in selectors:
                el = tile.select_one(sel)
                if el:
                    return el.get_text(" ", strip=True)
            return None

        # Core fields
        title        = tile_txt('[class*="p24_propertyTitle"]', '[class*="listingTitle"]', "h3", "h4")
        price        = tile_txt('[class*="p24_price"]', '[class*="price"]')
        suburb       = tile_txt('[class*="p24_suburb"]', '[class*="suburb"]', '[class*="address"]')
        prop_type    = tile_txt('[class*="p24_propertyType"]', '[class*="propertyType"]', '[class*="listing-type"]')

        # Bedrooms / bathrooms / parking / sizes from icon spans or attribute list
        beds    = tile_txt('[title*="Bedroom"]',  '[class*="p24_bedrooms"]',  '[class*="bedrooms"]')
        baths   = tile_txt('[title*="Bathroom"]', '[class*="p24_bathrooms"]', '[class*="bathrooms"]')
        parking = tile_txt('[title*="Parking"]',  '[class*="p24_garages"]',   '[class*="garages"]')
        floor   = tile_txt('[title*="Floor"]',    '[class*="p24_floorSize"]', '[class*="floorSize"]')
        erf     = tile_txt('[title*="Erf"]',      '[class*="p24_erfSize"]',   '[class*="erfSize"]')

        # Listing type (for sale / to rent)
        listing_type = "for_sale"
        if url and "to-rent" in url:
            listing_type = "to_rent"

        # Thumbnail image
        img_el = tile.select_one("img[src]")
        image = img_el["src"] if img_el else None
        if image and image.startswith("data:"):
            # lazy-loaded — try data-src
            image = img_el.get("data-src") or img_el.get("data-lazy-src") or None

        records.append({
            "listing_url":   url,
            "title":         title,
            "price":         price,
            "suburb":        suburb,
            "city":          city_name or None,
            "property_type": prop_type,
            "listing_type":  listing_type,
            "bedrooms":      beds,
            "bathrooms":     baths,
            "parking":       parking,
            "floor_size":    floor,
            "erf_size":      erf,
            "image":         image,
            "country":       _SCRAPE_COUNTRY,
            "date_scraped":  _SCRAPE_DATE,
        })
    return records


def parse_detail(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    def txt(selector):
        el = soup.select_one(selector)
        return el.get_text(" ", strip=True) if el else None

    def get_attr(selector, attribute):
        el = soup.select_one(selector)
        return el.get(attribute, "").strip() if el else None

    title       = txt("h1") or txt('[class*="p24_propertyTitle"]')
    price       = txt('[class*="p24_price"]') or txt('[class*="price"]')
    suburb      = txt('[class*="p24_suburb"]') or txt('[class*="suburb"]')
    description = txt('[class*="p24_description"]') or txt('[id*="description"]')

    attrs = {}
    for row in soup.select(
        '[class*="p24_featureDetails"] li, '
        '[class*="p24_attributes"] li, '
        '[class*="p24_info"] li'
    ):
        label_el = row.select_one('[class*="label"], strong, b')
        value_el = row.select_one('[class*="value"], span:last-child')
        if label_el and value_el:
            k = label_el.get_text(strip=True).lower().rstrip(":")
            v = value_el.get_text(strip=True)
            if k and v:
                attrs[k] = v
    for dt, dd in zip(soup.select("dt"), soup.select("dd")):
        k = dt.get_text(strip=True).lower().rstrip(":")
        v = dd.get_text(strip=True)
        if k and v:
            attrs[k] = v

    beds    = txt('[title*="Bedrooms"]')  or attrs.get("bedrooms")
    baths   = txt('[title*="Bathrooms"]') or attrs.get("bathrooms")
    parking = txt('[title*="Parking"]')   or attrs.get("parking") or attrs.get("garages")
    floor   = txt('[title*="Floor"]')     or attrs.get("floor size") or attrs.get("floor area")
    erf     = txt('[title*="Erf"]')       or attrs.get("erf size") or attrs.get("erf")
    prop_t  = txt('[class*="p24_propertyType"]') or attrs.get("property type")
    listed  = txt('[class*="p24_listingDate"]')   or attrs.get("listed")

    agent_name  = txt('[class*="p24_agentName"], [class*="agentName"]')
    agency_name = txt('[class*="p24_agencyName"], [class*="agencyName"], [class*="p24_branchName"]')
    agent_photo = get_attr('[class*="p24_agentPhoto"] img', "src")
    agency_logo = get_attr('[class*="p24_agencyLogo"] img', "src")

    phone_numbers = []
    for el in soup.select('[class*="p24_phoneNumber"], [class*="phoneNumber"], a[href^="tel:"]'):
        num = el.get("href", "").replace("tel:", "").strip() or el.get_text(strip=True)
        if num:
            phone_numbers.append(num)
    if not phone_numbers:
        raw = re.findall(r'(?:\+\d{3}|0)[\d\s\-]{8,14}', soup.get_text())
        phone_numbers = list(dict.fromkeys(p.strip() for p in raw))[:4]

    email = None
    email_el = soup.select_one('a[href^="mailto:"]')
    if email_el:
        email = email_el.get("href", "").replace("mailto:", "").split("?")[0].strip()
    if not email:
        matches = re.findall(r'[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}', soup.get_text())
        email = matches[0] if matches else None

    images = [
        img["src"] for img in soup.select('[class*="p24_gallery"] img, [class*="gallery"] img')
        if img.get("src")
    ][:10]

    lat = lng = None
    for script in soup.find_all("script"):
        src = script.string or ""
        lm = re.search(r'"latitude"\s*:\s*([-\d.]+)', src) or re.search(r'\blat\s*[:=]\s*([-\d.]+)', src)
        gm = re.search(r'"longitude"\s*:\s*([-\d.]+)', src) or re.search(r'\blng\s*[:=]\s*([-\d.]+)', src)
        if lm: lat = lm.group(1)
        if gm: lng = gm.group(1)
        if lat and lng: break

    features = [
        el.get_text(strip=True)
        for el in soup.select('[class*="p24_featureItem"], [class*="feature-item"]')
        if el.get_text(strip=True)
    ]

    return {
        "listing_url":   url,
        "title":         title,
        "price":         price,
        "suburb":        suburb,
        "property_type": prop_t,
        "bedrooms":      beds,
        "bathrooms":     baths,
        "parking":       parking,
        "floor_size":    floor,
        "erf_size":      erf,
        "description":   description,
        "date_listed":   listed,
        "features":      features or None,
        "images":        images or None,
        "latitude":      lat,
        "longitude":     lng,
        "agent_name":    agent_name,
        "agent_photo":   agent_photo,
        "agency_name":   agency_name,
        "agency_logo":   agency_logo,
        "phone_numbers": phone_numbers or None,
        "email":         email,
    }


# ---------------------------------------------------------------------------
# httpx fetch helpers — client pool with connection reuse + IP rotation
# ---------------------------------------------------------------------------

import random as _random

# Pool of persistent clients, each with a different proxy IP.
# Reusing clients avoids TLS handshake overhead on every request.
_CLIENT_POOL: list = []
_POOL_LOCK = asyncio.Lock()


async def build_client_pool(proxy_cfg, size: int = 10):
    """Pre-warm a pool of persistent httpx clients, each with a distinct proxy URL."""
    global _CLIENT_POOL
    clients = []
    for _ in range(size):
        proxy_url = await proxy_cfg.new_url() if proxy_cfg else None
        transport = httpx.AsyncHTTPTransport(proxy=proxy_url, retries=1) if proxy_url else None
        clients.append(httpx.AsyncClient(headers=HEADERS, timeout=25, transport=transport))
    _CLIENT_POOL = clients
    return clients


async def close_client_pool():
    for c in _CLIENT_POOL:
        await c.aclose()


async def fetch_html(url: str, semaphore: asyncio.Semaphore, proxy_cfg=None, label: str = "") -> tuple:
    """Returns (url, html_or_None). Picks a random pooled client for each request."""
    async with semaphore:
        for attempt in range(3):
            try:
                # Pick random client from pool (round-robin-ish without lock contention)
                client = _CLIENT_POOL[_random.randrange(len(_CLIENT_POOL))] if _CLIENT_POOL else None
                if client:
                    resp = await client.get(url, follow_redirects=True)
                else:
                    # Fallback: no pool yet, create ephemeral client
                    async with httpx.AsyncClient(headers=HEADERS, timeout=25) as c:
                        resp = await c.get(url, follow_redirects=True)
                if resp.status_code == 503:
                    await asyncio.sleep(1 + _random.random())
                    continue
                resp.raise_for_status()
                return url, resp.text
            except Exception as e:
                if attempt == 2:
                    Actor.log.warning(f"Failed {label or url}: {e}")
                else:
                    await asyncio.sleep(0.5 + _random.random())
    return url, None


# ---------------------------------------------------------------------------
# Phase 2: provinces -> cities  (httpx)
# ---------------------------------------------------------------------------

async def get_city_links(province_links: list, base_url: str, proxy_cfg) -> list:
    sem = asyncio.Semaphore(20)
    tasks = [fetch_html(url, sem, proxy_cfg, "province") for url in province_links]
    results = await asyncio.gather(*tasks)

    city_links = []
    for url, html in results:
        if not html:
            city_links.append(url)  # fallback: treat province page as listing page
            continue
        soup = BeautifulSoup(html, "lxml")
        found = 0
        for a in soup.select("a[href]"):
            href = a["href"]
            # Namibia style: /property-for-sale-in-windhoek-c2268
            # SA style:      /for-sale/cape-town/western-cape/432
            is_city = (
                ("property-for-sale-in-" in href and re.search(r"-(c|p)\d+$", href)) or
                (re.match(r"^/for-sale/[^/]+/[^/]+/\d+$", href))
            )
            if is_city:
                city_links.append(abs_url(base_url, href))
                found += 1
        if found == 0:
            city_links.append(url)
        Actor.log.info(f"Province {url.split('/')[-1]}: {found} cities")

    return list(set(city_links))


# ---------------------------------------------------------------------------
# Phase 3: city pages + ALL pagination in one parallel blast
# ---------------------------------------------------------------------------


def get_last_page(soup: BeautifulSoup) -> int:
    """
    Get the last page number by reading pagination links.
    Handles two formats:
      Namibia style: ?Page=N  (query param)
      SA style:      /pN      (path suffix)
    """
    max_page = 1
    for a in soup.select('a[href]'):
        href = a.get("href", "")
        # Namibia: ?Page=N
        m = re.search(r'[?&]Page=(\d+)', href)
        if m:
            max_page = max(max_page, int(m.group(1)))
            continue
        # SA: /p2, /p3 ... at end of path
        m = re.search(r'/p(\d+)$', href)
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page


async def scrape_all_listings(city_links: list, base_url: str, proxy_cfg) -> int:
    """
    Fully parallel tile scraper — no detail pages needed.

    For SA-style URLs (/for-sale/city/province/id): generates ALL page URLs upfront
    (pages 1-MAX per city) and fetches everything in one batched blast — no
    discovery phase needed since the URL pattern is known.

    For Namibia-style URLs: fetch page 1 first to discover last page number,
    then fetch the rest.
    """
    MAX_PAGES_PER_CITY = 20
    BATCH = 500
    sem = asyncio.Semaphore(30)
    total_pushed = 0
    seen_urls: set = set()

    async def push_tiles(html: str, page_url: str):
        nonlocal total_pushed
        if not html:
            return
        soup = BeautifulSoup(html, "lxml")
        records = parse_tiles_from_page(soup, base_url, page_url)
        new_records = [r for r in records if r["listing_url"] not in seen_urls]
        for r in new_records:
            seen_urls.add(r["listing_url"])
        if new_records:
            await Actor.push_data(new_records)
            total_pushed += len(new_records)

    sa_style = any("/for-sale/" in u for u in city_links)

    if sa_style:
        # SA: generate all pages upfront — no discovery fetch needed
        # Pages that don't exist just return 0 tiles, deduplication handles it
        all_pages = []
        for city_url in city_links:
            base = city_url.rstrip("/")
            for p in range(1, MAX_PAGES_PER_CITY + 1):
                page_url = base if p == 1 else f"{base}/p{p}"
                all_pages.append((page_url, base))

        Actor.log.info(f"Phase 3: fetching {len(all_pages)} pages ({len(city_links)} cities × up to {MAX_PAGES_PER_CITY} pages) in batches of {BATCH}...")
        done = 0
        for i in range(0, len(all_pages), BATCH):
            chunk = all_pages[i:i + BATCH]
            results = await asyncio.gather(*[fetch_html(u, sem, proxy_cfg, "page") for u, _ in chunk])
            for (_, city_url), (url, html) in zip(chunk, results):
                await push_tiles(html, city_url)
            done += len(chunk)
            Actor.log.info(f"Phase 3: {done}/{len(all_pages)} pages done, {total_pushed} records")

    else:
        # Namibia: fetch page 1 to discover actual last page, then fetch rest
        Actor.log.info(f"Phase 3a: fetching page 1 of {len(city_links)} cities in parallel...")
        results_a = await asyncio.gather(*[fetch_html(u, sem, proxy_cfg, "city-p1") for u in city_links])

        all_pages = []
        for url, html in results_a:
            await push_tiles(html, url)
            if not html:
                continue
            soup = BeautifulSoup(html, "lxml")
            last_page = min(get_last_page(soup), MAX_PAGES_PER_CITY)
            base = url.split("?")[0].rstrip("/")
            for p in range(2, last_page + 1):
                all_pages.append((f"{base}?Page={p}", base))

        Actor.log.info(f"Phase 3a done: {total_pushed} records from page 1, {len(all_pages)} pagination pages to fetch")

        done = 0
        for i in range(0, len(all_pages), BATCH):
            chunk = all_pages[i:i + BATCH]
            results = await asyncio.gather(*[fetch_html(u, sem, proxy_cfg, "page-N") for u, _ in chunk])
            for (_, city_url), (url, html) in zip(chunk, results):
                await push_tiles(html, city_url)
            done += len(chunk)
            Actor.log.info(f"Phase 3b: {done}/{len(all_pages)} pages done, {total_pushed} records")

    Actor.log.info(f"Phase 3 done: {total_pushed} total records pushed")
    return total_pushed


# ---------------------------------------------------------------------------
# Phase 4: detail pages  (httpx)
# ---------------------------------------------------------------------------

async def scrape_all_details(detail_urls: list, proxy_cfg) -> None:
    sem = asyncio.Semaphore(50)
    tasks = [fetch_html(url, sem, proxy_cfg, "detail") for url in detail_urls]
    done = 0
    for coro in asyncio.as_completed(tasks):
        url, html = await coro
        done += 1
        if html:
            record = parse_detail(html, url)
            await Actor.push_data(record)
        if done % 200 == 0:
            Actor.log.info(f"Details: {done}/{len(detail_urls)} scraped")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    async with Actor:
        # Read input
        actor_input = await Actor.get_input() or {}
        country_key = actor_input.get("country", "namibia").lower().replace(" ", "_")

        if country_key not in COUNTRIES:
            Actor.log.error(
                f"Unknown country '{country_key}'. "
                f"Valid options: {', '.join(COUNTRIES.keys())}"
            )
            return

        base_url  = COUNTRIES[country_key]
        start_url = f"{base_url}/"
        Actor.log.info(f"Scraping Property24 — {country_key.upper()} ({base_url})")

        # Set scrape metadata stamped on every record
        global _SCRAPE_DATE, _SCRAPE_COUNTRY
        _SCRAPE_DATE    = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _SCRAPE_COUNTRY = country_key

        proxy_configuration = await Actor.create_proxy_configuration(
            groups=["RESIDENTIAL"],
            country_code="ZA",
        )

        # ------------------------------------------------------------------ #
        # Phase 1: Build province URLs from hardcoded paths (no Playwright needed)
        # ------------------------------------------------------------------ #
        paths = PROVINCE_PATHS.get(country_key, [])
        if not paths:
            Actor.log.error(f"No province paths configured for '{country_key}'")
            return
        province_links = [f"{base_url}{path}" for path in paths]
        Actor.log.info(f"Phase 1 done: {len(province_links)} provinces configured")

        # ------------------------------------------------------------------ #
        # Build persistent connection pool (reuse connections across all phases)
        # ------------------------------------------------------------------ #
        Actor.log.info("Building connection pool...")
        await build_client_pool(proxy_configuration, size=12)

        # ------------------------------------------------------------------ #
        # Phase 2: httpx — provinces -> cities
        # ------------------------------------------------------------------ #
        Actor.log.info(f"Phase 2: fetching {len(province_links)} province pages...")
        city_links = await get_city_links(province_links, base_url, proxy_configuration)
        Actor.log.info(f"Phase 2 done: {len(city_links)} city pages found")

        if not city_links:
            Actor.log.error("No city pages found")
            return

        # ------------------------------------------------------------------ #
        # Phase 3: httpx — scrape tiles from all city + pagination pages
        # ------------------------------------------------------------------ #
        Actor.log.info(f"Phase 3: scraping tiles from {len(city_links)} cities...")
        total = await scrape_all_listings(city_links, base_url, proxy_configuration)
        await close_client_pool()
        Actor.log.info(f"Done! {total} listings scraped.")


if __name__ == "__main__":
    asyncio.run(main())