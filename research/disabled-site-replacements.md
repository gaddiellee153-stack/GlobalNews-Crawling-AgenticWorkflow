# Disabled Site Replacement Recommendations

## Summary

9 sites disabled due to hard paywall, bot-blocking, or accessibility issues. RSS-verified replacements found for all 9 slots.

| # | Disabled Site | Country | Replacement | RSS Items | Status |
|---|--------------|---------|-------------|-----------|--------|
| 1 | WSJ | US | CBS News MoneyWatch | 31 | VERIFIED |
| 2 | FT | UK | City A.M. | 20 | VERIFIED |
| 3 | CNN | US | ABC News | 25 | VERIFIED |
| 4 | iRobot News | KR | Digital Daily (ddaily) | 16 | VERIFIED |
| 5 | Le Figaro | FR | France24 FR (multiple feeds) | 30/feed | VERIFIED |
| 6 | Liberation | FR | France24 FR Europe | 30 | VERIFIED |
| 7 | Ouest-France | FR | France24 FR France | 30 | VERIFIED |
| 8 | Euractiv | EU | EUobserver | 20 | PARTIAL (soft paywall) |
| 9 | Iceland Monitor | IS | Reykjavik Grapevine | 15 | VERIFIED |

---

## Detailed Recommendations

### 1. WSJ Replacement: CBS News MoneyWatch

**Disabled reason**: Hard paywall
**Replacement**: CBS News MoneyWatch (cbsnews.com/moneywatch)
**Country**: US | **Language**: English | **Topic**: Financial/business news

| Check | Result |
|-------|--------|
| RSS URL | `https://www.cbsnews.com/latest/rss/moneywatch` |
| RSS valid | YES - 31 items, dated March 24, 2026 |
| Article access | YES - Full article text, no paywall, no login |
| Bot blocking | LOW - RSS and article pages both accessible |
| Content match | Financial news, market analysis, consumer costs, economy |

**Why CBS MoneyWatch over alternatives tested**:
- Business Insider: RSS works via Feedburner (`feeds.feedburner.com/businessinsider`, 10 items) BUT article pages block direct access (site returns connection refused via WebFetch)
- TheStreet: RSS works (10 items) BUT article pages return 403
- Quartz: RSS works (50 items) BUT article pages return 403
- Motley Fool: RSS is STALE (last updated Jan 2022, only 3 items)
- Investopedia: Blocks RSS access entirely

**Also available**: CBS News main feed (`/latest/rss/main`, 30 items) for general news coverage.

**sources.yaml key**: `cbsnews_moneywatch`

---

### 2. FT Replacement: City A.M.

**Disabled reason**: Hard paywall
**Replacement**: City A.M. (cityam.com)
**Country**: UK | **Language**: English | **Topic**: Financial/business news (London City focus)

| Check | Result |
|-------|--------|
| RSS URL | `https://www.cityam.com/feed/` |
| RSS valid | YES - 20 items, dated March 25, 2026 |
| Article access | YES - Full article text, no paywall, no login |
| Bot blocking | LOW - Both RSS and articles freely accessible |
| Content match | Banking, fintech, markets, UK economy, City of London |

**Why City A.M. over alternatives tested**:
- This Is Money (Daily Mail financial): Blocks access
- MoneyWeek: Returns 403 on RSS
- City A.M. is the only UK financial news site tested that has working RSS + accessible articles + no paywall

**Verified article**: "Revolut profit booms after poaching record customers from high street banks" - full text accessible.

**sources.yaml key**: `cityam`

---

### 3. CNN Replacement: ABC News

**Disabled reason**: Bot block HIGH
**Replacement**: ABC News (abcnews.com)
**Country**: US | **Language**: English | **Topic**: General news

| Check | Result |
|-------|--------|
| RSS URL | `https://abcnews.com/abcnews/topstories` |
| RSS valid | YES - 25 items, dated March 23-24, 2026 |
| Article access | YES - Full article text, no paywall, freely browsable |
| Bot blocking | LOW - RSS and pages accessible (note: redirects from go.com to abcnews.com) |
| Content match | General news, politics, international, technology, health |

**Additional RSS feeds**:
- International: `https://abcnews.com/abcnews/internationalheadlines` (25 items)
- Business: accessible via site sections

**Why ABC News over alternatives tested**:
- The Hill: RSS works (100 items!) BUT article pages return 403
- CBS News is recommended for WSJ/financial slot, so ABC News fills the general news gap
- ABC News article pages confirmed fully accessible without restrictions

**sources.yaml key**: `abcnews`

---

### 4. iRobot News Replacement: Digital Daily (ddaily)

**Disabled reason**: Bot block HIGH
**Replacement**: Digital Daily / ddaily (ddaily.co.kr)
**Country**: KR | **Language**: Korean | **Topic**: Tech/IT/digital industry news

| Check | Result |
|-------|--------|
| RSS URL | `https://feeds.feedburner.com/ddaily` |
| RSS valid | YES - 16 items, dated March 25, 2026 |
| Article access | YES - Full article text, no paywall, no login |
| Bot blocking | LOW - Both Feedburner RSS and article pages accessible |
| Content match | Cybersecurity, AI, semiconductors, telecom, IT industry |

**Why Digital Daily over alternatives tested**:
- ZDNet Korea: RSS URL returns 404 (`/rss/all_news.xml` not found)
- AI Times (aitimes.kr): Returns 403 on RSS
- IT Chosun (it.chosun.com): Connection refused / blocked
- Digital Daily via Feedburner is the only Korean tech RSS that works end-to-end

**Verified article**: "미국 RSAC 방문한 국산 보안업계" - full text accessible, Korean encoding correct.

**sources.yaml key**: `ddaily`

---

### 5. Le Figaro Replacement: France24 FR (une)

**Disabled reason**: Soft paywall
**Replacement**: France24 French edition - main feed (france24.com/fr)
**Country**: FR | **Language**: French | **Topic**: General news (French perspective)

| Check | Result |
|-------|--------|
| RSS URL | `https://www.france24.com/fr/rss` |
| RSS valid | YES - 26 items, dated March 25, 2026 |
| Article access | YES - France24 is publicly funded, no paywall |
| Bot blocking | LOW - All RSS feeds freely accessible |
| Content match | French and international news, politics, economy |

**NOTE**: France24 is already in the system as `france24` (English) and `france24_fr` (French) and `france24_en`. If `france24_fr` is already covering the French-language slot, this recommendation adds specific SECTION feeds to expand coverage:

**Section feeds for Le Figaro replacement scope**:
- Economy/Tech: `https://www.france24.com/fr/economie/rss` (30 items)
- France domestic: `https://www.france24.com/fr/france/rss` (30 items)
- Culture: `https://www.france24.com/fr/culture/rss` (30 items)

**If france24_fr already exists**: Consider adding these as additional `rss_urls` entries to the existing france24_fr config rather than creating a new source. Otherwise, the Le Figaro slot may not need a separate site if france24_fr already covers French general news adequately.

**Alternative approach**: If a distinct source is needed, the France24 French feeds provide ~150 items/day across sections, comparable to Le Figaro's output.

**sources.yaml key**: Expand `france24_fr` with additional section feeds OR keep as-is if coverage is sufficient.

---

### 6. Liberation Replacement: France24 FR Europe

**Disabled reason**: Soft paywall
**Replacement**: France24 French edition - Europe section
**Country**: FR | **Language**: French | **Topic**: European/progressive news

| Check | Result |
|-------|--------|
| RSS URL | `https://www.france24.com/fr/europe/rss` |
| RSS valid | YES - 30 items, dated March 25, 2026 |
| Article access | YES - Publicly funded, no paywall |
| Bot blocking | LOW |
| Content match | European politics, elections, EU policy from French perspective |

**Why this over alternatives tested**:
- All independent French outlets tested (Le Monde, Le Point, L'Express, Mediapart, L'Obs, RFI, BFM TV, La Depeche, Le Parisien, LCI, France TV Info, Courrier International, Ouest-France) either BLOCK access or return errors
- France24 is the only French-language outlet with reliably accessible RSS feeds from outside France
- This is likely a geographic IP restriction issue: French news sites may serve content differently to non-FR IPs

**Same note as #5**: If france24_fr already exists, add Europe section feed as an additional RSS URL.

**sources.yaml key**: Expand `france24_fr`

---

### 7. Ouest-France Replacement: France24 FR France section

**Disabled reason**: Accessibility issues
**Replacement**: France24 French edition - France domestic section
**Country**: FR | **Language**: French | **Topic**: French domestic/regional news

| Check | Result |
|-------|--------|
| RSS URL | `https://www.france24.com/fr/france/rss` |
| RSS valid | YES - 30 items, dated March 24-25, 2026 |
| Article access | YES - Publicly funded, no paywall |
| Bot blocking | LOW |
| Content match | French domestic news, municipal elections, social issues |

**Limitation**: France24 covers national-level French news, not regional news like Ouest-France did. Regional French news (Brittany, Loire) will not be covered. This is an acceptable trade-off given that no French regional news site has accessible RSS from outside France.

**sources.yaml key**: Already covered by expanded `france24_fr`

---

### 8. Euractiv Replacement: EUobserver (with caveats)

**Disabled reason**: Various issues
**Replacement**: EUobserver (euobserver.com)
**Country**: EU/Brussels | **Language**: English | **Topic**: EU policy, politics

| Check | Result |
|-------|--------|
| RSS URL | `https://euobserver.com/rss` |
| RSS valid | YES - 20 items, dated March 24, 2026 |
| Article access | PARTIAL - Headlines/previews free, full articles behind soft paywall |
| Bot blocking | LOW - RSS is accessible |
| Content match | EU politics, policy, economy, investigations |

**CAVEAT**: EUobserver has a soft paywall ("To read this story, log in or subscribe" starting at ~1.75 EUR/week). The RSS feed provides headlines and summaries but full articles require subscription.

**Alternatives tested**:
- EU Reporter (eureporter.co): RSS feed exists (8 items) BUT returns 403 on homepage
- Euractiv: Blocks all access from WebFetch
- New Europe: Domain redirects to unrelated site (vectorizer.io)
- Brussels Times: No working RSS feed found
- Politico EU: Already in system (`politico_eu`), blocks WebFetch

**Recommendation**: Use EUobserver RSS for **headline + summary extraction only** (title_only mode or extract RSS description field). The RSS `<description>` field typically contains a 1-2 sentence summary which is useful for news monitoring even without full article text. Configure `extraction.paywall_type: soft-metered` and `extraction.title_only: true` if full-text extraction fails.

**sources.yaml key**: `euobserver`

---

### 9. Iceland Monitor Replacement: Reykjavik Grapevine

**Disabled reason**: Various issues
**Replacement**: Reykjavik Grapevine (grapevine.is)
**Country**: IS | **Language**: English | **Topic**: Icelandic news, culture, events

| Check | Result |
|-------|--------|
| RSS URL | `https://grapevine.is/feed/` |
| RSS valid | YES - 15 items, dated March 12-24, 2026, WordPress-generated |
| Article access | YES - No paywall, freely accessible, mission-driven publication |
| Bot blocking | LOW - WordPress site, standard access |
| Content match | Iceland news, culture, music, politics, environment |

**Why Reykjavik Grapevine over alternatives**:
- Iceland Review: RSS works (10 items) BUT lower volume and WordPress JS-heavy rendering
- RUV English: Already in system (`ruv_english`)
- Grapevine has clear RSS, accessible articles, no paywall, and identifies as "Iceland's biggest and most widely read English-language publication"

**Verified**: News section shows 25+ articles, fully accessible, no login required. Covers fur farming policy, housing legislation, weather events, international relations, culture.

**sources.yaml key**: `grapevine`

---

## Consolidated French Strategy

Three French sites disabled (Le Figaro, Liberation, Ouest-France) but ALL independent French news RSS feeds are inaccessible from outside France. The only viable French-language option is France24 FR, which is already in the system.

**Recommended approach**:
1. If `france24_fr` already has a single RSS URL, expand to multiple section feeds:
   - Main: `https://www.france24.com/fr/rss` (26 items)
   - France: `https://www.france24.com/fr/france/rss` (30 items)
   - Europe: `https://www.france24.com/fr/europe/rss` (30 items)
   - Economy: `https://www.france24.com/fr/economie/rss` (30 items)
   - Middle East: `https://www.france24.com/fr/moyen-orient/rss` (30 items)
   - Africa: `https://www.france24.com/fr/afrique/rss` (30 items)
   - Culture: `https://www.france24.com/fr/culture/rss` (30 items)
2. This gives ~200 items/day from France24 FR across all sections
3. Accept that regional French news (Ouest-France's niche) has no replacement

---

## Additional Notes

### The Hill (thehill.com) - Almost Viable
- RSS feed is excellent: 100 items per refresh, unbiased politics coverage
- BUT article pages return 403 (bot blocking)
- **Could work with RSS-only extraction** (title + description from feed) if content extraction from articles is not required. RSS descriptions often contain 1-2 paragraph summaries.
- RSS URL: `https://thehill.com/feed/`
- sources.yaml key if added: `thehill` with `title_only: true`

### Business Insider - Almost Viable
- RSS works via Feedburner: `feeds.feedburner.com/businessinsider` (10 items)
- BUT direct site access blocked (cannot verify article extraction)
- Could serve as backup US financial source if RSS-only extraction is acceptable

### Failed Candidates Summary

| Site | Issue |
|------|-------|
| TheStreet | Article pages 403 |
| Quartz | Article pages 403 |
| Motley Fool | RSS stale (2022) |
| Investopedia | Blocks RSS |
| MoneyWeek (UK) | RSS 403 |
| This Is Money (UK) | Blocks access |
| ZDNet Korea | RSS 404 |
| AI Times Korea | RSS 403 |
| IT Chosun | Connection refused |
| Le Monde | Blocks access |
| Le Point | Blocks access |
| L'Express | Blocks access |
| BFM TV | Blocks access |
| Mediapart | Blocks access |
| L'Obs | Blocks access |
| RFI | Blocks access |
| La Depeche | Blocks access |
| Le Parisien | Blocks access |
| LCI | Blocks access |
| France TV Info | Blocks access |
| Courrier Intl | Blocks access |
| Sud Ouest | Blocks access |
| Ouest-France | Blocks access |
| EU Reporter | Homepage 403 |
| New Europe | Domain sold/redirected |
| Brussels Times | No RSS feed |
| The Local FR | RSS 404 |

---

## sources.yaml Configuration Templates

### cbsnews_moneywatch
```yaml
cbsnews_moneywatch:
  name: CBS News MoneyWatch
  url: https://www.cbsnews.com/moneywatch
  region: us
  language: en
  group: C  # or appropriate group
  crawl:
    primary_method: rss
    fallback_methods:
    - dom
    rss_url: https://www.cbsnews.com/latest/rss/moneywatch
    article_link_css: a[href*='/news/']
    rate_limit_seconds: 5
    max_requests_per_hour: 360
  anti_block:
    ua_tier: 1
    bot_block_level: LOW
  extraction:
    paywall_type: none
    title_only: false
    rendering_required: false
    charset: utf-8
  meta:
    difficulty_tier: Easy
    daily_article_estimate: 30
    enabled: true
```

### cityam
```yaml
cityam:
  name: City A.M.
  url: https://www.cityam.com
  region: uk
  language: en
  group: C
  crawl:
    primary_method: rss
    fallback_methods:
    - dom
    rss_url: https://www.cityam.com/feed/
    article_link_css: a[href*='cityam.com/']
    rate_limit_seconds: 5
    max_requests_per_hour: 360
  anti_block:
    ua_tier: 1
    bot_block_level: LOW
  extraction:
    paywall_type: none
    title_only: false
    rendering_required: false
    charset: utf-8
  meta:
    difficulty_tier: Easy
    daily_article_estimate: 40
    enabled: true
```

### abcnews
```yaml
abcnews:
  name: ABC News
  url: https://abcnews.com
  region: us
  language: en
  group: C
  crawl:
    primary_method: rss
    fallback_methods:
    - dom
    rss_url: https://abcnews.com/abcnews/topstories
    additional_rss_urls:
    - https://abcnews.com/abcnews/internationalheadlines
    article_link_css: a[href*='abcnews.com/']
    rate_limit_seconds: 5
    max_requests_per_hour: 360
  anti_block:
    ua_tier: 1
    bot_block_level: LOW
  extraction:
    paywall_type: none
    title_only: false
    rendering_required: false
    charset: utf-8
  meta:
    difficulty_tier: Easy
    daily_article_estimate: 80
    enabled: true
```

### ddaily
```yaml
ddaily:
  name: Digital Daily
  url: https://www.ddaily.co.kr
  region: kr
  language: ko
  group: B
  crawl:
    primary_method: rss
    fallback_methods:
    - dom
    rss_url: https://feeds.feedburner.com/ddaily
    article_link_css: a[href*='/page/view/']
    rate_limit_seconds: 5
    max_requests_per_hour: 360
  anti_block:
    ua_tier: 1
    bot_block_level: LOW
  extraction:
    paywall_type: none
    title_only: false
    rendering_required: false
    charset: utf-8
  meta:
    difficulty_tier: Easy
    daily_article_estimate: 50
    enabled: true
```

### euobserver
```yaml
euobserver:
  name: EUobserver
  url: https://euobserver.com
  region: eu
  language: en
  group: G
  crawl:
    primary_method: rss
    fallback_methods: []
    rss_url: https://euobserver.com/rss
    rate_limit_seconds: 10
    max_requests_per_hour: 120
  anti_block:
    ua_tier: 1
    bot_block_level: LOW
  extraction:
    paywall_type: soft-metered
    title_only: true
    rendering_required: false
    charset: utf-8
  meta:
    difficulty_tier: Medium
    daily_article_estimate: 20
    enabled: true
```

### grapevine
```yaml
grapevine:
  name: Reykjavik Grapevine
  url: https://grapevine.is
  region: is
  language: en
  group: G
  crawl:
    primary_method: rss
    fallback_methods:
    - dom
    rss_url: https://grapevine.is/feed/
    article_link_css: a[href*='grapevine.is/']
    rate_limit_seconds: 10
    max_requests_per_hour: 120
  anti_block:
    ua_tier: 1
    bot_block_level: LOW
  extraction:
    paywall_type: none
    title_only: false
    rendering_required: false
    charset: utf-8
  meta:
    difficulty_tier: Easy
    daily_article_estimate: 5
    enabled: true
```

### france24_fr (expanded - if updating existing entry)
```yaml
# Add these section feeds to existing france24_fr config:
crawl:
  rss_url: https://www.france24.com/fr/rss
  additional_rss_urls:
  - https://www.france24.com/fr/france/rss
  - https://www.france24.com/fr/europe/rss
  - https://www.france24.com/fr/economie/rss
  - https://www.france24.com/fr/moyen-orient/rss
  - https://www.france24.com/fr/afrique/rss
  - https://www.france24.com/fr/culture/rss
```
