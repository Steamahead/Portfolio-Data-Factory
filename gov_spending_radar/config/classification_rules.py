"""
Classification rules for Gov Spending Radar 2.0.
=================================================
All keyword/CPV rules in one editable file.
Used by classify_notice_multilabel() in main.py.
"""

# ── KEYWORD_RULES ─────────────────────────────────────────────────
# For each sector: `phrases` (positive match) and `negative_phrases` (exclusion).
# Matching is case-insensitive on notice title.
# If any negative_phrase matches → skip that sector entirely.
# If any phrase matches → emit KEYWORD record (confidence 0.5).

KEYWORD_RULES: dict[str, dict] = {
    "AI": {
        "phrases": [
            "sztuczna inteligencja",
            "sztucznej inteligencji",
            "artificial intelligence",
            "machine learning",
            "uczenie maszynowe",
            "uczenia maszynowego",
            "deep learning",
            "neural",
            "model językow",
            "modeli językow",
            "LLM",
            "GPT",
            "chatbot",
        ],
        "negative_phrases": [
            "sztuczna nawierzchnia",
            "sztucznej nawierzchni",
            "sztuczną nawierzchnię",
            "sztuczna trawa",
            "sztucznej trawy",
            "sztuczną trawę",
            "boisko",
            "nawierzchnia sportowa",
            "nawierzchni sportowej",
            "plac zabaw",
            "sztuczne lodowisko",
            "sztucznego lodowiska",
        ],
    },
    "CYBERSECURITY": {
        "phrases": [
            "cyberbezpieczeństwo",
            "cyberbezpieczeństwa",
            "bezpieczeństwo informacyjne",
            "bezpieczeństwa informacyjnego",
            "bezpieczeństwo informatyczne",
            "bezpieczeństwa informatycznego",
            "bezpieczeństwo IT",
            "bezpieczeństwa IT",
            "bezpieczeństwo sieci",
            "bezpieczeństwa sieci",
            "bezpieczeństwo cybernetyczne",
            "bezpieczeństwa cybernetycznego",
            "SOC",
            "SIEM",
            "pentest",
            "audyt bezpieczeństwa IT",
            "audyt cyberbezpieczeństwa",
            "ochrona danych osobowych",
            "ochrony danych osobowych",
            "firewall",
            "WAF",
            "IDS",
            "IPS",
        ],
        "negative_phrases": [
            "bezpieczeństwo ruchu drogowego",
            "bezpieczeństwa ruchu drogowego",
            "bezpieczeństwo pożarowe",
            "bezpieczeństwa pożarowego",
            "bezpieczeństwo żywności",
            "bezpieczeństwa żywności",
            "bezpieczeństwo i higiena pracy",
            "bezpieczeństwa i higieny pracy",
            "bezpieczeństwo publiczne",
            "bezpieczeństwa publicznego",
            "bezpieczeństwo zdrowotne",
            "bezpieczeństwa zdrowotnego",
            "bezpieczeństwo przeciwpożarowe",
            "bezpieczeństwa przeciwpożarowego",
            "bezpieczeństwo na drogach",
            "ochrona fizyczna",
            "ochrony fizycznej",
            "ochrona mienia",
            "ochrony mienia",
            "ochrona obiektów",
            "ochrony obiektów",
            "monitoring wizyjny",
            "monitoringu wizyjnego",
        ],
    },
    "CLOUD": {
        "phrases": [
            "chmura obliczeniowa",
            "chmury obliczeniowej",
            "cloud computing",
            "SaaS",
            "IaaS",
            "PaaS",
            "usługi chmurowe",
            "usług chmurowych",
            "infrastruktura chmurowa",
            "infrastruktury chmurowej",
            "Azure",
            "AWS",
            "Google Cloud",
        ],
        "negative_phrases": [],
    },
    "DATA_ANALYTICS": {
        "phrases": [
            "analiza danych",
            "analizy danych",
            "big data",
            "business intelligence",
            "hurtownia danych",
            "hurtowni danych",
            "data warehouse",
            "dashboard",
            "raportowanie",
            "Power BI",
            "Tableau",
            "data lake",
        ],
        "negative_phrases": [],
    },
    "IT": {
        "phrases": [
            "informatyk",
            "informatyczn",
            "oprogramowanie",
            "oprogramowania",
            "system informatyczn",
            "systemu informatyczn",
            "serwer",
            "baza danych",
            "bazy danych",
            "ERP",
            "CRM",
            "wdrożenie systemu",
            "wdrożenia systemu",
            "infrastruktura IT",
            "infrastruktury IT",
            "usługi IT",
            "usług IT",
        ],
        "negative_phrases": [],
    },
}

# ── CPV prefix lists ──────────────────────────────────────────────

# IT-related CPV prefixes (for boost logic)
CPV_IT_PREFIXES: list[str] = [
    "72",         # IT services (software, data processing, internet)
    "48",         # Software packages and information systems
    "30",         # Office and computing machinery
    "32",         # Radio, TV, telecom equipment
    "64",         # Postal and telecommunications services
    "79632000",   # Personnel training (IT context)
    "80533100",   # Computer training
]

# Non-IT CPV prefixes (for penalty logic)
CPV_NON_IT_HARD: list[str] = [
    "45",  # Construction works
    "44",  # Construction structures and materials
    "34",  # Transport equipment
    "15",  # Food, beverages, tobacco
    "33",  # Medical equipment
    "90",  # Sewage, refuse, cleaning, environmental
    "24",  # Chemical products
    "39",  # Furniture, furnishings
    "37",  # Musical instruments, sport goods, games
    "55",  # Hotel, restaurant, catering
    "19",  # Leather, textile
]

# Non-tech sector CPV mapping (moved from config.yaml for non-keyword sectors)
CPV_SECTOR_MAP: dict[str, list[str]] = {
    "CONSTRUCTION": ["45", "71"],
    "MEDICAL": ["33", "85"],
    "ENERGY": ["09", "65"],
    "TELECOM": ["642", "325"],
}
