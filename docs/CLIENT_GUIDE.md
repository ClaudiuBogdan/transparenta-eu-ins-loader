# INS Statistical Data API - Client Guide

> A comprehensive guide for frontend developers building applications that consume Romanian statistical data.

**Version:** 1.0
**Last Updated:** December 2024
**Base URL:** `http://localhost:3000/api/v1`

---

## Table of Contents

1. [Introduction & Quick Start](#1-introduction--quick-start)
2. [Understanding the Data Model](#2-understanding-the-data-model)
3. [Navigation Patterns](#3-navigation-patterns)
4. [Building a Matrix Explorer UI](#4-building-a-matrix-explorer-ui)
5. [Dimension Filters UI](#5-dimension-filters-ui)
6. [Territory Hierarchy Navigation](#6-territory-hierarchy-navigation)
7. [Data Display Patterns](#7-data-display-patterns)
8. [API Workflow Examples](#8-api-workflow-examples)
9. [Pagination & Performance](#9-pagination--performance)
10. [Error Handling](#10-error-handling)
11. [Best Practices](#11-best-practices)
12. [Appendix](#12-appendix)

---

## 1. Introduction & Quick Start

### What is this API?

The INS Statistical Data API provides access to Romania's official statistical data from the National Institute of Statistics (INS). It includes:

- **1,898 datasets** (matrices) covering population, economy, education, health, and more
- **340 thematic contexts** organized in 8 domains
- **55 territorial levels** from national to county (NUTS3)
- **Bilingual support** (Romanian and English)
- **Time series data** spanning decades (some from 1990)

### Authentication

**No authentication required.** The API is publicly accessible.

### Bilingual Support

All endpoints support the `locale` parameter:

```
?locale=ro  # Romanian (default)
?locale=en  # English
```

**Always use `locale` for user-facing text.** Names, labels, and descriptions are returned in the requested language.

### Quick Start Example

**Goal:** Get Romania's total population for 2023

```javascript
// Step 1: Find the population matrix
const matrices = await fetch('/api/v1/matrices?q=populatie%20rezidenta&locale=ro')
  .then(r => r.json());
// Result: POP105A - "Populatia rezidenta la 1 ianuarie..."

// Step 2: Get the data
const data = await fetch('/api/v1/statistics/POP105A?yearFrom=2023&yearTo=2023&locale=ro')
  .then(r => r.json());

console.log(data.data.dataPoints[0].value);
// Output: 19055228
```

**Simpler:** Use the trends endpoint for a quick overview:

```javascript
const trend = await fetch('/api/v1/trends/POP105A?yearFrom=2020&yearTo=2024')
  .then(r => r.json());

console.log(trend.data.trend);
// { startYear: 2020, endYear: 2024, totalChange: -286763, direction: "down" }
```

---

## 2. Understanding the Data Model

### The Data Hierarchy

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA HIERARCHY                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  CONTEXTS (8 Domains)          ← Browse by theme/topic                  │
│    │                                                                    │
│    └── A. Social Statistics                                             │
│        └── 1. Population                                                │
│            └── 1.1 Resident Population                                  │
│                │                                                        │
│                └── MATRICES (Datasets)    ← The core data containers    │
│                    │                                                    │
│                    └── POP105A: Population by age, sex, residence       │
│                        │                                                │
│                        └── DIMENSIONS     ← How data can be sliced      │
│                            │                                            │
│                            ├── Territorial (Where: Romania, Counties)   │
│                            ├── Temporal (When: Years, Quarters)         │
│                            ├── Classification (What: Sex, Age Group)    │
│                            └── Unit of Measure (How: Persons, %)        │
│                                │                                        │
│                                └── STATISTICS (Values)  ← Actual data   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Key Concepts

| Concept | Description | Example |
|---------|-------------|---------|
| **Context** | Thematic category (domain/subdomain) | "1. POPULATIA" (Population) |
| **Matrix** | A dataset with specific dimensions | POP105A (Resident population) |
| **Dimension** | An axis for slicing data | Sex, Age Group, Territory |
| **Statistics** | The actual numeric values | 19,055,228 persons |

### Dimension Types

Every matrix has 3-6 dimensions of these types:

| Type | Description | UI Element |
|------|-------------|------------|
| **TERRITORIAL** | Geographic location | Hierarchical tree selector |
| **TEMPORAL** | Time period | Year range picker / dropdown |
| **CLASSIFICATION** | Category breakdown | Radio buttons or multi-select |
| **UNIT_OF_MEASURE** | Measurement unit | Usually single dropdown |

---

## 3. Navigation Patterns

### Pattern A: Browse by Topic (Recommended for new users)

**Best for:** General exploration, discovering available data

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│   /discover/topics  →  /tags/:slug  →  /matrices/:code  →  /statistics  │
│        │                    │               │                  │        │
│   [See all topics]    [View matrices   [Get matrix       [Query data]   │
│                        with tag]        details]                        │
└─────────────────────────────────────────────────────────────────────────┘
```

**API Flow:**

```javascript
// 1. Get all topics grouped by category
GET /api/v1/discover/topics?locale=en

// Response: Categories with tags
{
  "data": {
    "categories": [
      {
        "category": "topic",
        "tags": [
          { "id": 1, "name": "Population", "slug": "population", "matrixCount": 47 },
          { "id": 2, "name": "Demographics", "slug": "demographics", "matrixCount": 23 }
        ]
      }
    ]
  }
}

// 2. User selects "Population" → Get matrices with this tag
GET /api/v1/tags/population?locale=en

// 3. User selects POP105A → Get matrix details
GET /api/v1/matrices/POP105A?locale=en

// 4. User configures filters → Query statistics
GET /api/v1/statistics/POP105A?yearFrom=2020&yearTo=2024&territoryLevel=NUTS3
```

---

### Pattern B: Browse by Domain Hierarchy

**Best for:** Users familiar with INS structure, systematic exploration

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│   /contexts  →  /contexts/:id  →  /matrices?contextPath=  →  /matrices  │
│       │              │                    │                      │      │
│   [Top-level    [Drill into        [List matrices         [Get details] │
│    domains]      subdomain]         in context]                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**API Flow:**

```javascript
// 1. Get top-level domains
GET /api/v1/contexts?level=1&locale=en

// 2. Drill into "A. SOCIAL STATISTICS"
GET /api/v1/contexts/1?locale=en
// Returns children: "1. Population", "2. Labor Market", etc.

// 3. Continue drilling to "1.1 Resident Population"
GET /api/v1/contexts/12?locale=en

// 4. Get matrices in this context
GET /api/v1/matrices?contextPath=1.1.12&locale=en
```

---

### Pattern C: Search First

**Best for:** Users who know what they're looking for

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│   /matrices?q=...  →  /matrices/:code  →  /matrices/:code/breakdowns    │
│        │                    │                      │                    │
│   [Search by          [Get details]          [See available             │
│    keyword]                                   dimensions]               │
└─────────────────────────────────────────────────────────────────────────┘
```

**API Flow:**

```javascript
// 1. Search for "unemployment"
GET /api/v1/matrices?q=somaj&locale=ro

// 2. Get details for selected matrix
GET /api/v1/matrices/SOM101B?locale=ro

// 3. See what filters are available
GET /api/v1/matrices/SOM101B/breakdowns?locale=ro
```

---

### Pattern D: Territory-First

**Best for:** Local government apps, regional analysis

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│   /territories  →  /territories/:id/available-data  →  /statistics      │
│        │                      │                            │            │
│   [Browse NUTS         [What data exists          [Query with           │
│    hierarchy]           for this place?]           territory filter]    │
└─────────────────────────────────────────────────────────────────────────┘
```

**API Flow:**

```javascript
// 1. Browse territories
GET /api/v1/territories?level=NUTS3&locale=ro  // Get all counties

// 2. What data is available for Sibiu county?
GET /api/v1/territories/45/available-data?locale=ro

// 3. Query population data for Sibiu
GET /api/v1/statistics/POP105A?territoryId=45&yearFrom=2020&yearTo=2024
```

---

## 4. Building a Matrix Explorer UI

### Suggested Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│  INS Data Explorer                                       [RO] [EN]       │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─ DISCOVER ──────────────────────────────────────────────────────────┐ │
│  │                                                                     │ │
│  │  [Topics ▼]    [Territory ▼]    [Time Range ▼]    [Search...]      │ │
│  │                                                                     │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌─ TOPICS ──────────────┐  ┌─ RESULTS ─────────────────────────────────┐│
│  │                       │  │                                           ││
│  │  ○ Population         │  │  Found 47 datasets matching filters       ││
│  │  ● Demographics       │  │                                           ││
│  │  ○ Economy            │  │  ┌─────────────────────────────────────┐  ││
│  │  ○ Labor Market       │  │  │  POP105A                            │  ││
│  │  ○ Education          │  │  │  Resident population by age & sex   │  ││
│  │  ○ Health             │  │  │                                     │  ││
│  │  ○ Construction       │  │  │  Years: 2003 - 2025                 │  ││
│  │  ○ Transport          │  │  │  Level: County (NUTS3)              │  ││
│  │  ○ Environment        │  │  │  Dimensions: 6                      │  ││
│  │  ○ Agriculture        │  │  │                                     │  ││
│  │                       │  │  │  [View Details]  [Quick Chart]      │  ││
│  │  ─────────────────    │  │  └─────────────────────────────────────┘  ││
│  │  Audience:            │  │                                           ││
│  │  ○ Researchers        │  │  ┌─────────────────────────────────────┐  ││
│  │  ○ Journalists        │  │  │  POP201A                            │  ││
│  │  ○ Public Admin       │  │  │  Vital statistics                   │  ││
│  │                       │  │  │  (births, deaths, marriages)        │  ││
│  │  ─────────────────    │  │  │                                     │  ││
│  │  Use Case:            │  │  │  Years: 1990 - 2024                 │  ││
│  │  ○ Time Series        │  │  │  Level: County (NUTS3)              │  ││
│  │  ○ Regional Data      │  │  │                                     │  ││
│  │  ○ Local Data         │  │  │  [View Details]  [Quick Chart]      │  ││
│  │                       │  │  └─────────────────────────────────────┘  ││
│  └───────────────────────┘  │                                           ││
│                             │  [Load More...]                           ││
│                             └───────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────┘
```

### Implementation Notes

**Topic Sidebar:**
```javascript
// Fetch topics on page load
const topics = await fetch('/api/v1/discover/topics?locale=en').then(r => r.json());

// Render by category
topics.data.categories.forEach(cat => {
  renderCategoryHeader(cat.category);  // "topic", "audience", "use-case"
  cat.tags.forEach(tag => {
    renderTagRadio(tag.name, tag.slug, tag.matrixCount);
  });
});
```

**Results Panel:**
```javascript
// When user selects a topic
async function onTopicSelected(slug) {
  const result = await fetch(`/api/v1/tags/${slug}?locale=en&limit=20`)
    .then(r => r.json());

  renderMatrixCards(result.data.matrices);
}
```

**Matrix Card Data:**
```javascript
// Each card shows:
{
  insCode: "POP105A",           // Matrix identifier
  name: "Resident population", // Localized name
  contextName: "1. POPULATION",// Parent context
  periodicity: ["ANNUAL"],     // Available periodicities
  startYear: 2003,             // Data range
  endYear: 2025,
  dimensionCount: 6            // Complexity indicator
}
```

---

## 5. Dimension Filters UI

### Filter Panel Design

```
┌─ FILTERS FOR: POP105A ─────────────────────────────────────────────────┐
│                                                                        │
│  Primary Filters (Always Visible)                                      │
│  ─────────────────────────────────────────────────────────────────────│
│                                                                        │
│  TERRITORY                              TIME PERIOD                    │
│  ┌────────────────────────────┐         ┌──────────────────────────┐   │
│  │ Level: [County      ▼]    │         │ From: [2020  ▼]          │   │
│  │                            │         │ To:   [2024  ▼]          │   │
│  │ ┌────────────────────────┐ │         │ Type: [Annual ▼]         │   │
│  │ │ □ All counties         │ │         └──────────────────────────┘   │
│  │ │ ☑ Sibiu                │ │                                        │
│  │ │ ☑ Cluj                 │ │         UNIT OF MEASURE                │
│  │ │ □ Brasov               │ │         ┌──────────────────────────┐   │
│  │ │ □ Timis                │ │         │ [Number of persons ▼]    │   │
│  │ │ □ Constanta            │ │         └──────────────────────────┘   │
│  │ │ ...                    │ │                                        │
│  │ └────────────────────────┘ │                                        │
│  │ Selected: 2 of 42          │                                        │
│  │ [Select All] [Clear]       │                                        │
│  └────────────────────────────┘                                        │
│                                                                        │
│  Classification Dimensions (Matrix-Specific)                           │
│  ─────────────────────────────────────────────────────────────────────│
│                                                                        │
│  SEX                    AGE GROUP                 RESIDENCE            │
│  ┌─────────────────┐    ┌─────────────────────┐   ┌─────────────────┐  │
│  │ ● Total         │    │ ● Total             │   │ ● Total         │  │
│  │ ○ Male          │    │ ○ 0-14 years        │   │ ○ Urban         │  │
│  │ ○ Female        │    │ ○ 15-64 years       │   │ ○ Rural         │  │
│  └─────────────────┘    │ ○ 65+ years         │   └─────────────────┘  │
│                         │ ○ [Show all 104...] │                        │
│                         └─────────────────────┘                        │
│                                                                        │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  [Reset All Filters]                    [Apply & View Data →]  │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### Building Dynamic Filters

**Step 1: Get matrix dimensions**

```javascript
const breakdowns = await fetch('/api/v1/matrices/POP105A/breakdowns?locale=en')
  .then(r => r.json());

// Response structure:
{
  "data": {
    "matrixCode": "POP105A",
    "dimensionCount": 6,
    "breakdowns": [
      {
        "dimIndex": 0,
        "label": "Age groups",
        "dimensionType": "CLASSIFICATION",
        "optionCount": 104,
        "classificationType": { "code": "AGE_GROUP", "name": "Age Groups" },
        "sampleValues": [
          { "nomItemId": 1, "label": "Total" },
          { "nomItemId": 2, "label": "0-4 years" }
        ]
      },
      {
        "dimIndex": 3,
        "label": "Territories",
        "dimensionType": "TERRITORIAL",
        "optionCount": 55
      }
    ],
    "byType": {
      "territorial": [...],
      "temporal": [...],
      "classification": [...],
      "unitOfMeasure": [...]
    }
  }
}
```

**Step 2: Render filters by type**

```javascript
function renderFilters(breakdowns) {
  const { byType } = breakdowns.data;

  // Always show territory filter if available
  if (byType.territorial.length > 0) {
    renderTerritoryFilter(byType.territorial[0]);
  }

  // Always show time filter if available
  if (byType.temporal.length > 0) {
    renderTimeFilter(byType.temporal[0]);
  }

  // Render classification filters dynamically
  byType.classification.forEach(dim => {
    renderClassificationFilter(dim);
  });

  // Unit of measure (usually single select)
  if (byType.unitOfMeasure.length > 0) {
    renderUnitFilter(byType.unitOfMeasure[0]);
  }
}
```

**Step 3: Get full options for a dimension**

```javascript
// When user clicks "Show all 104 age groups..."
const options = await fetch('/api/v1/matrices/POP105A/dimensions/0?locale=en')
  .then(r => r.json());

// Returns all options with nomItemIds needed for queries
{
  "data": {
    "options": [
      { "nomItemId": 1, "label": "Total", "parentId": null },
      { "nomItemId": 2, "label": "0-4 years", "parentId": null },
      { "nomItemId": 3, "label": "0 years", "parentId": 2 },  // Child of 0-4
      // ... 104 options
    ]
  }
}
```

### Filter UI Patterns by Dimension Type

| Type | UI Pattern | Behavior |
|------|------------|----------|
| **TERRITORIAL** | Hierarchical tree with checkboxes | Multi-select, drill-down |
| **TEMPORAL** | Year range picker (from/to) | Always range-based |
| **CLASSIFICATION** (few options) | Radio buttons | Single select for simplicity |
| **CLASSIFICATION** (many options) | Searchable dropdown | With "Show all" expansion |
| **UNIT_OF_MEASURE** | Simple dropdown | Usually single option |

### Important: Default Filter Values

By default, the API filters to "Total" values to avoid duplicating data:

```javascript
// Without filters - returns only TOTAL breakdowns (no duplication)
GET /api/v1/statistics/POP105A?yearFrom=2023&yearTo=2023

// With includeAll=true - returns ALL classification combinations
GET /api/v1/statistics/POP105A?yearFrom=2023&yearTo=2023&includeAll=true
// Warning: Can return 100x more rows!

// With specific filter - returns that breakdown only
GET /api/v1/statistics/POP105A?yearFrom=2023&yearTo=2023&classificationFilters={"SEX":["Male"]}
```

**Recommendation:** Start with defaults, let users drill down into breakdowns.

---

## 6. Territory Hierarchy Navigation

### The NUTS Hierarchy

```
NATIONAL (1)
    │
    └── NUTS1: Macroregions (4)
            │
            └── NUTS2: Development Regions (8)
                    │
                    └── NUTS3: Counties/Județe (42)
                            │
                            └── LAU: Communes/Cities (3,222)
```

### Territory Selector UI

```
┌─ SELECT TERRITORY ────────────────────────────────────────────────────┐
│                                                                       │
│  Breadcrumb: Romania > Macroregiunea 3 > Sud-Muntenia                 │
│              [↩ Back to Regions]                                      │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │                                                                 │  │
│  │  ☑ Sud-Muntenia (Region)                        [Select All]   │  │
│  │  │                                                             │  │
│  │  │  Counties in this region:                                   │  │
│  │  │                                                             │  │
│  │  ├── □ Argeș                           Pop: 612,431            │  │
│  │  ├── ☑ Călărași                        Pop: 289,523            │  │
│  │  ├── □ Dâmbovița                       Pop: 503,126            │  │
│  │  ├── ☑ Giurgiu                         Pop: 265,764            │  │
│  │  ├── □ Ialomița                        Pop: 264,528            │  │
│  │  ├── □ Prahova                         Pop: 720,314            │  │
│  │  └── □ Teleorman                       Pop: 348,219            │  │
│  │                                                                 │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  Selected: 2 counties (555,287 combined population)                   │
│                                                                       │
│  ┌──────────────────┐  ┌──────────────────────────────────────────┐   │
│  │ [Clear Selection]│  │ [Confirm Selection: 2 territories]      │   │
│  └──────────────────┘  └──────────────────────────────────────────┘   │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

### Implementation

**Step 1: Load territory levels**

```javascript
// Get all NUTS3 counties
const counties = await fetch('/api/v1/territories?level=NUTS3&locale=en')
  .then(r => r.json());

// Get children of a specific region
const regionChildren = await fetch('/api/v1/territories?parentId=5&locale=en')
  .then(r => r.json());
```

**Step 2: Build breadcrumb from territory details**

```javascript
const territory = await fetch('/api/v1/territories/45?locale=en')
  .then(r => r.json());

// Response includes path for breadcrumb
{
  "data": {
    "id": 45,
    "code": "SB",
    "name": "Sibiu",
    "level": "NUTS3",
    "path": "RO.4.7.45",  // Can parse for breadcrumb
    "children": []        // LAU children if any
  }
}
```

**Step 3: Use selected territories in query**

```javascript
// Single territory
GET /api/v1/statistics/POP105A?territoryId=45&yearFrom=2023&yearTo=2023

// Multiple territories (for comparison)
GET /api/v1/compare/territories?matrixCode=POP105A&territoryIds=45,23,31&year=2023

// All territories at a level
GET /api/v1/statistics/POP105A?territoryLevel=NUTS3&yearFrom=2023&yearTo=2023
```

### Territory Level Reference

| Level | Name | Count | Description |
|-------|------|-------|-------------|
| NATIONAL | Romania | 1 | Entire country |
| NUTS1 | Macroregions | 4 | Macroregiunea 1-4 |
| NUTS2 | Development Regions | 8 | Nord-Vest, Centru, etc. |
| NUTS3 | Counties (Județe) | 42 | Sibiu, Cluj, etc. |
| LAU | Local Units | 3,222 | Communes, cities |

---

## 7. Data Display Patterns

### Pattern A: Time Series Chart

**Use for:** Showing trends over time for a single territory

```
┌─ POPULATION TREND: Sibiu County ──────────────────────────────────────┐
│                                                                       │
│  420,000 ┤                                                            │
│          │         ●                                                  │
│  415,000 ┤        ╱ ╲                                                 │
│          │       ╱   ╲                                                │
│  410,000 ┤      ●     ●                                               │
│          │     ╱       ╲                                              │
│  405,000 ┤    ╱         ╲                                             │
│          │   ●           ●────●                                       │
│  400,000 ┤  ╱                                                         │
│          │ ●                                                          │
│  395,000 ┼──┬──────┬──────┬──────┬──────┬──────┬                       │
│          2019   2020   2021   2022   2023   2024                      │
│                                                                       │
│  Summary:                                                             │
│  • Start value (2019): 397,322                                        │
│  • End value (2024): 402,845                                          │
│  • Total change: +5,523 (+1.4%)                                       │
│  • YoY 2023→2024: +0.2%                                               │
│  • Trend direction: ↗ Slightly increasing                             │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

**API Endpoint:**

```javascript
const trend = await fetch(
  '/api/v1/trends/POP105A?territoryId=45&yearFrom=2019&yearTo=2024&locale=en'
).then(r => r.json());

// Response:
{
  "data": {
    "matrixCode": "POP105A",
    "territoryId": 45,
    "dataPoints": [
      { "year": 2019, "value": 397322, "yoyChange": null, "yoyChangePercent": null },
      { "year": 2020, "value": 410523, "yoyChange": 13201, "yoyChangePercent": 3.32 },
      // ... more years
    ],
    "trend": {
      "startYear": 2019,
      "endYear": 2024,
      "startValue": 397322,
      "endValue": 402845,
      "totalChange": 5523,
      "totalChangePercent": 1.39,
      "direction": "up"
    }
  }
}
```

---

### Pattern B: Territory Comparison Table

**Use for:** Comparing values across multiple regions

```
┌─ COMPARE COUNTIES (2023) ─────────────────────────────────────────────┐
│                                                                       │
│  Indicator: Resident Population                                       │
│  Year: 2023                                                           │
│                                                                       │
│  ┌────────────┬────────────┬──────┬───────────────┬────────────┐      │
│  │ County     │ Population │ Rank │ % of National │ YoY Change │      │
│  ├────────────┼────────────┼──────┼───────────────┼────────────┤      │
│  │ București  │  1,821,312 │   1  │     9.56%     │   -0.31%   │      │
│  │ Cluj       │    729,284 │   2  │     3.83%     │   +0.52%   │      │
│  │ Timiș      │    702,156 │   3  │     3.69%     │   +0.21%   │      │
│  │ Iași       │    698,423 │   4  │     3.67%     │   -0.42%   │      │
│  │ Constanța  │    643,219 │   5  │     3.38%     │   -0.58%   │      │
│  │ ────────── │ ────────── │ ──── │ ───────────── │ ────────── │      │
│  │ Selected   │  4,594,394 │   -  │    24.12%     │   -0.12%   │      │
│  │ Romania    │ 19,055,228 │   -  │   100.00%     │   -0.64%   │      │
│  └────────────┴────────────┴──────┴───────────────┴────────────┘      │
│                                                                       │
│  [Export CSV]  [Export Excel]  [Add to Comparison]                    │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

**API Endpoints:**

```javascript
// For rankings (all counties ranked)
const rankings = await fetch(
  '/api/v1/rankings/POP105A?year=2023&territoryLevel=NUTS3&locale=en'
).then(r => r.json());

// For specific comparison (selected territories)
const comparison = await fetch(
  '/api/v1/compare/territories?matrixCode=POP105A&territoryIds=10,23,42,31,15&year=2023&locale=en'
).then(r => r.json());
```

---

### Pattern C: Pivot Table

**Use for:** Cross-tabulating two dimensions

```
┌─ POPULATION BY AGE GROUP AND SEX (Romania, 2023) ─────────────────────┐
│                                                                       │
│              │     Male    │    Female   │    Total    │   % Total   │
│  ────────────┼─────────────┼─────────────┼─────────────┼─────────────│
│  0-14 years  │   1,523,412 │   1,445,623 │   2,969,035 │    15.58%   │
│  15-64 years │   6,234,521 │   6,012,345 │  12,246,866 │    64.27%   │
│  65+ years   │   1,623,456 │   2,215,871 │   3,839,327 │    20.15%   │
│  ────────────┼─────────────┼─────────────┼─────────────┼─────────────│
│  TOTAL       │   9,381,389 │   9,673,839 │  19,055,228 │   100.00%   │
│  % by Sex    │     49.23%  │     50.77%  │             │             │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

**API Endpoint:**

```javascript
const pivot = await fetch(
  '/api/v1/pivot/POP105A?rowDimension=classification&colDimension=classification&territoryId=1&yearFrom=2023&yearTo=2023&locale=en'
).then(r => r.json());

// Response:
{
  "data": {
    "matrixCode": "POP105A",
    "rowDimension": "classification",
    "colDimension": "classification",
    "rows": [
      { "key": "0-14", "label": "0-14 years" },
      { "key": "15-64", "label": "15-64 years" },
      { "key": "65+", "label": "65+ years" }
    ],
    "columns": [
      { "key": "M", "label": "Male" },
      { "key": "F", "label": "Female" },
      { "key": "T", "label": "Total" }
    ],
    "values": {
      "0-14": { "M": 1523412, "F": 1445623, "T": 2969035 },
      "15-64": { "M": 6234521, "F": 6012345, "T": 12246866 },
      "65+": { "M": 1623456, "F": 2215871, "T": 3839327 }
    }
  }
}
```

---

### Pattern D: Distribution Statistics

**Use for:** Understanding data spread across territories

```
┌─ POPULATION DISTRIBUTION BY COUNTY (2023) ────────────────────────────┐
│                                                                       │
│  Statistical Summary                                                  │
│  ─────────────────────────────────────────────────────────────────── │
│                                                                       │
│  Count:        42 counties                                            │
│  Total:        19,055,228                                             │
│  Mean:         453,696                                                │
│  Median:       380,245                                                │
│  Std Dev:      312,456                                                │
│                                                                       │
│  Range:                                                               │
│  • Minimum:    212,456 (Covasna)                                      │
│  • Maximum:    1,821,312 (București)                                  │
│                                                                       │
│  Percentiles:                                                         │
│  • P10:        256,000                                                │
│  • P25 (Q1):   298,000                                                │
│  • P50 (Q2):   380,245                                                │
│  • P75 (Q3):   523,000                                                │
│  • P90:        698,000                                                │
│                                                                       │
│  IQR: 225,000                                                         │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

**API Endpoint:**

```javascript
const distribution = await fetch(
  '/api/v1/distribution/POP105A?year=2023&territoryLevel=NUTS3&locale=en'
).then(r => r.json());

// Response:
{
  "data": {
    "matrixCode": "POP105A",
    "year": 2023,
    "territoryLevel": "NUTS3",
    "distribution": {
      "count": 42,
      "nonNullCount": 42,
      "nullCount": 0,
      "mean": 453696,
      "stddev": 312456,
      "min": 212456,
      "max": 1821312,
      "range": 1608856,
      "percentiles": {
        "p10": 256000,
        "p25": 298000,
        "p50": 380245,
        "p75": 523000,
        "p90": 698000
      },
      "iqr": 225000
    }
  }
}
```

---

## 8. API Workflow Examples

### Example 1: Build a Population Dashboard

**Goal:** Display Romania's population with breakdown by region

```javascript
async function buildPopulationDashboard() {
  const BASE_URL = '/api/v1';
  const YEAR = 2023;

  // 1. Get national total
  const nationalTrend = await fetch(
    `${BASE_URL}/trends/POP105A?yearFrom=${YEAR-4}&yearTo=${YEAR}`
  ).then(r => r.json());

  // 2. Get regional breakdown (NUTS2 level)
  const regionalRanking = await fetch(
    `${BASE_URL}/rankings/POP105A?year=${YEAR}&territoryLevel=NUTS2&locale=en`
  ).then(r => r.json());

  // 3. Get distribution statistics
  const distribution = await fetch(
    `${BASE_URL}/distribution/POP105A?year=${YEAR}&territoryLevel=NUTS3`
  ).then(r => r.json());

  return {
    headline: {
      value: nationalTrend.data.dataPoints.find(d => d.year === YEAR).value,
      trend: nationalTrend.data.trend.direction,
      change: nationalTrend.data.trend.totalChangePercent
    },
    regions: regionalRanking.data.map(r => ({
      name: r.territory.name,
      value: r.value,
      percentile: r.percentile
    })),
    stats: distribution.data.distribution
  };
}
```

---

### Example 2: Regional Comparison Widget

**Goal:** Compare selected counties side-by-side

```javascript
async function compareCounties(countyIds, year = 2023) {
  const BASE_URL = '/api/v1';

  // 1. Get comparison data
  const comparison = await fetch(
    `${BASE_URL}/compare/territories?` +
    `matrixCode=POP105A&` +
    `territoryIds=${countyIds.join(',')}&` +
    `year=${year}&locale=en`
  ).then(r => r.json());

  // 2. Get trend for each county
  const trends = await Promise.all(
    countyIds.map(id =>
      fetch(`${BASE_URL}/trends/POP105A?territoryId=${id}&yearFrom=${year-5}&yearTo=${year}`)
        .then(r => r.json())
    )
  );

  // 3. Combine data for display
  return comparison.data.territories.map((territory, i) => ({
    ...territory,
    trend: trends[i].data.trend
  }));
}

// Usage
const data = await compareCounties([45, 23, 31]); // Sibiu, Cluj, Brașov
```

---

### Example 3: Time Series with Multiple Indicators

**Goal:** Show population and birth rate trends together

```javascript
async function multiIndicatorChart(territoryId, yearFrom, yearTo) {
  const BASE_URL = '/api/v1';

  // Fetch both indicators in parallel
  const [population, birthRate] = await Promise.all([
    fetch(`${BASE_URL}/trends/POP105A?territoryId=${territoryId}&yearFrom=${yearFrom}&yearTo=${yearTo}`)
      .then(r => r.json()),
    fetch(`${BASE_URL}/indicators/birth-rate?territoryId=${territoryId}&year=${yearTo}`)
      .then(r => r.json())
  ]);

  return {
    population: {
      data: population.data.dataPoints,
      trend: population.data.trend
    },
    birthRate: birthRate.data.calculation
  };
}
```

---

## 9. Pagination & Performance

### Cursor-Based Pagination

All list endpoints use cursor-based pagination for efficient data retrieval:

```javascript
// First page
GET /api/v1/matrices?limit=20

// Response includes cursor for next page
{
  "data": [...],
  "meta": {
    "pagination": {
      "cursor": "eyJpZCI6MjB9",  // Base64 encoded cursor
      "hasMore": true,
      "limit": 20
    }
  }
}

// Next page
GET /api/v1/matrices?limit=20&cursor=eyJpZCI6MjB9
```

### Implementing Infinite Scroll

```javascript
async function loadMatrices(cursor = null) {
  const url = new URL('/api/v1/matrices', BASE_URL);
  url.searchParams.set('limit', '20');
  url.searchParams.set('locale', 'en');
  if (cursor) {
    url.searchParams.set('cursor', cursor);
  }

  const response = await fetch(url).then(r => r.json());

  return {
    items: response.data,
    nextCursor: response.meta.pagination.cursor,
    hasMore: response.meta.pagination.hasMore
  };
}

// Usage with React/Vue infinite scroll
let cursor = null;
let hasMore = true;

async function loadMore() {
  if (!hasMore) return;

  const result = await loadMatrices(cursor);
  appendToList(result.items);
  cursor = result.nextCursor;
  hasMore = result.hasMore;
}
```

### Caching Recommendations

| Data Type | Cache Duration | Reason |
|-----------|----------------|--------|
| Contexts | 24 hours | Rarely changes |
| Matrix list | 1 hour | New matrices added occasionally |
| Matrix details | 1 hour | Metadata stable |
| Dimension options | 1 hour | Dimension values stable |
| Statistics | 5 minutes | May be updated by sync |
| Rankings/Trends | 5 minutes | Computed from statistics |

### Avoiding N+1 Queries

**Bad Pattern:**
```javascript
// DON'T: Fetch details for each matrix individually
const matrices = await fetch('/api/v1/matrices?limit=50').then(r => r.json());
for (const matrix of matrices.data) {
  const details = await fetch(`/api/v1/matrices/${matrix.insCode}`).then(r => r.json());
  // N+1 problem!
}
```

**Good Pattern:**
```javascript
// DO: Use the list endpoint which includes summary data
const matrices = await fetch('/api/v1/matrices?limit=50').then(r => r.json());
// Summary data already included: name, yearRange, dimensionCount, periodicity

// Only fetch details when user clicks on a specific matrix
function onMatrixClick(insCode) {
  const details = await fetch(`/api/v1/matrices/${insCode}`).then(r => r.json());
}
```

---

## 10. Error Handling

### Error Response Format

All errors follow a consistent format:

```json
{
  "error": "ERROR_CODE",
  "message": "Human-readable description",
  "details": { ... },
  "requestId": "req-abc123"
}
```

### Common Error Codes

| Code | HTTP Status | Meaning | Solution |
|------|-------------|---------|----------|
| `NOT_FOUND` | 404 | Resource doesn't exist | Check matrix code or ID |
| `VALIDATION_ERROR` | 400 | Invalid parameters | Check required params |
| `INTERNAL_ERROR` | 500 | Server error | Retry or report bug |

### Handling Validation Errors

```javascript
async function queryStatistics(params) {
  const response = await fetch(`/api/v1/statistics/POP105A?${new URLSearchParams(params)}`);

  if (!response.ok) {
    const error = await response.json();

    if (error.error === 'VALIDATION_ERROR') {
      // Show specific field errors to user
      error.details.validation.forEach(v => {
        showFieldError(v.instancePath, v.message);
      });
      return null;
    }

    if (error.error === 'NOT_FOUND') {
      showNotification('Matrix not found', 'error');
      return null;
    }

    // Generic error
    showNotification('An error occurred', 'error');
    console.error('API Error:', error);
    return null;
  }

  return response.json();
}
```

### Empty Results vs Errors

**Empty results are NOT errors.** The API returns empty arrays with 200 OK:

```javascript
// No data for this filter - returns empty array, not error
GET /api/v1/statistics/POP105A?yearFrom=2050&yearTo=2050

// Response: 200 OK
{
  "data": {
    "dataPoints": []
  }
}
```

---

## 11. Best Practices

### 1. Always Use `locale` for User-Facing Text

```javascript
// Good - text will be in user's language
const data = await fetch('/api/v1/matrices/POP105A?locale=en').then(r => r.json());
displayName(data.data.name); // "Resident population..."

// Bad - always Romanian, ignores user preference
const data = await fetch('/api/v1/matrices/POP105A').then(r => r.json());
```

### 2. Use Default Filters to Avoid Data Duplication

```javascript
// Good - returns only total values (no duplication)
GET /api/v1/statistics/POP105A?yearFrom=2023&yearTo=2023

// Careful - returns ALL classification combinations (can be huge!)
GET /api/v1/statistics/POP105A?yearFrom=2023&yearTo=2023&includeAll=true
```

### 3. Cache Matrix Metadata

```javascript
// Matrix metadata rarely changes - cache it
const matrixCache = new Map();

async function getMatrixDetails(code) {
  if (!matrixCache.has(code)) {
    const data = await fetch(`/api/v1/matrices/${code}?locale=en`).then(r => r.json());
    matrixCache.set(code, data);
  }
  return matrixCache.get(code);
}
```

### 4. Pre-fetch Dimension Options for Filter UIs

```javascript
// On matrix selection, pre-fetch all dimension options
async function onMatrixSelected(code) {
  const breakdowns = await fetch(`/api/v1/matrices/${code}/breakdowns?locale=en`)
    .then(r => r.json());

  // Pre-fetch large dimensions in background
  breakdowns.data.breakdowns
    .filter(d => d.optionCount > 20)
    .forEach(dim => {
      prefetch(`/api/v1/matrices/${code}/dimensions/${dim.dimIndex}?locale=en`);
    });
}
```

### 5. Use Analytics Endpoints Instead of Client-Side Calculations

```javascript
// Bad - fetching all data and calculating client-side
const allData = await fetch('/api/v1/statistics/POP105A?yearFrom=2010&yearTo=2024&includeAll=true');
const trend = calculateTrendClientSide(allData); // Expensive!

// Good - let the server do the math
const trend = await fetch('/api/v1/trends/POP105A?yearFrom=2010&yearTo=2024');
```

### 6. Handle Missing Data Gracefully

```javascript
// Some data points may be null or have special status
function formatValue(dataPoint) {
  if (dataPoint.value === null) {
    if (dataPoint.valueStatus === ':') {
      return 'Not available';
    }
    if (dataPoint.valueStatus === 'c') {
      return 'Confidential';
    }
    return '-';
  }
  return formatNumber(dataPoint.value);
}
```

---

## 12. Appendix

### A. Territory Levels Reference

| Level | Code | Count | Description | Example |
|-------|------|-------|-------------|---------|
| National | NATIONAL | 1 | Entire country | Romania (TOTAL) |
| Macroregion | NUTS1 | 4 | Large regions | Macroregiunea Unu |
| Development Region | NUTS2 | 8 | Planning regions | Nord-Vest, Centru |
| County | NUTS3 | 42 | Administrative counties | Sibiu, Cluj, București |
| Local Unit | LAU | 3,222 | Cities & communes | Sibiu city, Rășinari |

### B. Common Classification Types

| Code | Name | Values | Description |
|------|------|--------|-------------|
| SEX | Sex | Total, Male, Female | Gender breakdown |
| RESIDENCE | Residence | Total, Urban, Rural | Urban/rural split |
| AGE_GROUP | Age Groups | 104 options | Detailed age brackets |
| OWNERSHIP | Ownership | Public, Private, etc. | Ownership type |
| SIZE_CLASS | Size Class | By employee count | Business size |
| ECONOMIC_ACTIVITY | CAEN | Hierarchical | Economic sectors |

### C. Periodicity Types

| Code | Description | Example Label |
|------|-------------|---------------|
| ANNUAL | Yearly data | "Year 2023" |
| QUARTERLY | Quarterly data | "Q2 2023" |
| MONTHLY | Monthly data | "June 2023" |

### D. API Quick Reference

#### Discovery
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/discover/topics` | Browse by topic/tag |
| GET | `/discover/by-territory` | Find data for location |
| GET | `/discover/by-time-range` | Find data by time period |
| GET | `/discover/by-breakdowns` | Find by dimension type |

#### Contexts
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/contexts` | List domain hierarchy |
| GET | `/contexts/:id` | Get context details |

#### Matrices
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/matrices` | Search/list matrices |
| GET | `/matrices/:code` | Get matrix details |
| GET | `/matrices/:code/breakdowns` | Get dimensions |
| GET | `/matrices/:code/data-coverage` | Get data availability |
| GET | `/matrices/:code/dimensions/:idx` | Get dimension options |

#### Statistics
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/statistics/:code` | Query time series data |
| GET | `/statistics/:code/summary` | Get aggregated summary |

#### Territories
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/territories` | List territories |
| GET | `/territories/:id` | Get territory details |
| GET | `/territories/:id/available-data` | Find data for territory |

#### Analytics
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/trends/:code` | Time series with YoY |
| GET | `/rankings/:code` | Territory rankings |
| GET | `/distribution/:code` | Statistical distribution |
| GET | `/compare/territories` | Compare territories |
| GET | `/aggregate/:code` | Hierarchical aggregation |
| GET | `/correlate` | Cross-matrix correlation |
| GET | `/pivot/:code` | Pivot table format |

#### Tags
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tags` | List all tags |
| GET | `/tags/:slug` | Get tag with matrices |
| GET | `/tags/:slug/related` | Get related tags |

#### Indicators
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/indicators` | List composite indicators |
| GET | `/indicators/:code` | Calculate indicator |

#### Saved Queries
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/queries` | List saved queries |
| POST | `/queries` | Create saved query |
| GET | `/queries/:id` | Get query details |
| GET | `/queries/:id/execute` | Execute saved query |
| DELETE | `/queries/:id` | Delete saved query |

---

## Need Help?

- **API Issues:** Check error response for details
- **Missing Data:** Use `/matrices/:code/data-coverage` to check availability
- **Performance:** Review caching and pagination recommendations

---

*This documentation is for API version 1.0. Last updated: December 2024.*
