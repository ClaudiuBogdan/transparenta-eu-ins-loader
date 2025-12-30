# INS Statistical Data API - Query & Exploration Specification

> **Status:** Specification Document
> **Version:** 1.0
> **Last Updated:** 2024-12-30
> **Purpose:** Reference for future API implementation

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Current API Overview](#current-api-overview)
3. [Part 1: Data Discovery Perspectives](#part-1-data-discovery-perspectives)
4. [Part 2: Question-Driven API Queries](#part-2-question-driven-api-queries)
5. [Part 3: Advanced Query Patterns](#part-3-advanced-query-patterns)
6. [Part 4: Schema Improvements](#part-4-schema-improvements)
7. [Part 5: New Endpoint Summary](#part-5-new-endpoint-summary)
8. [Part 6: Implementation Priority](#part-6-implementation-priority)
9. [Appendix: SQL Query Reference](#appendix-sql-query-reference)

---

## Executive Summary

This document specifies useful ways to query and explore Romanian statistical data (INS - Institutul Național de Statistică) from different perspectives. It covers:

- **API query designs** for different user needs
- **SQL patterns** for data extraction
- **Schema improvements** for flexibility and discoverability

### Target Audiences

| Audience | Needs | API Complexity |
|----------|-------|----------------|
| **Developers** | Building applications, raw data access | High flexibility |
| **Data Analysts** | Querying for insights, aggregations | Medium complexity |
| **Public Portal Users** | Simple discovery, pre-built queries | Low complexity |

### API Layering Strategy

| Layer | Audience | Complexity | Example Endpoints |
|-------|----------|------------|-------------------|
| **Simple** | Public users | Low | `/discover/topics`, `/popular`, `/quick-stats` |
| **Standard** | Analysts | Medium | `/statistics`, `/compare`, `/rankings` |
| **Advanced** | Developers | High | `/pivot`, `/correlate`, raw dimension queries |

---

## Current API Overview

### Existing Endpoints (`/api/v1`)

| Category | Endpoint | Purpose |
|----------|----------|---------|
| **Discovery** | `GET /contexts` | Browse hierarchical domains |
| **Discovery** | `GET /matrices` | Search/list statistical datasets |
| **Discovery** | `GET /matrices/:code` | Get matrix details with dimensions |
| **Dimensions** | `GET /territories` | Browse territorial hierarchy |
| **Dimensions** | `GET /time-periods` | List available time periods |
| **Dimensions** | `GET /classifications` | Explore classification systems |
| **Data** | `GET /statistics/:matrixCode` | Query statistical data |
| **Data** | `GET /statistics/:matrixCode/summary` | Aggregated summary |

### Database Schema Reference

```
contexts                    # 8 domains → ~340 subcategories
    ↓
matrices                    # ~1,898 statistical datasets
    ↓
matrix_dimensions           # 3-6 dimensions per matrix
    ↓
matrix_dimension_options    # Maps nomItemId → reference tables
    ├── territory_id        → territories (NUTS + LAU/SIRUTA)
    ├── time_period_id      → time_periods (annual/quarterly/monthly)
    ├── classification_id   → classification_values
    └── unit_of_measure_id  → units_of_measure

statistics                  # Fact table (partitioned by matrix_id)
    ├── territory_id
    ├── time_period_id
    ├── unit_of_measure_id
    └── statistic_classifications (junction table)
```

---

## Part 1: Data Discovery Perspectives

### 1.1 By Subject Area (What data exists?)

**Use Case:** Users browse available datasets by topic (population, economy, education).

**Current State:** Context hierarchy with 8 domains → ~340 subcategories → ~1,898 matrices

**Proposed Enhancement:**

```http
GET /api/v1/discover/topics
```

**Response:**
```json
{
  "topics": [
    {
      "id": 1,
      "code": "1",
      "name": "STATISTICA SOCIALA",
      "nameEn": "SOCIAL STATISTICS",
      "matrixCount": 523,
      "totalViews": 1250000,
      "topMatrix": {
        "code": "POP105A",
        "name": "Populatia pe sexe si varste",
        "views": 45000
      },
      "children": [
        {
          "id": 10,
          "code": "1010",
          "name": "POPULATIE",
          "matrixCount": 87
        }
      ]
    }
  ]
}
```

**SQL Pattern:**
```sql
SELECT
    c.id,
    c.ins_code,
    c.name,
    c.name_en,
    c.level,
    COUNT(DISTINCT m.id) AS matrix_count,
    SUM(m.view_count) AS total_views,
    (SELECT ins_code FROM matrices
     WHERE context_id = c.id
     ORDER BY view_count DESC LIMIT 1) AS top_matrix
FROM contexts c
LEFT JOIN matrices m ON m.context_id = c.id
GROUP BY c.id
ORDER BY c.path;
```

---

### 1.2 By Geographic Coverage (Where is data available?)

**Use Case:** Users want datasets for specific territories (county, city, village).

**Current State:** `has_county_data`, `has_uat_data` boolean flags on matrices

**Proposed Enhancements:**

```http
GET /api/v1/discover/by-territory?level=LAU&sirutaCode=143450
GET /api/v1/territories/:id/available-data
```

**Response for `/territories/:id/available-data`:**
```json
{
  "territory": {
    "id": 1234,
    "code": "143450",
    "name": "MUNICIPIUL SIBIU",
    "level": "LAU"
  },
  "availableMatrices": [
    {
      "code": "POP105A",
      "name": "Populatia pe sexe si varste",
      "dataFromYear": 2002,
      "dataToYear": 2023,
      "dataPointCount": 2156
    }
  ],
  "relatedTerritories": [
    {
      "id": 42,
      "code": "SB",
      "name": "SIBIU",
      "level": "NUTS3",
      "additionalMatrices": 45
    }
  ]
}
```

**SQL Pattern:**
```sql
-- Find matrices with data for a specific territory
SELECT DISTINCT
    m.ins_code,
    m.name,
    m.name_en,
    MIN(tp.year) AS data_from_year,
    MAX(tp.year) AS data_to_year,
    COUNT(*) AS data_point_count
FROM statistics s
JOIN matrices m ON s.matrix_id = m.id
JOIN time_periods tp ON s.time_period_id = tp.id
WHERE s.territory_id = :territory_id
GROUP BY m.id
ORDER BY data_point_count DESC;
```

---

### 1.3 By Time Coverage (When is data available?)

**Use Case:** Users want datasets with historical depth or recent data.

**Current State:** `start_year`, `end_year` on matrices (from INS metadata, not actual synced data)

**Proposed Enhancements:**

```http
GET /api/v1/discover/by-time-range?from=2010&to=2024&periodicity=ANNUAL
GET /api/v1/matrices/:code/data-coverage
```

**Response for `/matrices/:code/data-coverage`:**
```json
{
  "matrix": {
    "code": "POP105A",
    "name": "Populatia pe sexe si varste"
  },
  "timeCoverage": {
    "firstYear": 1992,
    "lastYear": 2023,
    "yearsAvailable": [1992, 1993, ..., 2023],
    "gaps": [],
    "completeness": 100
  },
  "territoryCoverage": {
    "national": true,
    "nuts1": true,
    "nuts2": true,
    "nuts3": true,
    "lau": true,
    "territoriesCount": 3265
  },
  "periodicity": ["ANNUAL"]
}
```

**SQL Pattern:**
```sql
-- Find matrices with complete annual data coverage 2010-2024
WITH year_coverage AS (
    SELECT
        m.id,
        m.ins_code,
        m.name,
        COUNT(DISTINCT tp.year) AS years_covered,
        ARRAY_AGG(DISTINCT tp.year ORDER BY tp.year) AS years_available
    FROM matrices m
    JOIN statistics s ON s.matrix_id = m.id
    JOIN time_periods tp ON s.time_period_id = tp.id
    WHERE tp.periodicity = 'ANNUAL'
      AND tp.year BETWEEN 2010 AND 2024
    GROUP BY m.id
)
SELECT * FROM year_coverage
WHERE years_covered = 15  -- Complete coverage
ORDER BY ins_code;
```

---

### 1.4 By Demographic Dimensions (Who is the data about?)

**Use Case:** Users want datasets that can be broken down by sex, age, education, etc.

**Current State:** Classification types exist but not easily discoverable per matrix

**Proposed Enhancements:**

```http
GET /api/v1/discover/by-breakdowns?dimensions=SEX,AGE_GROUP
GET /api/v1/matrices/:code/breakdowns
```

**Response for `/matrices/:code/breakdowns`:**
```json
{
  "matrix": {
    "code": "POP105A",
    "name": "Populatia pe sexe si varste"
  },
  "breakdowns": [
    {
      "code": "SEX",
      "name": "Sexe",
      "options": ["TOTAL", "M", "F"],
      "isHierarchical": false
    },
    {
      "code": "AGE_GROUP",
      "name": "Grupe de varsta",
      "options": ["TOTAL", "0-4", "5-9", ...],
      "isHierarchical": true
    }
  ],
  "combinations": {
    "totalPossible": 156,
    "example": "Sex × Age Group = 3 × 52 = 156 combinations"
  }
}
```

**SQL Pattern:**
```sql
-- Find matrices with specific classification dimensions
SELECT
    m.ins_code,
    m.name,
    ARRAY_AGG(DISTINCT ct.code) AS classification_codes,
    ARRAY_AGG(DISTINCT ct.name) AS classification_names
FROM matrices m
JOIN matrix_dimensions md ON md.matrix_id = m.id
JOIN classification_types ct ON md.classification_type_id = ct.id
WHERE md.dimension_type = 'CLASSIFICATION'
GROUP BY m.id
HAVING ARRAY['SEX', 'AGE_GROUP'] <@ ARRAY_AGG(ct.code);
```

---

### 1.5 By Data Granularity (How detailed is the data?)

**Use Case:** Users want the most granular data available (locality-level, monthly, detailed breakdowns).

**Proposed Enhancement:**

```http
GET /api/v1/discover/granularity-matrix
```

**Response:**
```json
{
  "matrices": [
    {
      "code": "POP107D",
      "name": "Populatia pe localitati",
      "granularity": {
        "territorial": "LAU",
        "temporal": "ANNUAL",
        "dimensions": 4,
        "classificationOptions": 12
      },
      "score": 95  // Granularity score
    }
  ],
  "summary": {
    "withLocalityData": 234,
    "withCountyData": 1456,
    "withMonthlyData": 89,
    "withQuarterlyData": 156
  }
}
```

**SQL Pattern:**
```sql
SELECT
    m.ins_code,
    m.name,
    m.has_uat_data,
    m.has_county_data,
    m.periodicity,
    m.dimension_count,
    (SELECT COUNT(DISTINCT cv.id)
     FROM matrix_dimensions md
     JOIN matrix_dimension_options mdo ON mdo.matrix_dimension_id = md.id
     JOIN classification_values cv ON mdo.classification_value_id = cv.id
     WHERE md.matrix_id = m.id AND md.dimension_type = 'CLASSIFICATION'
    ) AS classification_options_count
FROM matrices m
ORDER BY
    m.has_uat_data DESC,
    m.has_county_data DESC,
    m.dimension_count DESC;
```

---

## Part 2: Question-Driven API Queries

### 2.1 Comparative Questions

**Q: How does X compare across territories?**

```http
GET /api/v1/compare/territories
    ?matrixCode=POP105A
    &territories=BH,CJ,TM
    &year=2023
    &classification[SEX]=TOTAL
```

**Response:**
```json
{
  "matrix": { "code": "POP105A", "name": "Populatia pe sexe si varste" },
  "year": 2023,
  "filters": { "SEX": "TOTAL" },
  "comparison": [
    { "territory": "BH", "name": "Bihor", "value": 575400, "rank": 2 },
    { "territory": "CJ", "name": "Cluj", "value": 713500, "rank": 1 },
    { "territory": "TM", "name": "Timis", "value": 705800, "rank": 3 }
  ],
  "statistics": {
    "min": 575400,
    "max": 713500,
    "mean": 664900,
    "range": 138100
  }
}
```

---

**Q: How has X changed over time?**

```http
GET /api/v1/trends/:matrixCode
    ?territoryCode=RO
    &yearFrom=2000
    &yearTo=2024
    &metric=growth_rate
```

**Response:**
```json
{
  "matrix": { "code": "POP105A" },
  "territory": { "code": "RO", "name": "ROMANIA" },
  "metric": "growth_rate",
  "data": [
    { "year": 2000, "value": 22435000, "growthRate": null },
    { "year": 2001, "value": 22329000, "growthRate": -0.47 },
    { "year": 2002, "value": 22223000, "growthRate": -0.47 },
    ...
  ],
  "summary": {
    "startValue": 22435000,
    "endValue": 19053000,
    "totalChange": -3382000,
    "totalChangePercent": -15.08,
    "averageAnnualGrowth": -0.68,
    "trend": "declining"
  }
}
```

**SQL Pattern - Growth Rate:**
```sql
WITH yearly_data AS (
    SELECT
        tp.year,
        s.value,
        LAG(s.value) OVER (ORDER BY tp.year) AS prev_value
    FROM statistics s
    JOIN time_periods tp ON s.time_period_id = tp.id
    WHERE s.matrix_id = :matrix_id
      AND s.territory_id = :territory_id
      AND tp.periodicity = 'ANNUAL'
)
SELECT
    year,
    value,
    prev_value,
    CASE WHEN prev_value > 0
         THEN ROUND(((value - prev_value) / prev_value) * 100, 2)
         ELSE NULL
    END AS growth_rate_pct
FROM yearly_data
ORDER BY year;
```

---

### 2.2 Ranking Questions

**Q: Which territories rank highest/lowest for X?**

```http
GET /api/v1/rankings/:matrixCode
    ?year=2023
    &territoryLevel=NUTS3
    &limit=10
    &order=desc
```

**Response:**
```json
{
  "matrix": { "code": "POP105A" },
  "year": 2023,
  "territoryLevel": "NUTS3",
  "order": "desc",
  "rankings": [
    { "rank": 1, "territory": "B", "name": "Bucuresti", "value": 1883400, "percentile": 100 },
    { "rank": 2, "territory": "CJ", "name": "Cluj", "value": 713500, "percentile": 97.6 },
    { "rank": 3, "territory": "TM", "name": "Timis", "value": 705800, "percentile": 95.2 },
    ...
  ],
  "statistics": {
    "total": 42,
    "min": 210400,
    "max": 1883400,
    "median": 385200,
    "mean": 453650
  }
}
```

---

**Q: Where does territory X rank?**

```http
GET /api/v1/rankings/:matrixCode/position
    ?territoryCode=BH
    &year=2023
    &compareLevel=NUTS3
```

**Response:**
```json
{
  "territory": { "code": "BH", "name": "Bihor" },
  "value": 575400,
  "ranking": {
    "position": 8,
    "total": 42,
    "percentile": 81.0,
    "aboveAverage": true
  },
  "neighbors": {
    "above": { "code": "IS", "name": "Iasi", "value": 783200 },
    "below": { "code": "PH", "name": "Prahova", "value": 556900 }
  }
}
```

**SQL Pattern - Ranking:**
```sql
WITH ranked AS (
    SELECT
        t.id,
        t.code,
        t.name,
        s.value,
        ROW_NUMBER() OVER (ORDER BY s.value DESC) AS rank_desc,
        ROW_NUMBER() OVER (ORDER BY s.value ASC) AS rank_asc,
        PERCENT_RANK() OVER (ORDER BY s.value DESC) AS percentile
    FROM statistics s
    JOIN territories t ON s.territory_id = t.id
    JOIN time_periods tp ON s.time_period_id = tp.id
    WHERE s.matrix_id = :matrix_id
      AND tp.year = :year
      AND t.level = :territory_level
)
SELECT * FROM ranked ORDER BY rank_desc;
```

---

### 2.3 Distribution Questions

**Q: What is the distribution of X across territories?**

```http
GET /api/v1/distribution/:matrixCode
    ?year=2023
    &groupBy=territory
    &territoryLevel=NUTS3
    &buckets=quartiles
```

**Response:**
```json
{
  "matrix": { "code": "POP105A" },
  "year": 2023,
  "distribution": {
    "count": 42,
    "min": 210400,
    "max": 1883400,
    "mean": 453650,
    "median": 385200,
    "stdDev": 285300,
    "q1": 298400,
    "q3": 575400,
    "iqr": 277000
  },
  "quartiles": [
    { "quartile": 1, "range": "210K - 298K", "count": 11, "territories": ["CS", "CV", ...] },
    { "quartile": 2, "range": "298K - 385K", "count": 10, "territories": ["GJ", "HR", ...] },
    { "quartile": 3, "range": "385K - 575K", "count": 11, "territories": ["BC", "DJ", ...] },
    { "quartile": 4, "range": "575K - 1.9M", "count": 10, "territories": ["CJ", "IS", ...] }
  ],
  "histogram": {
    "buckets": [
      { "from": 200000, "to": 400000, "count": 21 },
      { "from": 400000, "to": 600000, "count": 12 },
      { "from": 600000, "to": 800000, "count": 5 },
      { "from": 800000, "to": 2000000, "count": 4 }
    ]
  }
}
```

**SQL Pattern - Distribution:**
```sql
SELECT
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    AVG(value) AS mean,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS median,
    STDDEV(value) AS std_dev,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value) AS q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value) AS q3
FROM statistics s
JOIN time_periods tp ON s.time_period_id = tp.id
WHERE s.matrix_id = :matrix_id
  AND tp.year = :year
  AND s.value IS NOT NULL;
```

---

### 2.4 Correlation Questions

**Q: Is there a relationship between X and Y?**

```http
GET /api/v1/correlate
    ?matrix1=POP105A
    &matrix2=ECO101B
    &territoryLevel=NUTS3
    &year=2023
```

**Response:**
```json
{
  "matrix1": { "code": "POP105A", "name": "Populatia" },
  "matrix2": { "code": "ECO101B", "name": "PIB pe locuitor" },
  "year": 2023,
  "territoryLevel": "NUTS3",
  "correlation": {
    "pearson": 0.73,
    "interpretation": "strong positive",
    "pValue": 0.001,
    "dataPoints": 42
  },
  "scatterData": [
    { "territory": "B", "x": 1883400, "y": 145000 },
    { "territory": "CJ", "x": 713500, "y": 98000 },
    ...
  ],
  "regression": {
    "slope": 0.052,
    "intercept": 28500,
    "rSquared": 0.53
  }
}
```

**SQL Pattern - Correlation:**
```sql
WITH matrix1_data AS (
    SELECT t.id AS territory_id, s.value AS value1
    FROM statistics s
    JOIN territories t ON s.territory_id = t.id
    JOIN time_periods tp ON s.time_period_id = tp.id
    WHERE s.matrix_id = :matrix_id_1
      AND tp.year = :year
      AND t.level = :territory_level
),
matrix2_data AS (
    SELECT t.id AS territory_id, s.value AS value2
    FROM statistics s
    JOIN territories t ON s.territory_id = t.id
    JOIN time_periods tp ON s.time_period_id = tp.id
    WHERE s.matrix_id = :matrix_id_2
      AND tp.year = :year
      AND t.level = :territory_level
),
combined AS (
    SELECT m1.value1, m2.value2
    FROM matrix1_data m1
    JOIN matrix2_data m2 ON m1.territory_id = m2.territory_id
    WHERE m1.value1 IS NOT NULL AND m2.value2 IS NOT NULL
)
SELECT
    CORR(value1, value2) AS pearson_correlation,
    COUNT(*) AS data_points
FROM combined;
```

---

### 2.5 Aggregation Questions

**Q: What is the total/average of X at different levels?**

```http
GET /api/v1/aggregate/:matrixCode
    ?year=2023
    &aggregateFrom=NUTS3
    &aggregateTo=NUTS2
    &function=sum
```

**Response:**
```json
{
  "matrix": { "code": "POP105A" },
  "year": 2023,
  "aggregateFrom": "NUTS3",
  "aggregateTo": "NUTS2",
  "function": "sum",
  "results": [
    {
      "territory": { "code": "RO11", "name": "Nord-Vest" },
      "value": 2584700,
      "componentCount": 6,
      "components": [
        { "code": "BH", "name": "Bihor", "value": 575400 },
        { "code": "BN", "name": "Bistrita-Nasaud", "value": 286300 },
        ...
      ]
    },
    ...
  ]
}
```

**SQL Pattern - Hierarchical Aggregation:**
```sql
SELECT
    parent.id AS region_id,
    parent.code AS region_code,
    parent.name AS region_name,
    SUM(s.value) AS total,
    AVG(s.value) AS average,
    COUNT(*) AS county_count
FROM statistics s
JOIN territories t ON s.territory_id = t.id
JOIN territories parent ON t.parent_id = parent.id
JOIN time_periods tp ON s.time_period_id = tp.id
WHERE s.matrix_id = :matrix_id
  AND tp.year = :year
  AND t.level = 'NUTS3'
GROUP BY parent.id
ORDER BY parent.code;
```

---

## Part 3: Advanced Query Patterns

### 3.1 Cross-Tabulation (Pivot Tables)

**Use Case:** Show matrix data as a pivot table with dimensions on rows/columns.

```http
GET /api/v1/pivot/:matrixCode
    ?rows=territory
    &columns=year
    &filter[SEX]=TOTAL
    &territoryLevel=NUTS3
    &yearFrom=2020
    &yearTo=2024
```

**Response:**
```json
{
  "metadata": {
    "matrix": "POP105A",
    "unit": "Persons",
    "filters": { "SEX": "TOTAL" }
  },
  "columns": [2020, 2021, 2022, 2023, 2024],
  "rows": [
    { "key": "AB", "label": "Alba", "values": [323400, 320100, 318200, 316500, 314800] },
    { "key": "AR", "label": "Arad", "values": [421500, 418900, 416800, 414200, 411600] },
    ...
  ],
  "totals": {
    "row": [19408000, 19186000, 19053000, 18920000, 18787000],
    "column": { "AB": 1593000, "AR": 2083000, ... }
  }
}
```

**SQL Pattern:**
```sql
SELECT
    t.code AS territory_code,
    t.name AS territory,
    SUM(CASE WHEN tp.year = 2020 THEN s.value END) AS "2020",
    SUM(CASE WHEN tp.year = 2021 THEN s.value END) AS "2021",
    SUM(CASE WHEN tp.year = 2022 THEN s.value END) AS "2022",
    SUM(CASE WHEN tp.year = 2023 THEN s.value END) AS "2023",
    SUM(CASE WHEN tp.year = 2024 THEN s.value END) AS "2024"
FROM statistics s
JOIN territories t ON s.territory_id = t.id
JOIN time_periods tp ON s.time_period_id = tp.id
WHERE s.matrix_id = :matrix_id
  AND t.level = 'NUTS3'
  AND tp.year BETWEEN 2020 AND 2024
  AND EXISTS (
      SELECT 1 FROM statistic_classifications sc
      JOIN classification_values cv ON sc.classification_value_id = cv.id
      WHERE sc.statistic_id = s.id
        AND sc.matrix_id = s.matrix_id
        AND cv.code = 'TOTAL'
  )
GROUP BY t.id
ORDER BY t.name;
```

---

### 3.2 Time Series with Interpolation

**Use Case:** Get continuous time series with missing values filled.

```http
GET /api/v1/timeseries/:matrixCode
    ?territoryCode=RO
    &yearFrom=2000
    &yearTo=2024
    &interpolate=linear
```

**Response:**
```json
{
  "matrix": { "code": "POP105A" },
  "territory": { "code": "RO" },
  "interpolation": "linear",
  "data": [
    { "year": 2000, "value": 22435000, "source": "actual" },
    { "year": 2001, "value": 22329000, "source": "actual" },
    { "year": 2002, "value": null, "interpolated": 22223000, "source": "interpolated" },
    ...
  ],
  "quality": {
    "actualPoints": 23,
    "interpolatedPoints": 2,
    "completeness": 92
  }
}
```

**Interpolation Methods:**
- `none` - Return null for missing values
- `previous` - Use last known value (forward fill)
- `next` - Use next known value (backward fill)
- `linear` - Linear interpolation between known points

---

### 3.3 Multi-Matrix Composite Indicators

**Use Case:** Calculate derived indicators from multiple matrices.

```http
GET /api/v1/indicators/population-density
    ?year=2023
    &territoryLevel=NUTS3
```

**Built-in Indicators:**

| Indicator | Formula | Matrices Used |
|-----------|---------|---------------|
| `population-density` | Population / Area | POP105A / GEO101A |
| `urbanization-rate` | Urban Pop / Total Pop × 100 | POP105B |
| `birth-rate` | Live Births / Pop × 1000 | POP201A / POP105A |
| `dependency-ratio` | (0-14 + 65+) / (15-64) × 100 | POP105A |

**Response:**
```json
{
  "indicator": {
    "code": "population-density",
    "name": "Population Density",
    "unit": "persons/km²",
    "formula": "POP105A / GEO101A"
  },
  "year": 2023,
  "data": [
    { "territory": "B", "name": "Bucuresti", "value": 7893.5, "rank": 1 },
    { "territory": "IF", "name": "Ilfov", "value": 289.2, "rank": 2 },
    { "territory": "PH", "name": "Prahova", "value": 118.5, "rank": 3 },
    ...
  ],
  "statistics": {
    "national": 80.3,
    "min": 29.4,
    "max": 7893.5,
    "median": 68.2
  }
}
```

**SQL Pattern:**
```sql
WITH population AS (
    SELECT t.id AS territory_id, s.value AS pop
    FROM statistics s
    JOIN matrices m ON s.matrix_id = m.id
    JOIN territories t ON s.territory_id = t.id
    JOIN time_periods tp ON s.time_period_id = tp.id
    WHERE m.ins_code = 'POP105A'
      AND tp.year = :year
      AND t.level = :territory_level
),
area AS (
    SELECT t.id AS territory_id, s.value AS area_km2
    FROM statistics s
    JOIN matrices m ON s.matrix_id = m.id
    JOIN territories t ON s.territory_id = t.id
    JOIN time_periods tp ON s.time_period_id = tp.id
    WHERE m.ins_code = 'GEO101A'
      AND tp.year = :year
)
SELECT
    t.code,
    t.name,
    p.pop,
    a.area_km2,
    ROUND(p.pop / NULLIF(a.area_km2, 0), 2) AS population_density
FROM population p
JOIN area a ON p.territory_id = a.territory_id
JOIN territories t ON p.territory_id = t.id
ORDER BY population_density DESC;
```

---

### 3.4 Weighted Aggregation

**Use Case:** Calculate population-weighted statistics when aggregating.

```http
GET /api/v1/aggregate/:matrixCode/weighted
    ?year=2023
    &weightMatrix=POP105A
    &aggregateFrom=NUTS3
    &aggregateTo=NUTS1
```

**Response:**
```json
{
  "matrix": { "code": "INC101A", "name": "Venitul mediu pe gospodarie" },
  "weightMatrix": { "code": "POP105A", "name": "Populatia" },
  "year": 2023,
  "results": [
    {
      "territory": { "code": "RO1", "name": "Macroregiunea 1" },
      "simpleAverage": 4850,
      "weightedAverage": 5120,
      "totalWeight": 5678000
    },
    ...
  ]
}
```

**SQL Pattern:**
```sql
WITH target_data AS (
    SELECT t.id, t.parent_id, s.value AS metric
    FROM statistics s
    JOIN territories t ON s.territory_id = t.id
    JOIN time_periods tp ON s.time_period_id = tp.id
    WHERE s.matrix_id = :target_matrix_id
      AND tp.year = :year
      AND t.level = 'NUTS3'
),
weight_data AS (
    SELECT t.id, s.value AS weight
    FROM statistics s
    JOIN matrices m ON s.matrix_id = m.id
    JOIN territories t ON s.territory_id = t.id
    JOIN time_periods tp ON s.time_period_id = tp.id
    WHERE m.ins_code = :weight_matrix_code
      AND tp.year = :year
)
SELECT
    parent.code,
    parent.name,
    SUM(td.metric * wd.weight) / SUM(wd.weight) AS weighted_avg,
    SUM(td.metric) / COUNT(*) AS simple_avg,
    SUM(wd.weight) AS total_weight
FROM target_data td
JOIN weight_data wd ON td.id = wd.id
JOIN territories parent ON td.parent_id = parent.id
GROUP BY parent.id
ORDER BY parent.code;
```

---

## Part 4: Schema Improvements

### 4.1 Matrix Tags/Keywords

**Problem:** Matrices are only discoverable via context hierarchy or full-text search.

**Solution:** Add a many-to-many tagging system for improved discoverability.

```sql
-- New tables
CREATE TABLE matrix_tags (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    name_en VARCHAR(100),
    category VARCHAR(50),  -- 'topic', 'audience', 'use_case'
    CONSTRAINT uq_matrix_tags_name UNIQUE (name)
);

CREATE TABLE matrix_tag_assignments (
    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES matrix_tags(id) ON DELETE CASCADE,
    PRIMARY KEY (matrix_id, tag_id)
);

CREATE INDEX idx_matrix_tags_category ON matrix_tags(category);
CREATE INDEX idx_tag_assignments_tag ON matrix_tag_assignments(tag_id);
```

**Example Tags:**

| Category | Tags |
|----------|------|
| `topic` | demografie, economie, educatie, sanatate, mediu |
| `audience` | cercetare-academica, politici-publice, jurnalism, business |
| `use_case` | analiza-tendinte, comparatii-regionale, raportare |

**API Endpoints:**
```http
GET /api/v1/tags                      -- List all tags with matrix counts
GET /api/v1/matrices?tags=demografie,economie  -- Filter by tags
```

---

### 4.2 Pre-computed Aggregates

**Problem:** Aggregating large datasets (e.g., all localities to counties) is slow.

**Solution:** Materialized views for common aggregation patterns.

```sql
-- Pre-computed annual totals at NUTS2 level
CREATE MATERIALIZED VIEW mv_annual_nuts2_totals AS
SELECT
    s.matrix_id,
    tp.year,
    nuts2.id AS territory_id,
    SUM(s.value) AS total_value,
    AVG(s.value) AS avg_value,
    COUNT(*) AS data_points
FROM statistics s
JOIN territories t ON s.territory_id = t.id
JOIN territories nuts2 ON t.path LIKE nuts2.path || '::%'
JOIN time_periods tp ON s.time_period_id = tp.id
WHERE nuts2.level = 'NUTS2'
  AND tp.periodicity = 'ANNUAL'
  AND s.value IS NOT NULL
GROUP BY s.matrix_id, tp.year, nuts2.id;

CREATE INDEX idx_mv_nuts2_matrix_year ON mv_annual_nuts2_totals(matrix_id, year);

-- Refresh strategy
-- Option 1: Daily refresh during off-peak
-- Option 2: Trigger refresh after data sync
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_annual_nuts2_totals;
```

**Additional Views:**
- `mv_annual_nuts3_totals` - County-level aggregates
- `mv_time_series_national` - National time series
- `mv_classification_totals` - Aggregates by classification

---

### 4.3 Data Quality Metadata

**Problem:** Users don't know data reliability or completeness.

**Solution:** Track data quality metrics per matrix-territory-year combination.

```sql
CREATE TABLE data_quality_metrics (
    id SERIAL PRIMARY KEY,
    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,
    territory_id INTEGER REFERENCES territories(id),
    year SMALLINT,

    -- Completeness metrics
    expected_data_points INTEGER,
    actual_data_points INTEGER,
    completeness_pct NUMERIC(5,2),

    -- Special value counts
    null_count INTEGER DEFAULT 0,
    unavailable_count INTEGER DEFAULT 0,  -- value_status = ':'
    confidential_count INTEGER DEFAULT 0, -- value_status = '*'

    -- Temporal consistency
    has_time_series_break BOOLEAN DEFAULT FALSE,

    -- Metadata
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_data_quality UNIQUE (matrix_id, territory_id, year)
);

CREATE INDEX idx_data_quality_matrix ON data_quality_metrics(matrix_id);
CREATE INDEX idx_data_quality_completeness ON data_quality_metrics(completeness_pct);
```

**Computation Job:**
```sql
INSERT INTO data_quality_metrics (
    matrix_id, territory_id, year,
    actual_data_points, null_count, unavailable_count
)
SELECT
    matrix_id,
    territory_id,
    (SELECT year FROM time_periods WHERE id = time_period_id),
    COUNT(*),
    COUNT(*) FILTER (WHERE value IS NULL),
    COUNT(*) FILTER (WHERE value_status = ':')
FROM statistics
GROUP BY matrix_id, territory_id, time_period_id
ON CONFLICT (matrix_id, territory_id, year)
DO UPDATE SET
    actual_data_points = EXCLUDED.actual_data_points,
    null_count = EXCLUDED.null_count,
    unavailable_count = EXCLUDED.unavailable_count,
    computed_at = NOW();
```

**API Endpoints:**
```http
GET /api/v1/matrices/:code/quality
GET /api/v1/statistics/:matrixCode?minCompleteness=80
```

---

### 4.4 Related Matrices

**Problem:** Users don't know which matrices can be combined or compared.

**Solution:** Track matrix relationships explicitly.

```sql
CREATE TABLE matrix_relationships (
    id SERIAL PRIMARY KEY,
    matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,
    related_matrix_id INTEGER NOT NULL REFERENCES matrices(id) ON DELETE CASCADE,
    relationship_type VARCHAR(50) NOT NULL,
    notes TEXT,

    CONSTRAINT uq_matrix_relationships UNIQUE (matrix_id, related_matrix_id, relationship_type)
);

CREATE INDEX idx_matrix_rel_type ON matrix_relationships(relationship_type);
```

**Relationship Types:**

| Type | Description | Example |
|------|-------------|---------|
| `continuation` | Time series continues from previous matrix | POP105B → POP105A |
| `supersedes` | Newer version replaces older | POP107D → POP107C |
| `complement` | Related topic, can be combined | POP105A + GEO101A |
| `ratio_numerator` | Can compute ratio (this ÷ other) | POP201A (births) |
| `ratio_denominator` | Can compute ratio (other ÷ this) | POP105A (population) |
| `same_dimensions` | Similar structure, comparable | All POP1xx matrices |

**API Endpoint:**
```http
GET /api/v1/matrices/:code/related
```

---

### 4.5 Saved Queries / Bookmarks

**Problem:** Users repeatedly build the same complex queries.

**Solution:** Allow saving and sharing query configurations.

```sql
CREATE TABLE saved_queries (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,

    -- Query definition
    matrix_code VARCHAR(20) NOT NULL,
    territory_filter JSONB,        -- {"level": "NUTS3", "codes": ["BH", "CJ"]}
    time_filter JSONB,             -- {"yearFrom": 2020, "yearTo": 2024}
    classification_filter JSONB,   -- {"SEX": ["M"], "AGE_GROUP": ["TOTAL"]}

    -- Access control
    is_public BOOLEAN DEFAULT FALSE,

    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_saved_queries_matrix ON saved_queries(matrix_code);
CREATE INDEX idx_saved_queries_public ON saved_queries(is_public) WHERE is_public = TRUE;
```

**API Endpoints:**
```http
POST /api/v1/queries              -- Save a query
GET /api/v1/queries               -- List saved queries
GET /api/v1/queries/:id           -- Get query definition
GET /api/v1/queries/:id/execute   -- Execute saved query
DELETE /api/v1/queries/:id        -- Delete saved query
```

---

## Part 5: New Endpoint Summary

### Discovery Endpoints (Simple Layer)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/discover/topics` | GET | Browse by subject area with statistics |
| `/discover/by-territory` | GET | Find data for specific location |
| `/discover/by-time-range` | GET | Find data with coverage period |
| `/discover/by-breakdowns` | GET | Find data with specific dimensions |
| `/discover/search` | GET | Full-text search across all metadata |
| `/territories/:id/available-data` | GET | Datasets with data for territory |
| `/matrices/:code/data-coverage` | GET | Actual data availability |
| `/matrices/:code/breakdowns` | GET | Available classification dimensions |

### Analytical Endpoints (Standard Layer)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/compare/territories` | GET | Side-by-side territory comparison |
| `/trends/:matrixCode` | GET | Time series with growth rates |
| `/rankings/:matrixCode` | GET | Ranked list with percentiles |
| `/rankings/:matrixCode/position` | GET | Single territory ranking position |
| `/distribution/:matrixCode` | GET | Statistical distribution analysis |
| `/aggregate/:matrixCode` | GET | Hierarchical aggregation |

### Advanced Endpoints (Advanced Layer)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/correlate` | GET | Cross-matrix correlation analysis |
| `/pivot/:matrixCode` | GET | Pivot table format |
| `/timeseries/:matrixCode` | GET | Time series with interpolation |
| `/aggregate/:matrixCode/weighted` | GET | Weighted hierarchical aggregation |
| `/indicators` | GET | List composite indicators |
| `/indicators/:name` | GET | Calculate specific indicator |

### Metadata & Management Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/tags` | GET | List all tags with matrix counts |
| `/matrices/:code/quality` | GET | Data quality metrics |
| `/matrices/:code/related` | GET | Related matrices |
| `/queries` | GET, POST | List/save queries |
| `/queries/:id` | GET, DELETE | Get/delete saved query |
| `/queries/:id/execute` | GET | Execute saved query |

---

## Part 6: Implementation Priority

### Phase 1: Discovery & Navigation (High Value, Low Effort)

**Estimated Files to Modify:**
- `src/server/routes/discover.ts` (new)
- `src/server/routes/territories.ts` (enhance)
- `src/server/routes/matrices.ts` (enhance)
- `src/db/postgres-schema.sql` (add tags tables)

**Endpoints:**
1. `/discover/topics` - Topic browser with matrix counts
2. `/territories/:id/available-data` - Data availability per territory
3. `/matrices/:code/breakdowns` - Available classification dimensions
4. Matrix tags system (schema + API)

---

### Phase 2: Analytical Queries (High Value, Medium Effort)

**Estimated Files to Modify:**
- `src/server/routes/analytics.ts` (new)
- `src/services/analytics/` (new directory)
- `src/db/postgres-schema.sql` (add materialized views)

**Endpoints:**
1. `/trends/:matrixCode` - Time series with growth rates
2. `/rankings/:matrixCode` - Territory rankings with percentiles
3. `/compare/territories` - Multi-territory comparison
4. Pre-computed aggregates (materialized views)

---

### Phase 3: Advanced Analytics (Medium Value, Higher Effort)

**Estimated Files to Modify:**
- `src/server/routes/analytics.ts` (enhance)
- `src/services/analytics/correlation.ts` (new)
- `src/services/analytics/pivot.ts` (new)
- `src/db/postgres-schema.sql` (add quality tables)

**Endpoints:**
1. `/distribution/:matrixCode` - Statistical distribution
2. `/correlate` - Cross-matrix correlation
3. `/pivot/:matrixCode` - Pivot table format
4. Data quality metrics (schema + API + computation job)

---

### Phase 4: User Features (Future)

**Estimated Files to Modify:**
- `src/server/routes/queries.ts` (new)
- `src/server/routes/indicators.ts` (new)
- `src/db/postgres-schema.sql` (add saved queries, relationships)

**Endpoints:**
1. Saved queries / bookmarks
2. Composite indicators
3. Related matrices suggestions

---

## Appendix: SQL Query Reference

### A. Common Query Patterns

#### Territory Path Filtering
```sql
-- Find all children of a territory using path prefix
SELECT * FROM territories
WHERE path LIKE 'RO::MACRO1::NORD_VEST::%';

-- Find all ancestors
SELECT * FROM territories
WHERE 'RO::MACRO1::NORD_VEST::BH::26573' LIKE path || '%'
ORDER BY LENGTH(path);
```

#### Classification Filtering with TOTAL Default
```sql
-- Default: only TOTAL classification values
SELECT s.*
FROM statistics s
WHERE s.matrix_id = :matrix_id
  AND NOT EXISTS (
      SELECT 1 FROM statistic_classifications sc
      JOIN classification_values cv ON sc.classification_value_id = cv.id
      WHERE sc.statistic_id = s.id
        AND sc.matrix_id = s.matrix_id
        AND cv.code != 'TOTAL'
  );
```

#### Efficient Pagination
```sql
-- Cursor-based pagination (avoid OFFSET)
SELECT * FROM statistics
WHERE matrix_id = :matrix_id
  AND id > :cursor
ORDER BY id
LIMIT :limit;
```

### B. Aggregation Patterns

#### Window Functions for Rankings
```sql
SELECT
    t.name,
    s.value,
    ROW_NUMBER() OVER (ORDER BY s.value DESC) AS rank,
    PERCENT_RANK() OVER (ORDER BY s.value) AS percentile,
    NTILE(4) OVER (ORDER BY s.value) AS quartile
FROM statistics s
JOIN territories t ON s.territory_id = t.id
WHERE s.matrix_id = :matrix_id;
```

#### Hierarchical Aggregation with Recursive CTE
```sql
WITH RECURSIVE territory_tree AS (
    -- Base: target level
    SELECT id, code, name, parent_id, value
    FROM statistics s
    JOIN territories t ON s.territory_id = t.id
    WHERE t.level = 'NUTS3'

    UNION ALL

    -- Recurse: aggregate to parent
    SELECT p.id, p.code, p.name, p.parent_id, SUM(c.value)
    FROM territory_tree c
    JOIN territories p ON c.parent_id = p.id
    GROUP BY p.id
)
SELECT * FROM territory_tree WHERE level = 'NUTS1';
```

### C. Time Series Patterns

#### Gap Detection
```sql
WITH yearly AS (
    SELECT DISTINCT EXTRACT(year FROM period_start)::INT AS year
    FROM time_periods
    WHERE periodicity = 'ANNUAL'
),
expected AS (
    SELECT generate_series(
        (SELECT MIN(year) FROM yearly),
        (SELECT MAX(year) FROM yearly)
    ) AS year
)
SELECT e.year AS missing_year
FROM expected e
LEFT JOIN yearly y ON e.year = y.year
WHERE y.year IS NULL;
```

#### Moving Average
```sql
SELECT
    year,
    value,
    AVG(value) OVER (
        ORDER BY year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3yr
FROM ...
```

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Target Audience** | All (developers, analysts, public) | Multi-layered API with progressive complexity |
| **Response Format** | JSON only | Clients handle conversion; keeps API simple |
| **Pagination** | Cursor-based | Better performance than OFFSET for large datasets |
| **Caching** | Per-layer strategy | Simple: aggressive, Advanced: minimal |
| **Authentication** | Optional (future) | Start public, add auth for saved queries later |

---

## Open Questions (For Future Implementation)

1. **Rate Limiting:** Should expensive analytical endpoints have stricter limits?
2. **Caching TTL:** How long to cache pre-computed aggregates?
3. **Indicator Configuration:** Should composite indicators be configurable via API?
4. **Versioning:** How to handle API version changes?
5. **WebSocket:** Real-time updates for frequently-changing data?
