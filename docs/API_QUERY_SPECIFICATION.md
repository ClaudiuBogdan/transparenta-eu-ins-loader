# INS Statistical Data API

> **Version:** 2.0  
> **Base URL:** `http://localhost:3000/api/v1`  
> **Status:** Production Ready

Romanian statistical data API providing access to INS (Institutul National de Statistica) datasets including population, economy, education, health, and more.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Core Concepts](#core-concepts)
3. [API Reference](#api-reference)
   - [Health & Status](#health--status)
   - [Contexts (Domain Hierarchy)](#contexts-domain-hierarchy)
   - [Matrices (Datasets)](#matrices-datasets)
   - [Dimensions](#dimensions)
   - [Statistics](#statistics)
   - [Analytics](#analytics)
   - [Discovery](#discovery)
4. [Error Handling](#error-handling)
5. [Pagination](#pagination)
6. [Common Use Cases](#common-use-cases)
7. [Appendix: SQL Patterns](#appendix-sql-patterns)

---

## Quick Start

### Your First API Call

Check if the API is running:

```bash
curl http://localhost:3000/health
```

**Response:**
```json
{"status":"ok"}
```

### Get Population Data for Romania

```bash
curl "http://localhost:3000/api/v1/statistics/POP105A?territoryCode=RO&yearFrom=2020&yearTo=2024"
```

**Response:**
```json
{
  "data": {
    "matrix": {
      "insCode": "POP105A",
      "name": "Populatia rezidenta la 1 ianuarie..."
    },
    "series": [{
      "seriesId": "POP105A_series",
      "data": [
        {"x": "Anul 2020", "y": 19354339},
        {"x": "Anul 2021", "y": 19229519},
        {"x": "Anul 2022", "y": 19043098},
        {"x": "Anul 2023", "y": 19055228},
        {"x": "Anul 2024", "y": 19067576}
      ]
    }]
  }
}
```

### Response Format

All responses follow this structure:

```json
{
  "data": { ... },           // Main response data
  "meta": {                  // Optional metadata
    "pagination": { ... }    // Pagination info (for lists)
  }
}
```

---

## Core Concepts

### Data Hierarchy

```
Contexts (8 domains)
    └── Matrices (~1,898 datasets)
            └── Statistics (fact data)
                    ├── Territory dimension
                    ├── Time period dimension
                    ├── Classification dimensions (sex, age, etc.)
                    └── Unit of measure
```

### Dimensions Explained

| Dimension | Description | Example |
|-----------|-------------|---------|
| **Territory** | Geographic location (NUTS hierarchy + LAU) | Romania, Cluj county, Sibiu city |
| **Time Period** | When the data applies | Year 2023, Q1 2023, January 2023 |
| **Classification** | Demographic breakdowns | Sex (M/F), Age groups, Environment (Urban/Rural) |
| **Unit** | Measurement unit | Persons, Hectares, RON |

### Territory Levels

| Level | Description | Count |
|-------|-------------|-------|
| `NATIONAL` | Romania | 1 |
| `NUTS1` | Macroregions | 4 |
| `NUTS2` | Development regions | 8 |
| `NUTS3` | Counties (judete) | 42 |
| `LAU` | Localities (UAT) | ~3,200 |

### Matrix Codes

Each dataset has a unique INS code like `POP105A`:
- `POP` = Domain (Population)
- `105` = Numeric identifier
- `A` = Version/variant

---

## API Reference

### Health & Status

#### Check API Health

```http
GET /health
```

```bash
curl http://localhost:3000/health
```

**Response:**
```json
{"status":"ok"}
```

---

### Contexts (Domain Hierarchy)

#### List Contexts

Browse the hierarchical domain structure (8 root domains with ~340 subcategories).

```http
GET /api/v1/contexts
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `parentId` | number | No | Filter by parent context ID |
| `level` | number | No | Filter by hierarchy level (1-4) |
| `limit` | number | No | Results per page (default: 50, max: 100) |
| `cursor` | string | No | Pagination cursor |

```bash
# List root domains
curl "http://localhost:3000/api/v1/contexts?level=1"

# List children of a context
curl "http://localhost:3000/api/v1/contexts?parentId=1"
```

**Response:**
```json
{
  "data": [
    {
      "id": 1,
      "insCode": "1",
      "name": "STATISTICA SOCIALA",
      "level": 1,
      "parentId": null,
      "path": "0.1",
      "childrenType": "context",
      "childCount": 18
    }
  ],
  "meta": {
    "pagination": {
      "cursor": null,
      "hasMore": false,
      "limit": 50
    }
  }
}
```

#### Get Context Details

```http
GET /api/v1/contexts/:id
```

```bash
curl "http://localhost:3000/api/v1/contexts/1"
```

**Response:**
```json
{
  "data": {
    "context": {
      "id": 1,
      "insCode": "1",
      "name": "STATISTICA SOCIALA",
      "level": 1
    },
    "children": [...],
    "ancestors": []
  }
}
```

---

### Matrices (Datasets)

#### List Matrices

Search and filter statistical datasets.

```http
GET /api/v1/matrices
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `q` | string | No | Search query (searches name) |
| `contextId` | number | No | Filter by context ID |
| `contextPath` | string | No | Filter by context path prefix |
| `status` | string | No | Filter by sync status: `PENDING`, `SYNCED`, `ERROR` |
| `hasUatData` | boolean | No | Filter matrices with UAT-level data |
| `hasCountyData` | boolean | No | Filter matrices with county-level data |
| `periodicity` | string | No | Filter by periodicity: `ANNUAL`, `QUARTERLY`, `MONTHLY` |
| `sortBy` | string | No | Sort field: `name` or `lastUpdate` |
| `sortOrder` | string | No | Sort order: `asc` or `desc` |
| `limit` | number | No | Results per page (default: 50, max: 100) |
| `cursor` | string | No | Pagination cursor |
| `locale` | string | No | Language: `ro` (default) or `en` |

```bash
# Search for population matrices
curl "http://localhost:3000/api/v1/matrices?q=populat"

# Get synced matrices only
curl "http://localhost:3000/api/v1/matrices?status=SYNCED"

# Get matrices with county data
curl "http://localhost:3000/api/v1/matrices?hasCountyData=true&limit=10"
```

**Response:**
```json
{
  "data": [
    {
      "id": 1033,
      "insCode": "POP105A",
      "name": "Populatia rezidenta la 1 ianuarie...",
      "contextPath": "0.1.10.1010",
      "contextName": "1. POPULATIA REZIDENTA",
      "periodicity": ["ANNUAL"],
      "hasUatData": false,
      "hasCountyData": true,
      "dimensionCount": 6,
      "startYear": 2003,
      "endYear": 2025,
      "lastUpdate": "2025-02-08T22:00:00.000Z",
      "status": "SYNCED"
    }
  ],
  "meta": {
    "pagination": {
      "cursor": "eyJpZCI6MTA0MCwic29ydFZhbHVlIjoiUG9wdWxhdGlhIn0=",
      "hasMore": true,
      "limit": 50
    }
  }
}
```

#### Get Matrix Details

Get detailed information about a specific matrix including dimensions.

```http
GET /api/v1/matrices/:code
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `locale` | string | No | Language: `ro` (default) or `en` |

```bash
curl "http://localhost:3000/api/v1/matrices/POP105A"
```

**Response:**
```json
{
  "data": {
    "matrix": {
      "id": 1033,
      "insCode": "POP105A",
      "name": "Populatia rezidenta la 1 ianuarie...",
      "definition": "Populatia rezidenta reprezinta...",
      "methodology": "Date preluate din...",
      "periodicity": ["ANNUAL"],
      "hasUatData": false,
      "hasCountyData": true,
      "dimensionCount": 6,
      "startYear": 2003,
      "endYear": 2025,
      "status": "SYNCED"
    },
    "dimensions": [
      {
        "id": 1,
        "dimCode": 1,
        "label": "Ani",
        "dimensionType": "TEMPORAL",
        "isHierarchical": false,
        "optionCount": 23
      },
      {
        "id": 2,
        "dimCode": 2,
        "label": "Judete",
        "dimensionType": "TERRITORIAL",
        "isHierarchical": true,
        "optionCount": 43
      },
      {
        "id": 3,
        "dimCode": 3,
        "label": "Sexe",
        "dimensionType": "CLASSIFICATION",
        "classificationTypeCode": "SEX",
        "isHierarchical": false,
        "optionCount": 3
      }
    ],
    "syncStatus": {
      "syncStatus": "SYNCED",
      "lastFullSync": "2025-01-15T10:30:00.000Z",
      "dataStartYear": 2020,
      "dataEndYear": 2024,
      "rowCount": 5
    }
  }
}
```

#### Get Matrix Data Coverage

Get data coverage statistics for a matrix.

```http
GET /api/v1/matrices/:code/data-coverage
```

```bash
curl "http://localhost:3000/api/v1/matrices/POP105A/data-coverage"
```

**Response:**
```json
{
  "data": {
    "matrixCode": "POP105A",
    "name": "Populatia rezidenta...",
    "territorial": [
      {"level": "NATIONAL", "territoryCount": 1, "dataPointCount": 5}
    ],
    "temporal": [
      {"periodicity": "ANNUAL", "minYear": 2020, "maxYear": 2024, "yearCount": 5}
    ],
    "overall": {
      "totalDataPoints": 5,
      "nonNullCount": 5,
      "nullCount": 0,
      "completeness": 100
    }
  }
}
```

#### Get Matrix Breakdowns

Get available dimension breakdowns for a matrix.

```http
GET /api/v1/matrices/:code/breakdowns
```

```bash
curl "http://localhost:3000/api/v1/matrices/POP105A/breakdowns"
```

**Response:**
```json
{
  "data": {
    "matrixCode": "POP105A",
    "dimensionCount": 6,
    "breakdowns": [
      {
        "dimIndex": 1,
        "label": "Ani",
        "dimensionType": "TEMPORAL",
        "optionCount": 23,
        "sampleValues": [
          {"nomItemId": 4494, "label": "Anul 2023"},
          {"nomItemId": 4495, "label": "Anul 2024"}
        ]
      }
    ],
    "byType": {
      "territorial": [...],
      "temporal": [...],
      "classification": [...]
    }
  }
}
```

#### Get Dimension Options

Get all options for a specific dimension.

```http
GET /api/v1/matrices/:code/dimensions/:dimIndex
```

```bash
curl "http://localhost:3000/api/v1/matrices/POP105A/dimensions/2"
```

**Response:**
```json
{
  "data": [
    {
      "id": 101,
      "nomItemId": 4494,
      "label": "TOTAL",
      "offsetOrder": 0,
      "parentNomItemId": null,
      "reference": {
        "type": "TERRITORY",
        "id": 1,
        "code": "RO",
        "name": "ROMANIA"
      }
    }
  ],
  "meta": {
    "dimension": {
      "id": 2,
      "dimCode": 2,
      "label": "Judete",
      "dimensionType": "TERRITORIAL",
      "optionCount": 43
    }
  }
}
```

---

### Dimensions

#### List Territories

```http
GET /api/v1/territories
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `level` | string | No | Filter by level: `NATIONAL`, `NUTS1`, `NUTS2`, `NUTS3`, `LAU` |
| `parentId` | number | No | Filter by parent territory ID |
| `q` | string | No | Search by name or code |
| `limit` | number | No | Results per page (default: 50, max: 500) |
| `cursor` | string | No | Pagination cursor |

```bash
# Get all counties
curl "http://localhost:3000/api/v1/territories?level=NUTS3"

# Search territories
curl "http://localhost:3000/api/v1/territories?q=Cluj"
```

**Response:**
```json
{
  "data": [
    {
      "id": 14,
      "code": "AB",
      "sirutaCode": null,
      "name": "Alba",
      "level": "NUTS3",
      "parentId": 5,
      "path": "RO.RO1.RO12.AB"
    }
  ],
  "meta": {
    "pagination": {"cursor": null, "hasMore": false, "limit": 50}
  }
}
```

#### List Time Periods

```http
GET /api/v1/time-periods
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `periodicity` | string | No | Filter: `ANNUAL`, `QUARTERLY`, `MONTHLY` |
| `yearFrom` | number | No | Start year (inclusive) |
| `yearTo` | number | No | End year (inclusive) |
| `limit` | number | No | Results per page |

```bash
# Get annual periods from 2020
curl "http://localhost:3000/api/v1/time-periods?periodicity=ANNUAL&yearFrom=2020"
```

**Response:**
```json
{
  "data": [
    {
      "id": 2023,
      "year": 2023,
      "quarter": null,
      "month": null,
      "periodicity": "ANNUAL",
      "insLabel": "Anul 2023",
      "periodStart": "2023-01-01",
      "periodEnd": "2023-12-31"
    }
  ]
}
```

#### List Classifications

```http
GET /api/v1/classifications
```

```bash
curl "http://localhost:3000/api/v1/classifications"
```

**Response:**
```json
{
  "data": [
    {
      "id": 1,
      "code": "SEX",
      "name": "Sexe",
      "isHierarchical": false,
      "valueCount": 3
    }
  ]
}
```

#### Get Classification Values

```http
GET /api/v1/classifications/:code
```

```bash
curl "http://localhost:3000/api/v1/classifications/SEX"
```

**Response:**
```json
{
  "data": {
    "type": {"id": 1, "code": "SEX", "name": "Sexe"},
    "values": [
      {"id": 1, "code": "TOTAL", "name": "Total", "parentId": null, "level": 0},
      {"id": 2, "code": "M", "name": "Masculin", "parentId": 1, "level": 1},
      {"id": 3, "code": "F", "name": "Feminin", "parentId": 1, "level": 1}
    ]
  }
}
```

---

### Statistics

#### Query Statistics

Query statistical data as time series. **By default, only TOTAL values are returned** for classification dimensions to avoid duplicate data.

```http
GET /api/v1/statistics/:matrixCode
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `territoryId` | number | No | Filter by territory ID |
| `territoryCode` | string | No | Filter by territory code (e.g., `RO`, `CJ`, `BH`) |
| `territoryPath` | string | No | Filter by territory path prefix |
| `territoryLevel` | string | No | Filter: `NATIONAL`, `NUTS1`, `NUTS2`, `NUTS3`, `LAU` |
| `yearFrom` | number | No | Start year (inclusive) |
| `yearTo` | number | No | End year (inclusive) |
| `periodicity` | string | No | Filter: `ANNUAL`, `QUARTERLY`, `MONTHLY` |
| `classificationFilters` | string | No | JSON object: `{"SEX":["M","F"]}` or `{"*":["TOTAL"]}` |
| `includeAll` | boolean | No | If `true`, returns all classification values (not just TOTAL) |
| `groupBy` | string | No | Group series by: `none`, `territory`, `classification` |
| `limit` | number | No | Results per page (default: 100, max: 1000) |
| `cursor` | string | No | Pagination cursor |
| `locale` | string | No | Language: `ro` (default) or `en` |

```bash
# Romania population 2020-2024
curl "http://localhost:3000/api/v1/statistics/POP105A?territoryCode=RO&yearFrom=2020&yearTo=2024"

# All counties, grouped by territory
curl "http://localhost:3000/api/v1/statistics/POP105A?territoryLevel=NUTS3&year=2023&groupBy=territory"

# Filter by sex (male only)
curl "http://localhost:3000/api/v1/statistics/POP105A?classificationFilters={\"SEX\":[\"M\"]}"

# Include all classification breakdowns
curl "http://localhost:3000/api/v1/statistics/POP105A?includeAll=true"
```

**Response:**
```json
{
  "data": {
    "matrix": {
      "insCode": "POP105A",
      "name": "Populatia rezidenta...",
      "periodicity": ["ANNUAL"],
      "status": "SYNCED"
    },
    "series": [
      {
        "seriesId": "POP105A_series",
        "name": "Populatia rezidenta...",
        "dimensions": {
          "territory": {
            "id": 1,
            "code": "RO",
            "name": "TOTAL",
            "level": "NATIONAL"
          },
          "unit": {
            "id": 1,
            "code": "PERS",
            "name": "Numar persoane"
          }
        },
        "xAxis": {"name": "Period", "type": "STRING", "unit": ""},
        "yAxis": {"name": "Value", "type": "FLOAT", "unit": "Numar persoane"},
        "data": [
          {"x": "Anul 2020", "y": 19354339, "timePeriod": {"year": 2020, "periodicity": "ANNUAL"}},
          {"x": "Anul 2021", "y": 19229519, "timePeriod": {"year": 2021, "periodicity": "ANNUAL"}},
          {"x": "Anul 2022", "y": 19043098, "timePeriod": {"year": 2022, "periodicity": "ANNUAL"}},
          {"x": "Anul 2023", "y": 19055228, "timePeriod": {"year": 2023, "periodicity": "ANNUAL"}},
          {"x": "Anul 2024", "y": 19067576, "timePeriod": {"year": 2024, "periodicity": "ANNUAL"}}
        ]
      }
    ]
  },
  "meta": {
    "query": {
      "appliedFilters": {
        "territoryCode": "RO",
        "yearFrom": 2020,
        "yearTo": 2024
      }
    },
    "pagination": {"cursor": null, "hasMore": false, "limit": 100}
  }
}
```

#### Get Statistics Summary

Get aggregated summary statistics for a matrix.

```http
GET /api/v1/statistics/:matrixCode/summary
```

```bash
curl "http://localhost:3000/api/v1/statistics/POP105A/summary"
```

**Response:**
```json
{
  "data": {
    "matrix": {
      "insCode": "POP105A",
      "name": "Populatia rezidenta..."
    },
    "summary": {
      "totalRecords": 5,
      "timeRange": {"from": 2020, "to": 2024},
      "territoryLevels": [
        {"level": "NATIONAL", "count": 5}
      ],
      "valueStats": {
        "min": 19043098,
        "max": 19354339,
        "avg": 19149952,
        "sum": 95749760,
        "nullCount": 0
      }
    }
  }
}
```

---

### Analytics

#### Get Trends (Year-over-Year Growth)

```http
GET /api/v1/trends/:matrixCode
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `territoryCode` | string | No | Territory code (default: `RO`) |
| `territoryId` | number | No | Territory ID |
| `yearFrom` | number | No | Start year |
| `yearTo` | number | No | End year |

```bash
curl "http://localhost:3000/api/v1/trends/POP105A?territoryCode=RO&yearFrom=2020"
```

**Response:**
```json
{
  "data": {
    "matrixCode": "POP105A",
    "territory": {"code": "RO", "name": "TOTAL"},
    "trends": [
      {"year": 2020, "value": 19354339, "growthRate": null},
      {"year": 2021, "value": 19229519, "growthRate": -0.65},
      {"year": 2022, "value": 19043098, "growthRate": -0.97},
      {"year": 2023, "value": 19055228, "growthRate": 0.06},
      {"year": 2024, "value": 19067576, "growthRate": 0.06}
    ],
    "summary": {
      "startValue": 19354339,
      "endValue": 19067576,
      "totalChange": -286763,
      "totalChangePercent": -1.48,
      "averageGrowthRate": -0.37
    }
  }
}
```

#### Get Rankings

```http
GET /api/v1/rankings/:matrixCode
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `year` | number | **Yes** | Year to rank |
| `territoryLevel` | string | No | Territory level (default: `NUTS3`) |
| `order` | string | No | Sort order: `desc` (default) or `asc` |
| `limit` | number | No | Number of results (default: 10) |

```bash
curl "http://localhost:3000/api/v1/rankings/POP105A?year=2023&territoryLevel=NUTS3&limit=5"
```

**Response:**
```json
{
  "data": {
    "matrixCode": "POP105A",
    "year": 2023,
    "territoryLevel": "NUTS3",
    "rankings": [
      {"rank": 1, "territory": {"code": "B", "name": "Bucuresti"}, "value": 1883400, "percentile": 100},
      {"rank": 2, "territory": {"code": "CJ", "name": "Cluj"}, "value": 713500, "percentile": 97.6}
    ],
    "statistics": {
      "count": 42,
      "min": 210400,
      "max": 1883400,
      "mean": 453650
    }
  }
}
```

#### Get Distribution

```http
GET /api/v1/distribution/:matrixCode
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `year` | number | **Yes** | Year for distribution |
| `territoryLevel` | string | No | Territory level (default: `NUTS3`) |

```bash
curl "http://localhost:3000/api/v1/distribution/POP105A?year=2023"
```

**Response:**
```json
{
  "data": {
    "matrixCode": "POP105A",
    "year": 2023,
    "distribution": {
      "count": 42,
      "min": 210400,
      "max": 1883400,
      "mean": 453650,
      "median": 385200,
      "stdDev": 285300,
      "q1": 298400,
      "q3": 575400
    },
    "histogram": [
      {"from": 200000, "to": 400000, "count": 21},
      {"from": 400000, "to": 600000, "count": 12}
    ]
  }
}
```

#### Compare Territories

```http
GET /api/v1/compare/territories
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `matrixCode` | string | **Yes** | Matrix code |
| `year` | number | **Yes** | Year to compare |
| `territoryIds` | array | **Yes** | Territory IDs to compare (min 2) |

```bash
curl "http://localhost:3000/api/v1/compare/territories?matrixCode=POP105A&year=2023&territoryIds=14&territoryIds=15"
```

**Response:**
```json
{
  "data": {
    "matrixCode": "POP105A",
    "year": 2023,
    "territories": [
      {"id": 14, "code": "AB", "name": "Alba", "value": 323000},
      {"id": 15, "code": "AR", "name": "Arad", "value": 421000}
    ],
    "summary": {
      "min": 323000,
      "max": 421000,
      "range": 98000
    }
  }
}
```

#### Aggregate Data

```http
GET /api/v1/aggregate/:matrixCode
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `year` | number | **Yes** | Year to aggregate |
| `parentTerritoryId` | number | **Yes** | Parent territory ID |
| `groupBy` | string | No | Group by: `year`, `territory` |
| `aggregateFunction` | string | No | Function: `SUM`, `AVG`, `MIN`, `MAX` |

```bash
curl "http://localhost:3000/api/v1/aggregate/POP105A?year=2023&parentTerritoryId=14&groupBy=year"
```

**Response:**
```json
{
  "data": {
    "matrixCode": "POP105A",
    "year": 2023,
    "aggregateFunction": "SUM",
    "parent": {"id": 14, "code": "AB", "name": "Alba", "level": "NUTS3"},
    "aggregateByLevel": {},
    "directChildren": []
  }
}
```

#### Pivot Table

```http
GET /api/v1/pivot/:matrixCode
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `rowDimension` | string | **Yes** | Row dimension: `territory`, `time`, `classification` |
| `colDimension` | string | **Yes** | Column dimension: `territory`, `time`, `classification` |
| `yearFrom` | number | No | Start year |
| `yearTo` | number | No | End year |
| `territoryLevel` | string | No | Territory level filter |

```bash
curl "http://localhost:3000/api/v1/pivot/POP105A?rowDimension=time&colDimension=territory"
```

**Response:**
```json
{
  "data": {
    "matrixCode": "POP105A",
    "rowDimension": "time",
    "colDimension": "territory",
    "rows": [
      {"key": "2020", "label": "2020"},
      {"key": "2021", "label": "2021"},
      {"key": "2022", "label": "2022"},
      {"key": "2023", "label": "2023"},
      {"key": "2024", "label": "2024"}
    ],
    "columns": [
      {"key": "RO", "label": "TOTAL"}
    ],
    "values": {
      "2020": {"RO": "19354339"},
      "2021": {"RO": "19229519"},
      "2022": {"RO": "19043098"},
      "2023": {"RO": "19055228"},
      "2024": {"RO": "19067576"}
    }
  }
}
```

#### Correlate Matrices

```http
GET /api/v1/correlate
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `matrixCodeX` | string | **Yes** | First matrix code |
| `matrixCodeY` | string | **Yes** | Second matrix code |
| `year` | number | **Yes** | Year to correlate |
| `territoryLevel` | string | No | Territory level (default: `NUTS3`) |

```bash
curl "http://localhost:3000/api/v1/correlate?matrixCodeX=POP105A&matrixCodeY=SOM101B&year=2023"
```

**Response:**
```json
{
  "data": {
    "matrixX": {"code": "POP105A", "name": "Populatia rezidenta..."},
    "matrixY": {"code": "SOM101B", "name": "Someri inregistrati..."},
    "year": 2023,
    "territoryLevel": "NUTS3",
    "correlation": {
      "pearsonR": 0.73,
      "rSquared": 0.53,
      "regression": {"slope": 0.052, "intercept": 28500},
      "sampleSize": 42,
      "interpretation": "strong positive"
    }
  }
}
```

---

### Discovery

#### List Tags

```http
GET /api/v1/tags
```

```bash
curl "http://localhost:3000/api/v1/tags"
```

**Response:**
```json
{
  "data": {
    "tags": [
      {"id": 1, "name": "Populatie", "slug": "population", "category": "topic", "matrixCount": 87},
      {"id": 2, "name": "Demografie", "slug": "demographics", "category": "topic", "matrixCount": 45}
    ],
    "byCategory": {
      "topic": [...],
      "audience": [...],
      "use-case": [...]
    },
    "total": 16
  }
}
```

#### List Indicators

```http
GET /api/v1/indicators
```

```bash
curl "http://localhost:3000/api/v1/indicators"
```

**Response:**
```json
{
  "data": {
    "indicators": [
      {
        "id": 1,
        "code": "population-density",
        "name": "Densitatea populatiei",
        "formula": "population / area",
        "unitCode": "PERSONS",
        "category": "demographics",
        "config": {
          "requiredMatrices": ["POP105A", "GEO101A"]
        }
      }
    ],
    "byCategory": {
      "demographics": [...]
    },
    "total": 3
  }
}
```

---

## Error Handling

### Error Response Format

All errors follow this structure:

```json
{
  "error": "ERROR_CODE",
  "message": "Human-readable error message",
  "details": { ... },
  "requestId": "req-abc123"
}
```

### Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `NOT_FOUND` | 404 | Resource not found |
| `VALIDATION_ERROR` | 400 | Invalid request parameters |
| `NO_DATA` | 404 | No data found for query |
| `INTERNAL_ERROR` | 500 | Server error |

### Error Examples

**Resource Not Found:**
```bash
curl "http://localhost:3000/api/v1/matrices/INVALID_CODE"
```

```json
{
  "error": "NOT_FOUND",
  "message": "Matrix with code INVALID_CODE not found",
  "requestId": "req-1a"
}
```

**Validation Error:**
```bash
curl "http://localhost:3000/api/v1/rankings/POP105A"
# Missing required 'year' parameter
```

```json
{
  "error": "VALIDATION_ERROR",
  "message": "Invalid request parameters",
  "details": {
    "validation": [
      {
        "instancePath": "",
        "keyword": "required",
        "params": {"missingProperty": "year"},
        "message": "must have required property 'year'"
      }
    ]
  },
  "requestId": "req-2b"
}
```

**No Data Found:**
```bash
curl "http://localhost:3000/api/v1/statistics/POP105A?yearFrom=1900&yearTo=1901"
```

```json
{
  "error": "NO_DATA",
  "message": "No statistics found for the given query",
  "requestId": "req-3c"
}
```

---

## Pagination

The API uses **cursor-based pagination** for efficient navigation through large datasets.

### Pagination Response

```json
{
  "data": [...],
  "meta": {
    "pagination": {
      "cursor": "eyJpZCI6MTAwLCJzb3J0VmFsdWUiOiJQb3B1bGF0aWEifQ==",
      "hasMore": true,
      "limit": 50
    }
  }
}
```

| Field | Description |
|-------|-------------|
| `cursor` | Opaque string to fetch next page (or `null` if no more pages) |
| `hasMore` | `true` if more results exist |
| `limit` | Number of results per page |

### Using Pagination

**First page:**
```bash
curl "http://localhost:3000/api/v1/matrices?limit=10"
```

**Next page:**
```bash
curl "http://localhost:3000/api/v1/matrices?limit=10&cursor=eyJpZCI6MTAwLCJzb3J0VmFsdWUiOiJQb3B1bGF0aWEifQ=="
```

### Example: Fetching All Pages

```javascript
async function fetchAllMatrices() {
  const results = [];
  let cursor = null;
  
  do {
    const url = cursor 
      ? `http://localhost:3000/api/v1/matrices?limit=100&cursor=${cursor}`
      : `http://localhost:3000/api/v1/matrices?limit=100`;
    
    const response = await fetch(url);
    const data = await response.json();
    
    results.push(...data.data);
    cursor = data.meta.pagination.cursor;
    
  } while (data.meta.pagination.hasMore);
  
  return results;
}
```

---

## Common Use Cases

### 1. Get Population for a Specific County

```bash
# Get Cluj county population 2020-2024
curl "http://localhost:3000/api/v1/statistics/POP105A?territoryCode=CJ&yearFrom=2020&yearTo=2024"
```

### 2. Compare Multiple Counties

```bash
# First, get territory IDs
curl "http://localhost:3000/api/v1/territories?level=NUTS3&q=Cluj"
# Returns id: 20

curl "http://localhost:3000/api/v1/territories?level=NUTS3&q=Bihor"
# Returns id: 14

# Then compare
curl "http://localhost:3000/api/v1/compare/territories?matrixCode=POP105A&year=2023&territoryIds=14&territoryIds=20"
```

### 3. Get Population Trend Analysis

```bash
# Get year-over-year growth rates
curl "http://localhost:3000/api/v1/trends/POP105A?territoryCode=RO&yearFrom=2015&yearTo=2024"
```

### 4. Find Matrices with County Data

```bash
# Search for matrices with county-level data about education
curl "http://localhost:3000/api/v1/matrices?q=educatie&hasCountyData=true"
```

### 5. Get Statistical Distribution

```bash
# Get distribution of population across all counties
curl "http://localhost:3000/api/v1/distribution/POP105A?year=2023&territoryLevel=NUTS3"
```

### 6. Build a Pivot Table (Territory × Year)

```bash
curl "http://localhost:3000/api/v1/pivot/POP105A?rowDimension=time&colDimension=territory&yearFrom=2020&yearTo=2024"
```

### 7. Explore Available Data for a Matrix

```bash
# Step 1: Get matrix details
curl "http://localhost:3000/api/v1/matrices/POP105A"

# Step 2: Get available breakdowns
curl "http://localhost:3000/api/v1/matrices/POP105A/breakdowns"

# Step 3: Get data coverage
curl "http://localhost:3000/api/v1/matrices/POP105A/data-coverage"
```

---

## Appendix: SQL Patterns

For developers who want to understand the underlying queries or build custom integrations.

### Territory Path Filtering

```sql
-- Find all children of a territory using path prefix
SELECT * FROM territories
WHERE path LIKE 'RO.RO1.RO12.%';

-- Find all ancestors
SELECT * FROM territories
WHERE 'RO.RO1.RO12.CJ' LIKE path || '%'
ORDER BY LENGTH(path);
```

### Classification Filtering (Default TOTAL)

```sql
-- Return only TOTAL classification values (default behavior)
SELECT s.*
FROM statistics s
WHERE s.matrix_id = :matrix_id
  AND NOT EXISTS (
      SELECT 1 FROM statistic_classifications sc
      JOIN classification_values cv ON sc.classification_value_id = cv.id
      WHERE sc.statistic_id = s.id
        AND cv.code != 'TOTAL'
  );
```

### Ranking with Percentiles

```sql
SELECT
    t.code,
    t.name,
    s.value,
    ROW_NUMBER() OVER (ORDER BY s.value DESC) AS rank,
    PERCENT_RANK() OVER (ORDER BY s.value) * 100 AS percentile
FROM statistics s
JOIN territories t ON s.territory_id = t.id
JOIN time_periods tp ON s.time_period_id = tp.id
WHERE s.matrix_id = :matrix_id
  AND tp.year = :year
  AND t.level = 'NUTS3'
ORDER BY rank;
```

### Year-over-Year Growth Rate

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
)
SELECT
    year,
    value,
    CASE WHEN prev_value > 0
         THEN ROUND(((value - prev_value) / prev_value) * 100, 2)
         ELSE NULL
    END AS growth_rate_pct
FROM yearly_data
ORDER BY year;
```

### Correlation Between Two Matrices

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
)
SELECT
    CORR(m1.value1, m2.value2) AS pearson_r,
    COUNT(*) AS sample_size
FROM matrix1_data m1
JOIN matrix2_data m2 ON m1.territory_id = m2.territory_id
WHERE m1.value1 IS NOT NULL AND m2.value2 IS NOT NULL;
```

### Pivot Table Query

```sql
SELECT
    t.code AS territory_code,
    t.name AS territory_name,
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
GROUP BY t.id
ORDER BY t.name;
```

---

## Changelog

### Version 2.0 (2024-12-30)
- Restructured for client use
- Added Quick Start section
- Added cURL examples for all endpoints
- Added Error Handling section
- Added Pagination documentation
- Moved SQL patterns to appendix
- Clarified implemented vs planned endpoints

### Version 1.0 (Initial)
- Specification document for future implementation
