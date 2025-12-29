---
name: ins-data
description: Query Romanian statistical data from INS Tempo API. Use this skill when the user asks for population, demographics, education, employment, or other Romanian statistics by locality, county, or nationally.
---

# INS Tempo Data Query Skill

This skill helps you query Romanian statistical data from the INS (Institutul Național de Statistică) Tempo API using the project CLI.

## Available CLI Commands

```bash
# Search for matrices (datasets) by keyword
pnpm cli matrices --search <keyword>

# View matrix details and dimensions
pnpm cli matrix <MATRIX_CODE>

# View dimension options (to find nomItemId values)
pnpm cli dimensions <MATRIX_CODE> <DIMENSION_INDEX>

# Interactive query builder
pnpm cli query <MATRIX_CODE>
```

## Common Matrices

| Code | Description | Has UAT Data |
|------|-------------|--------------|
| `POP107D` | Population by localities (with SIRUTA) | Yes |
| `POP105A` | Population by counties | No |
| `SCL103B` | Pre-university students by county | No |
| `SOM101B` | Unemployment by county | No |
| `FOR101B` | Labor force by county | No |

## Query Building Process

### Step 1: Find the Matrix
Search for relevant matrices:
```bash
pnpm cli matrices --search "populatie"
pnpm cli matrices --search "elevi"
pnpm cli matrices --uat  # Only UAT-level data
```

### Step 2: Examine Matrix Structure
```bash
pnpm cli matrix POP107D
```

### Step 3: Get Dimension Options
Each dimension has options with `nomItemId` values needed for queries:
```bash
pnpm cli dimensions POP107D 0  # Age groups
pnpm cli dimensions POP107D 1  # Sex
pnpm cli dimensions POP107D 2  # Counties
pnpm cli dimensions POP107D 3  # Localities
pnpm cli dimensions POP107D 4  # Years
pnpm cli dimensions POP107D 5  # Unit of measure
```

### Step 4: Execute Direct API Query
Build `encQuery` with colon-separated `nomItemId` values (one group per dimension):

```bash
curl -s -X POST 'http://statistici.insse.ro:8077/tempo-ins/pivot' \
  -H 'Content-Type: application/json' \
  -d '{
    "encQuery": "1:105:3095:2444:4285,4304,4323:9685",
    "language": "ro",
    "matCode": "POP107D",
    "matMaxDim": 6,
    "matRegJ": 0,
    "matUMSpec": 0
  }'
```

## Common nomItemId Values

### Totals (Aggregates)
- Age Total: `1`
- Sex Total: `105`
- County Total: `112`

### Counties (Judete)
| County | nomItemId |
|--------|-----------|
| Alba | 3064 |
| Arad | 3065 |
| Brasov | 3071 |
| Bucuresti | 3104 |
| Cluj | 3075 |
| Constanta | 3076 |
| Iasi | 3085 |
| Sibiu | 3095 |
| Timis | 3098 |

### Key Localities (nomItemId)
| Locality | nomItemId | SIRUTA |
|----------|-----------|--------|
| Municipiul Sibiu | 2444 | 143450 |
| Municipiul Cluj-Napoca | 1920 | 125150 |
| Municipiul Bucuresti | 370 | 179141 |

### Years
Years from 1992-2025 have nomItemIds starting at 4285, incrementing by 19:
- 1992: 4285
- 1993: 4304
- 2000: 4437
- 2010: 4627
- 2020: 4817
- 2024: 4893
- 2025: 4912

### Unit of Measure
- Number of persons: `9685`
- Thousands of persons: `9686`

## Example Queries

### Total Population History for Municipiul Sibiu (1992-2025)
```bash
curl -s -X POST 'http://statistici.insse.ro:8077/tempo-ins/pivot' \
  -H 'Content-Type: application/json' \
  -d '{
    "encQuery": "1:105:3095:2444:4285,4304,4323,4342,4361,4380,4399,4418,4437,4456,4475,4494,4513,4532,4551,4570,4589,4608,4627,4646,4665,4684,4703,4722,4741,4760,4779,4798,4817,4836,4855,4874,4893,4912:9685",
    "language": "ro",
    "matCode": "POP107D",
    "matMaxDim": 6,
    "matRegJ": 0,
    "matUMSpec": 0
  }'
```

### Population by County (All Counties, Latest Year)
```bash
curl -s -X POST 'http://statistici.insse.ro:8077/tempo-ins/pivot' \
  -H 'Content-Type: application/json' \
  -d '{
    "encQuery": "1:105:112:4912:9685",
    "language": "ro",
    "matCode": "POP105A",
    "matMaxDim": 5,
    "matRegJ": 0,
    "matUMSpec": 0
  }'
```

## Response Format
The API returns CSV data:
```csv
Varste si grupe de varsta, Sexe, Judete, Localitati, Ani, UM: Numar persoane, Valoare
Total, Total, Sibiu, 143450 MUNICIPIUL SIBIU, Anul 1992, Numar persoane, 170324
```

- Last column `Valoare` contains the numeric value
- Missing data is represented as `::`

## Constraints

- **Rate limit**: Wait 750ms between API requests
- **Cell limit**: Maximum 30,000 cells per query (product of all selected options)
- **Chunking**: For large queries, split by years or locations

## Workflow for User Requests

1. **Identify data type**: population, students, employment, etc.
2. **Find matrix**: Use `pnpm cli matrices --search`
3. **Get structure**: Use `pnpm cli matrix <CODE>`
4. **Find IDs**: Use `pnpm cli dimensions <CODE> <N>` for each dimension
5. **Build query**: Construct encQuery with nomItemId values
6. **Execute**: Use curl to POST to the pivot endpoint
7. **Present**: Parse CSV and display results in a table
