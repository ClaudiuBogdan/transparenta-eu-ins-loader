# Postgres Schema Design

We need to design a sql schema for a postgres database that maps the domain from ins to out custom needs. we want to keep full compatibility with the ins data structure, but we also want to optimize the schema for performance and scalability, simplicity and improve the data integrity and standardization.

<ins>
This is the ins data structure:
  1. Core Data Model: OLAP Cube Structure

  The INS Tempo system is a multi-dimensional OLAP database, not a traditional relational database:

  Context (Domain)           → 8 top-level domains (A-H)
    └── Context (Category)   → ~340 subcategories
          └── Matrix         → ~1,898 datasets (hypercubes)
                └── Dimension 1..N (3-6 dimensions per matrix)
                      └── Options (dimension values with nomItemId)

  ---

  1. Entity Types

  | Entity           | Description                  | Key Fields                                               |
  |------------------|------------------------------|----------------------------------------------------------|
  | Context          | Hierarchical folder/category | code, name, level, parentCode, childrenUrl               |
  | Matrix           | Statistical dataset (cube)   | code (e.g., POP105A), matrixName, dimensionsMap, details |
  | Dimension        | Axis of the cube             | dimCode, label, options[]                                |
  | Dimension Option | Value on a dimension         | nomItemId, label, offset, parentId                       |

  ---

  1. Dimension Types

  | Type            | Examples                                                                | Notes                                                          |
  |-----------------|-------------------------------------------------------------------------|----------------------------------------------------------------|
  | Temporal        | "Ani", "Perioade"                                                       | Values: "Anul 2023", "Trimestrul I 2024", "Luna Ianuarie 2024" |
  | Territorial     | "Macroregiuni, regiuni de dezvoltare si judete", "Localitati"           | Hierarchical: Macroregion → Region → County → Locality         |
  | Classification  | "Sexe", "Varste si grupe de varsta", "Medii de rezidenta", "CAEN Rev.2" | Categorical breakdowns                                         |
  | Unit of Measure | "UM: Numar persoane", "UM: Ha"                                          | Single-value dimension indicating measurement unit             |

  ---

  1. Territorial Hierarchy (NUTS + LAU)

  TOTAL (National)
  ├── MACROREGIUNEA UNU (NUTS 1)
  │   ├── Regiunea NORD-VEST (NUTS 2)
  │   │   ├── Bihor (NUTS 3 / County)
  │   │   │   ├── 26573 Oradea (LAU / UAT with SIRUTA)
  │   │   │   ├── 38731 Ripiceni
  │   │   │   └── ...
  │   │   └── Cluj
  │   └── Regiunea CENTRU
  └── MACROREGIUNEA DOI
      └── ...

  Key insight: Localities include SIRUTA codes in their labels (e.g., "38731 Ripiceni").

  ---

  1. Matrix Metadata Flags (details object)

  | Flag       | Type | Meaning                               |
  |------------|------|---------------------------------------|
  | nomJud     | int  | County dimension index (0 = none)     |
  | nomLoc     | int  | Locality dimension index (0 = none)   |
  | matMaxDim  | int  | Number of dimensions (3-6)            |
  | matSiruta  | bool | Includes SIRUTA codes in output       |
  | matCaen1/2 | bool | Uses CAEN Rev.1/2 classification      |
  | matRegJ    | int  | Regional dimension index              |
  | matTime    | int  | Time dimension index                  |
  | matActive  | bool | Dataset is active                     |
  | matUMSpec  | bool | Has special unit of measure dimension |

  ---

  1. Data Granularity

  | Level                     | Available Datasets | Examples                                                               |
  |---------------------------|--------------------|------------------------------------------------------------------------|
  | UAT/Locality (nomLoc > 0) | ~50 matrices       | POP107D (population), LOC103B (housing), TUR104E/TUR105H (tourism)     |
  | County (nomJud > 0)       | Majority           | SOM101E (unemployment), FOM103A (employment), most economic indicators |
  | National/Regional         | Some               | IPC (inflation), CON (GDP)                                             |

  ---

  1. Query/Pivot Response Format

  The /pivot endpoint returns CSV-formatted text:

  Varste si grupe de varsta, Sexe, Medii de rezidenta, Judete, Ani, UM: Numar persoane, Valoare
  Total, Total, Total, TOTAL, Anul 2023, Numar persoane, 19053815
  Total, Masculin, Urban, Bihor, Anul 2023, Numar persoane, 144439

  Special values: : (missing), - (N/A), * (confidential), <0.5 (small value)

  ---

  1. Key Integration Challenges

  | Challenge                                           | Solution                                       |
  |-----------------------------------------------------|------------------------------------------------|
  | SIRUTA Gap: Internal nomItemId ≠ official SIRUTA    | Parse locality labels to extract SIRUTA prefix |
  | Hierarchical dimensions: Parent-child relationships | Use parentId to build hierarchy, or flatten    |
  | 30,000 cell limit                                   | Chunk queries by year or county                |
  | Multiple periodicities                              | Normalize to date/period representation        |

More detailes here: /Users/claudiuconstantinbogdan/projects/devostack/statistical-data-ins/INS_API_SPEC.md
</ins>

For the hierarchy, I want to use a computed field that make it easy to filter, like a string path with a separator, like this: "::". This way we can filter by using a prefix filter. We could still have a parent_id field for the hierarchy, but we need to generate this path format to make it easy to filter the data.

We need to decide how are we going to model the dimensions. We need to keep the full compatibility with all the dimensions types that are available in the ins data structure, but we also need to optimize and standardize the data, specially the temporal dimensions, territorial, etc.

We can iterate over the solution. Ask as much questions as you need using the questions tool.

This is a new schema, that build on the learning from the previous schema.

---

I want to receive a reference link on the api query, to make it easy to open the ins data link and verify the data. Analyze what endpoints can provide this link and add it to the api response.
