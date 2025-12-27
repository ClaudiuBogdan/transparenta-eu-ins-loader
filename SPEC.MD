## Overview

Before implementing the INS data integration, we need a complete understanding of INS's data model. The INS Tempo API uses a multi-dimensional OLAP cube structure that must be mapped to our relational entities.

## Background

INS (Institutul Național de Statistică) provides statistical data through their Tempo API. The API exposes a hierarchical catalog of datasets (matrices) with multiple dimensions (territorial, temporal, demographic, etc.).

## Tasks

- [ ] **Analyze INS Tempo API structure** using the discovery endpoints:
  - `http://statistici.insse.ro:8077/tempo-ins/context/` - root catalog
  - `http://statistici.insse.ro:8077/tempo-ins/context/{id}` - category details
  - `http://statistici.insse.ro:8077/tempo-ins/matrix/{code}` - dataset matrix
- [ ] **Document the hierarchical structure**: contexts → categories → matrices → dimensions
- [ ] **Identify all dimension types**:
  - Territorial (SIRUTA codes, counties, regions)
  - Temporal (annual, quarterly, monthly)
  - Indicators (population, employment, etc.)
  - Classifications (gender, age groups, area type)
- [ ] **Map INS territorial codes to SIRUTA codes** (critical for UAT linkage)
- [ ] **Document temporal granularity options** (annual, quarterly, monthly)
- [ ] **Identify datasets with UAT-level data** vs. county/national only
- [ ] **Create a data dictionary** with field descriptions
- [ ] **Study reference projects** for API parsing patterns:
  - [tempo.py](https://github.com/mark-veres/tempo.py) - Python client
  - [QTempo](https://github.com/alecsandrei/QTempo) - QGIS plugin (comprehensive coverage)
  - [Prometeu](https://github.com/gov2-ro/prometeu) - Romanian gov data tools

## Deliverables

1. Technical documentation explaining INS data model
2. Mapping table: INS territorial codes ↔ SIRUTA codes
3. List of datasets with UAT-level granularity
4. List of relevant statistical indicators for Transparenta.eu

## Acceptance Criteria

- [ ] Technical documentation explains cube structure clearly
- [ ] All dimension types identified and documented
- [ ] INS territorial codes mapped to SIRUTA
- [ ] UAT-level datasets identified

## Resources

- INS Tempo Online: <http://statistici.insse.ro:8077/tempo-online/>
- Gov2.ro Tempo Browser: <https://tempo-online.gov2.ro/2/category-browser.html>
