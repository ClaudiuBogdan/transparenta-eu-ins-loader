# **Technical Architecture and Data Integration Strategy: INS Tempo Online OLAP-to-Relational Mapping**

## **1\. Executive Summary**

The integration of the National Institute of Statistics (INS) Tempo Online database into the *Transparenta.eu* platform represents a pivotal step in democratizing access to Romania’s socio-economic data. This comprehensive research report details the architectural requirements, data modeling strategies, and technical specifications necessary to ingest, transform, and expose INS statistical data. The primary challenge identified is the fundamental impedance mismatch between the source system—a multi-dimensional Online Analytical Processing (OLAP) cube structure—and the target system, which necessitates a normalized relational entity model to support User Acceptance Testing (UAT) and geospatial linkage.

The analysis reveals that the INS Tempo API (statistici.insse.ro:8077) does not function as a conventional RESTful service delivering flat resources. Instead, it operates as a query engine for a hierarchical catalog of "Contexts" (folders) and "Matrices" (hypercubes). Successful integration requires a client capable of recursive discovery to navigate the deep hierarchy of statistical domains, from "Population" and "Workforce" down to specific indicators. Furthermore, the client must dynamically parse the metadata of each Matrix to understand its dimensionality—specifically the variable axes of Time, Geography, and Classification—before constructing complex POST payloads to retrieve data slices.

A critical finding for the *Transparenta.eu* objectives is the distinct segmentation of data granularity. While the vast majority of economic indicators (e.g., GDP, Unemployment Rate, Inflation) are aggregated at the National (NUTS 0), Regional (NUTS 2), or County (NUTS 3\) levels, a specific "Golden Set" of matrices has been identified that provides data at the Local Administrative Unit (UAT/Localitate) level. These matrices—specifically POP107D (Population by Domicile), LOC103B (Housing Stock), and TUR105H (Tourism Capacity)—are the essential building blocks for local-level transparency.

However, utilizing these datasets presents a significant technical hurdle: the "SIRUTA Gap." The Tempo API frequently utilizes internal, non-standard identifiers for localities or simple text labels, rather than consistently exposing the official SIRUTA (Sistemul Informatic al Registrului Unităților Teritoriale-Administrative) codes required for geospatial interoperability with ANCPI boundary layers. This report outlines a rigorous algorithm for "Bridge Construction," utilizing fuzzy matching and dimension harvesting to map Tempo’s internal IDs to standard SIRUTA codes, thereby enabling the visualization of statistical indicators on the map of Romania.

## **2\. The INS Tempo Data Ecosystem**

To engineer a robust integration pipeline, one must first understand the theoretical and practical underpinnings of the source system. The INS Tempo Online platform serves as the central repository for official statistics in Romania, aggregating data from censuses, administrative sources, and sample-based surveys.

### **2.1 The OLAP Cube Paradigm**

Unlike transactional databases that store records row-by-row, the Tempo system is built upon the concept of the "Hypercube" or Matrix. In this paradigm, a statistical indicator is not a column in a table, but a cell in a multi-dimensional array.

* **The Matrix:** This is the fundamental unit of data storage (e.g., POP107D). It represents the entire cube.  
* **Dimensions (Axes):** These define the edges of the cube. A typical INS matrix has between 3 and 6 dimensions. Common dimensions include:  
  * **Temporal:** Years, Quarters, Months.  
  * **Territorial:** Macroregions, Development Regions, Counties, Localities.  
  * **Categorical:** Gender, Age Groups, Ownership Forms, NACE (CAEN) activities.  
  * **Measure:** The unit of measurement (e.g., "Number of persons", "Thousand RON").  
* **Cells (Values):** The intersection of a specific value from *every* dimension yields a single statistical figure.

The implication for the integration client is profound: one cannot simply "GET /population". The client must explicitly ask for "Population *FOR* Year 2023 *AND* County Bihor *AND* Gender Male *AND* Age Group 20-24". If any dimension is omitted, the query fails or returns an aggregation (if the API supports implicit totaling, which Tempo often does not without explicit "Total" selection). This structure mirrors the logic found in the reference Python client tempo.py, which requires a tuple-based query construction to target specific "LeafNodes" in the hierarchy.1

### **2.2 Hierarchical Data Catalog**

The data is organized in a deep, nested directory structure known as "Contexts."

* **Contexts:** These act as folders. They can contain other Contexts (sub-folders) or Matrices (files). The hierarchy is often 3-5 levels deep. For example: Social Statistics \-\> Population \-\> Demography \-\> Population by Domicile.  
* **Discovery:** The API provides endpoints to traverse this tree. A robust ETL (Extract, Transform, Load) process must perform a "Crawl" phase, recursively fetching children of each context until it identifies all available Matrices. This is superior to hardcoding Matrix IDs, as INS frequently moves datasets or adds new sub-categories.2

## **3\. API Architecture and Endpoint Analysis**

The technical interaction with the INS system relies on three primary categories of endpoints. These endpoints were analyzed based on the provided discovery URLs and patterns observed in open-source connectors.

### **3.1 The Discovery Layer**

The entry point for any automated harvester is the Context API.

* **Endpoint:** <http://statistici.insse.ro:8077/tempo-ins/context/>  
* **Method:** GET  
* **Function:** Returns the root-level nodes of the statistical tree.  
* **Response Structure:**  
  JSON  
    },  
   ...  
  \]

* **Recursive Traversal Strategy:** The integration logic must implement a function fetchContext(id). If the response contains a children array, the function calls itself for each child. If the response contains a matrices array (or similar leaf indicator), it registers those Matrix Codes for the next phase. This pattern is evident in the tempo.py library's Node.get\_all() implementation, which builds a complete tree of the database before allowing queries.1

### **3.2 The Definition Layer (Metadata)**

Once a Matrix Code (e.g., POP107D) is identified, the system must understand *how* to query it.

* **Endpoint:** <http://statistici.insse.ro:8077/tempo-ins/matrix/{code}>  
* **Method:** GET  
* **Function:** Retrieves the schema of the hypercube.  
* **Critical Metadata Fields:**  
  * matrixName: The immutable code.  
  * dimensionsMap: A complex object defining available axes.  
  * options: For each dimension, a list of valid values (Label and ID).  
  * details: Update frequency, methodology text, and contact person.4

**Analysis of dimensionsMap:** This is the most critical component for the "Data Dictionary" deliverable. It tells the ETL pipeline that, for example, Matrix POP107D has dimensions: "Sexe" (Gender), "Varste" (Ages), "Judete si Localitati" (Territorial), and "Ani" (Years). The options array within each dimension provides the mapping keys (e.g., NomItemId: 1125 \= Label: "Bihor").

### **3.3 The Query Layer (Data Extraction)**

To retrieve the actual data, the system must transition from GET to POST.

* **Endpoint:** <http://statistici.insse.ro:8077/tempo-ins/matrix/{code}>  
* **Method:** POST  
* Payload Construction:  
  The request body must be a JSON object specifying the "slice" of the cube required. Based on the behavior of tempo.py and standard OLAP APIs, the payload likely resembles:  
  JSON  
  {  
    "language": "ro",  
    "matrixName": "POP107D",  
    "query": }, // Male, Female  
      { "dimension": "Ani", "options": \[ "2022", "2023" \] },  
      { "dimension": "Judete", "options": \[ "all" \] } // or specific IDs  
    \]  
  }

  *Note:* The specific parameter names (query, arr, selection) vary by implementation, but the logic remains: one must supply a list of Dimension-Option pairs. The tempo.py library implements this via a leaf.query(...) method that accepts tuples of (DimensionName, OptionValue).1

Rate Limiting and Stability:  
Research into the reliability of the Tempo API suggests it can be fragile under heavy load. The QTempo plugin documentation implies that while the API is public, high-volume scraping should be throttled. Furthermore, the sheer size of some matrices (e.g., Population x 3000 UATs x 100 Ages x 2 Genders x 30 Years) generates millions of cells. The integration strategy must partition queries—fetching data year-by-year or county-by-county—to avoid timeout errors from the server.6

## **4\. Dimensional Analysis and Entity Mapping**

To satisfy the requirement of mapping the OLAP structure to relational entities, we must define how each Tempo dimension type translates into the Transparenta.eu database schema.

### **4.1 The "Time" Dimension (Temporal)**

Time in Tempo is not a continuous timeline but a discrete categorical dimension.

* **Granularity:**  
  * **Annual (Anual):** Labeled as "Anul 2020", "Anul 2021".  
  * **Quarterly (Trimestrial):** Labeled as "Trimestrul I Anul 2023".  
  * **Monthly (Lunar):** Labeled as "Luna Ianuarie Anul 2023".  
* **Mapping Challenge:** The database needs a standard DATE or DATETIME column.  
* **Transformation Logic:**  
  * "Anul YYYY" $\\rightarrow$ YYYY-01-01 (stored with a period\_type='annual' flag).  
  * "Trimestrul I Anul YYYY" $\\rightarrow$ YYYY-03-31 (End of Q1) or YYYY-01-01 (Start of Q1).  
  * "Luna \[MonthName\] Anul YYYY" $\\rightarrow$ YYYY-MM-01.  
  * *Note:* The extraction logic must parse Romanian month names (Ianuarie, Februarie...) into integers (1, 2...).

### **4.2 The "Territory" Dimension (Geospatial)**

This is the most critical dimension for the project.

* **Labels:** "Macroregiuni, regiuni de dezvoltare si judete" OR "Judete si localitati".  
* **Structure:** Hierarchical.  
  * **Level 1:** Macroregions (Macrorgiunea 1...).  
  * **Level 2:** Development Regions (Nord-Vest, Centru...).  
  * **Level 3:** Counties (Judetul Bihor, Judetul Cluj...).  
  * **Level 4:** Localities (Municipiul Oradea, Comuna Sanmartin...).  
* **Mapping Strategy:** These must map to a geographical\_units table referenced by SIRUTA codes. The complexity of this mapping is addressed in Section 5\.

### **4.3 The "Indicator" Dimension (Classification)**

These dimensions vary wildly between matrices and define *what* is being measured.

* **Examples:**  
  * **NACE/CAEN Codes:** Used in Workforce matrices (FOM103A). e.g., "Agriculture, forestry and fishing".  
  * **Ownership:** Used in Housing (LOC103B). e.g., "Private property", "Public property".  
  * **Demographic:** Gender, Age Groups (5-year cohorts or single years).  
* **Relational Design:**  
  * **Option A (EAV Model):** Store attributes in a key-value structure (dimension\_name, dimension\_value). Flexible but hard to query.  
  * **Option B (JSONB):** Store the dimension set as a JSON blob in a PostgreSQL jsonb column. Ideal for widely varying schemas.  
  * **Option C (Star Schema):** Create look-up tables for common dimensions (dim\_gender, dim\_nace) and link via foreign keys. Recommended for performance on high-volume indicators.

## **5\. The Territorial Analysis: SIRUTA Integration Strategy**

A primary requirement is mapping INS territorial codes to SIRUTA codes. This is critical because the Tempo API often uses internal surrogate keys (NomItemId) that do not match the official SIRUTA registry used by ANCPI for map geometries.

### **5.1 The Disconnect**

When querying POP107D, the API might return:

* NomItemId: 152  
* Label: "Municipiul Oradea"

However, the official SIRUTA code for Oradea is 26573\. The 152 is likely an internal index within the matrix definition, or a legacy code. Relying on NomItemId for geospatial joining will fail.7

### **5.2 The SIRUTA Mapping Algorithm**

To bridge this gap, the Transparenta.eu ETL pipeline must implement a **Name-Based Reconciliation** strategy, similar to the approach hinted at in the QTempo plugin which joins statistical tables to GISCO/ANCPI layers.6

Step 1: Ingest the Master SIRUTA Registry  
Load the official SIRUTA classification (available from data.gov.ro or INS publications) into a ref\_siruta table.

* Columns: siruta\_code (PK), name, county\_id, level (Municipality, Town, Commune, Village).

Step 2: Harvest Tempo Locations  
For each UAT-level matrix, download the full list of options for the "Judete si localitati" dimension.  
Step 3: Fuzzy Matching & Sanitization  
The matching logic must handle string discrepancies:

* **Normalization:** Strip prefixes like "Mun.", "Oras", "Com.", "Jud." from both Tempo labels and SIRUTA names.  
* **Diacritic Handling:** Normalize "ș/ş" and "ț/ţ" to standard forms, or strip diacritics entirely for comparison (e.g., "București" matches "Bucuresti").  
* **Parent Context:** Always match within the context of a County. There are multiple communes named "Nicolae Balcescu" in Romania. Matching must verify: Tempo.County \== SIRUTA.County AND Tempo.Locality \== SIRUTA.Locality.

Step 4: Persistence (The Bridge Table)  
Create a mapping table ins\_tempo\_siruta\_bridge:

| Column | Description |
| :---- | :---- |
| matrix\_code | e.g., 'POP107D' |
| tempo\_internal\_id | e.g., 152 |
| tempo\_label | e.g., 'Municipiul Oradea' |
| siruta\_code | e.g., 26573 |
| confidence\_score | 1.0 for exact matches, \<1.0 for fuzzy |

**Manual Review:** Any matches with low confidence must be flagged for manual review by a data steward. This is a one-time setup cost per matrix, but essential for data integrity.

### **5.3 Superior vs. Inferior SIRUTA**

It is crucial to note that INS Tempo statistics are almost exclusively reported at the **Administrative Unit (UAT)** level (Communes, Cities, Municipalities). This corresponds to the **SIRUTA Superior** codes.

* **UAT Level:** The administrative entity (e.g., Comuna Floresti).  
* **Village Level:** The component villages (e.g., Satul Luna de Sus).  
* **Integration Note:** Do not attempt to map Tempo data to Village-level SIRUTA codes (Inferior), as the data simply does not exist at that granularity. The map interface should color the entire Commune polygon based on the UAT value.9

## **6\. Granularity Analysis: Identifying UAT-Level Datasets**

A specific task of this research is to separate datasets available at the granular UAT level from those aggregated at the county or national level. This distinction is vital for setting user expectations on the *Transparenta.eu* platform.

### **6.1 The "Golden Set": UAT-Level Matrices**

These matrices are the highest priority for integration as they provide the hyper-local context required for transparency.

#### **6.1.1 POP107D \- Population by Domicile**

* **Description:** The legal population residing in the locality based on ID card address.  
* **Dimensions:** Year, Gender, Age Group (0-85+), County/Locality.  
* **Significance:** This is the *legal* denominator for per-capita calculations (e.g., "Local Budget per Capita"). Even if a person lives abroad, if their ID says "Videle", they are counted here.10  
* **Mapping:** Requires full SIRUTA linkage.

#### **6.1.2 LOC103B \- Housing Stock (Living Floor)**

* **Description:** Total living area (square meters) of dwellings at the end of the year.  
* **Dimensions:** Year, Ownership Form (Public/Private), County/Locality.  
* **Significance:** A key indicator of local wealth and development. Comparing "Public" vs. "Private" ownership at the local level offers insights into privatization and social housing availability.13

#### **6.1.3 TUR105H \- Tourism Capacity (Overnight Stays)**

* **Description:** Number of overnight stays in accommodation structures.  
* **Dimensions:** Year, Structure Type (Hotel, Motel, Pension), County/Locality.  
* **Significance:** Allows the platform to identify "Tourist Hotspots" at a granular level. Vital for correlating local budget revenues from tourism taxes with actual activity.9

#### **6.1.4 LOC104B \- Finished Dwellings**

* **Description:** Number of new dwellings completed during the year.  
* **Dimensions:** Year, Financing Source (Public funds/Private funds), County/Locality.  
* **Significance:** A proxy for economic growth and real estate dynamism.13

### **6.2 The Aggregated Set: County/Region Level**

Most economic and labor market indicators are *not* available at the UAT level due to the statistical methods used (sample-based surveys like AMIGO).

* **FOM103A (Civil Employment):** Available only at **County** level. Shows employment by NACE sector.  
* **AMG Series (Unemployment \- AMIGO):** Available at **Region/County** level.  
* **CON Series (GDP/National Accounts):** Available at **Region/County** level.  
* **IPC Series (Inflation):** Available at **National** level only.

**Implication for UI:** The platform must implement a "Fallback Granularity" logic. If a user selects a specific Commune (UAT) for an indicator like "Unemployment", the system should display the **County** average, clearly labeled as such, to avoid misleading the user.17

### **6.3 The "Resident" vs. "Domicile" Distinction**

A frequent source of confusion in Romanian statistics is the difference between:

1. **Population by Domicile (POP107D):** Based on ID cards. Available at **UAT**.  
2. **Usually Resident Population (POP105A):** Based on Census methodology (where people *actually* live). Available at **County**.

*Insight:* The POP105A dataset is considered more accurate for demographic analysis (migration, aging), but because it lacks UAT granularity in Tempo (except in Census years), POP107D remains the necessary default for local-level dashboards.5

## **7\. Data Dictionary and Indicator Reference**

The following table provides a precise data dictionary for the "Golden Set" of indicators relevant to *Transparenta.eu*, detailing the fields found in the Tempo metadata.

| INS Matrix Code | Indicator Name | Granularity | Update Frequency | Key Dimensions (Columns) | Measurement Unit | Utility for Transparency |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| **POP107D** | Legal Population | UAT | Annual | Varste (Ages), Sexe (Sex), Judete si localitati | Persons | Base denominator for all per-capita metrics. |
| **LOC103B** | Housing Area | UAT | Annual | Forme de proprietate (Ownership), Judete si localitati | Sq. Meters | Assessing housing density and public assets. |
| **TUR104E** | Tourist Arrivals | UAT | Annual/Monthly | Tipuri de structuri (Hotel/Pension), Judete si localitati | Persons | Measuring tourism flow. |
| **TUR105H** | Overnight Stays | UAT | Annual/Monthly | Tipuri de structuri, Judete si localitati | Stays | Measuring tourism economic impact. |
| **SAN101B** | Sanitary Units | UAT | Annual | Categorii de unitati (Hospitals/Clinics), Forme de proprietate | Units | Mapping healthcare infrastructure gaps. |
| **SCL101A** | Education Units | UAT | Annual | Niveluri de educatie (Primary/Secondary), Judete si localitati | Units | Mapping school network coverage. |
| **FOM103A** | Civil Employment | County | Annual | Activitati ale economiei (CANE Rev.2), Judete | Persons | Regional economic profiling. |
| **AGR101A** | Land Fund | County | Annual | Modul de folosinta (Arable/Pasture), Forme de proprietate | Hectares | Agricultural potential analysis. |

*Note on Field Descriptions:* In the API response, these dimensions appear in the dimensionsMap. The field nomItemId is the internal ID, and label is the human-readable text. The system must store both.

## **8\. Technical Implementation Roadmap**

Based on the analysis of reference projects (tempo.py, QTempo) and the API structure, the following implementation roadmap is proposed.

### **Phase 1: Discovery & Cataloging**

**Objective:** Build a local map of the Tempo universe.

1. **Crawler Script:** Develop a script to traverse <http://statistici.insse.ro:8077/tempo-ins/context/>.  
2. **Filter:** Store all Matrices found, but flag those containing "Localitati" or "Comune" in their metadata as **Priority 1**.  
3. **Metadata Store:** Persist the JSON schema of each matrix into a ins\_matrix\_definitions table.

### **Phase 2: The "Bridge" Construction**

**Objective:** Solve the SIRUTA mapping.

1. **Harvest:** For all Priority 1 matrices, extract the "Judete si Localitati" dimension options.  
2. **Match:** Run the fuzzy matching algorithm (Section 5.2) against the SIRUTA registry.  
3. **Validate:** Output a report of unmatched entities for manual correction.

### **Phase 3: Dynamic ETL Development**

**Objective:** Extract data without breaking the server.

1. **Partitioning:** Do not query "All Years" \+ "All Localities" in one POST. Partition by **Year** and **County**.  
   * *Loop:* For Year 2015 to 2024 \-\> For CountyID in Bridge \-\> Fetch Data.  
2. **Payload Generation:** Dynamically construct the POST body based on the dimensionsMap.  
   * *Reference:* Use the tempo.py logic where query is a list of objects defining the selected options.1  
3. **Error Handling:** Implement exponential backoff. The Tempo server is known to be slow or unresponsive during peak hours.  
4. **Sanitization:**  
   * Handle INS specific symbols:  
     * : (Missing data) \-\> Convert to NULL.  
     * \- (Not applicable) \-\> Convert to NULL.  
     * \* (Confidential) \-\> Convert to NULL (or flag as confidential).  
     * \<0.5 (Small values) \-\> Handle as string or round to 0\.

### **Phase 4: Reference Project Integration**

**Objective:** Leverage existing logic.

* **Python Client (tempo.py):** Use this library's Node traversal logic to handle the recursive discovery of contexts. It correctly models the parent-child relationships which can be brittle to implement from scratch.1  
* **QGIS Plugin (QTempo):** Review this plugin's source code (specifically how it joins data) to understand the "Pivot" logic required. Tempo data comes in "Long" format (Row per dimension combination); Transparenta.eu likely needs "Wide" format (Attributes as columns) for efficient querying.6

## **9\. Conclusion**

The integration of INS Tempo data is a complex but manageable engineering task. It moves beyond simple API consumption into the realm of Business Intelligence engineering. By treating the Tempo API as an OLAP source and building a rigorous translation layer—specifically focusing on the SIRUTA bridge and the "Golden Set" of UAT-level matrices—*Transparenta.eu* can successfully expose granular, meaningful data to the public.

The distinction between **POP107D** (Domicile) and **POP105A** (Residence), and the careful handling of the SIRUTA mapping, are the two most critical success factors. Failure to address the SIRUTA gap will result in "orphaned" data that cannot be placed on a map, while confusing the population definitions will lead to inaccurate per-capita financial metrics. The roadmap provided herein mitigates these risks through a structured, metadata-driven approach.

### ---

**Appendix A: Mapping Logic Code Snippet (Python Conceptual)**

Python

\# Conceptual logic for generating the Tempo POST payload  
def generate\_payload(matrix\_code, dimensions\_map, year, county\_id):  
    query\_list \=  

    \# Time Dimension  
    time\_dim \= next(d for d in dimensions\_map if "Ani" in d\['label'\])  
    query\_list.append({  
        "dimension": time\_dim\['label'\],  
        "options": \[year\]  
    })  
      
    \# Territorial Dimension  
    geo\_dim \= next(d for d in dimensions\_map if "Judete" in d\['label'\])  
    query\_list.append({  
        "dimension": geo\_dim\['label'\],  
        "options": \[county\_id\] \# Partitioning by county to reduce size  
    })  
      
    \# Select "Total" for other dimensions to flatten the cube  
    for dim in dimensions\_map:  
        if dim not in \[time\_dim, geo\_dim\]:  
            \# Logic to find the "Total" option ID from metadata  
            total\_option \= find\_total\_option(dim\['options'\])  
            query\_list.append({  
                "dimension": dim\['label'\],  
                "options": \[total\_option\]  
            })  
              
    return {  
        "language": "ro",  
        "matrixName": matrix\_code,  
        "query": query\_list  
    }

#### **Works cited**

1. mark-veres/tempo.py: A python library for working with the ... \- GitHub, accessed on December 27, 2025, [https://github.com/mark-veres/tempo.py](https://github.com/mark-veres/tempo.py)  
2. Statistical DB \- TEMPO-Online time series \- INSSE, accessed on December 27, 2025, [http://statistici.insse.ro/shop/?lang=en](http://statistici.insse.ro/shop/?lang=en)  
3. Statistical DB \- TEMPO-Online time series \- INSSE, accessed on December 27, 2025, [http://statistici.insse.ro/shop/?page=tempo2\&lang=en\&context=40](http://statistici.insse.ro/shop/?page=tempo2&lang=en&context=40)  
4. Statistical DB \- TEMPO-Online time series \- INSSE, accessed on December 27, 2025, [http://statistici.insse.ro/tempoins/index.jsp?page=tempo3\&lang=en\&ind=POP206A](http://statistici.insse.ro/tempoins/index.jsp?page=tempo3&lang=en&ind=POP206A)  
5. Statistical DB \- TEMPO-Online time series \- INSSE, accessed on December 27, 2025, [http://statistici.insse.ro/tempoins/index.jsp?page=tempo3\&lang=en\&ind=POP113A](http://statistici.insse.ro/tempoins/index.jsp?page=tempo3&lang=en&ind=POP113A)  
6. alecsandrei/QTempo: A QGIS plugin for accessing data from the TEMPO-Online statistical database \- GitHub, accessed on December 27, 2025, [https://github.com/alecsandrei/QTempo](https://github.com/alecsandrei/QTempo)  
7. accessed on December 27, 2025, [https://raw.githubusercontent.com/dinobby/ZS-BERT/master/resources/property\_list.html](https://raw.githubusercontent.com/dinobby/ZS-BERT/master/resources/property_list.html)  
8. ConvKBQA/subgraphs\_unsupervised.ipynb at master \- GitHub, accessed on December 27, 2025, [https://github.com/svakulenk0/ConvKBQA/blob/master/subgraphs\_unsupervised.ipynb](https://github.com/svakulenk0/ConvKBQA/blob/master/subgraphs_unsupervised.ipynb)  
9. romania \- World Bank Documents and Reports, accessed on December 27, 2025, [https://documents1.worldbank.org/curated/en/099012202252226051/pdf/P16792500815de0790b75404b84f68fd0ce.pdf](https://documents1.worldbank.org/curated/en/099012202252226051/pdf/P16792500815de0790b75404b84f68fd0ce.pdf)  
10. Statistical DB \- TEMPO-Online time series \- INSSE, accessed on December 27, 2025, [http://statistici.insse.ro/tempoins/index.jsp?page=tempo3\&lang=en\&ind=POP107D](http://statistici.insse.ro/tempoins/index.jsp?page=tempo3&lang=en&ind=POP107D)  
11. MORAVIAN GEOGRAPHICAL REPORTS \- geonika.cz, accessed on December 27, 2025, [https://www.geonika.cz/mgr/articles/MGR\_Volume\_31\_Issue\_2\_full.pdf](https://www.geonika.cz/mgr/articles/MGR_Volume_31_Issue_2_full.pdf)  
12. POP107D \- POPULATIA DUPA DOMICILIU la 1 ianuarie pe grupe de varsta si varste, sexe, judete si localitati \- INSSE \- Baze de date statistice \- TEMPO-Online serii de timp, accessed on December 27, 2025, [http://statistici.insse.ro/tempoins/index.jsp?page=tempo3\&lang=ro\&ind=POP107D](http://statistici.insse.ro/tempoins/index.jsp?page=tempo3&lang=ro&ind=POP107D)  
13. Statistical DB \- TEMPO-Online time series \- INSSE, accessed on December 27, 2025, [http://statistici.insse.ro/shop/?page=tempo2\&lang=en\&context=53](http://statistici.insse.ro/shop/?page=tempo2&lang=en&context=53)  
14. OUTPUT 2\. COMPANION PAPER 5 An analysis of public infrastructure shortage in suburban and peri-urban areas, with focus on a number of key indicators \- World Bank Documents and Reports, accessed on December 27, 2025, [https://documents1.worldbank.org/curated/en/099315002252216895/pdf/P1711760312d5203a0ac2e0dcfd977a73c2.pdf](https://documents1.worldbank.org/curated/en/099315002252216895/pdf/P1711760312d5203a0ac2e0dcfd977a73c2.pdf)  
15. Statistical DB \- TEMPO-Online time series \- INSSE, accessed on December 27, 2025, [http://statistici.insse.ro/tempoins/index.jsp?page=tempo3\&lang=en\&ind=LOC103B](http://statistici.insse.ro/tempoins/index.jsp?page=tempo3&lang=en&ind=LOC103B)  
16. Exploring the Relationship between Tourist Perception and Motivation at a Museum Attraction \- MDPI, accessed on December 27, 2025, [https://www.mdpi.com/2071-1050/16/1/370](https://www.mdpi.com/2071-1050/16/1/370)  
17. AMG165B \- AMIGO-Long-term unemployment rate (15-74 years), by level of education, by age groups ( comparable data ): Please select the criteria for your query \- INSSE, accessed on December 27, 2025, [http://statistici.insse.ro/tempoins/index.jsp?page=tempo3\&lang=en\&ind=AMG165B](http://statistici.insse.ro/tempoins/index.jsp?page=tempo3&lang=en&ind=AMG165B)  
18. Statistical DB \- TEMPO-Online time series \- INSSE, accessed on December 27, 2025, [http://statistici.insse.ro/tempoins/?page=tempo3\&lang=en\&ind=FOM103A](http://statistici.insse.ro/tempoins/?page=tempo3&lang=en&ind=FOM103A)  
19. Romania Open Data Inventory Profile, accessed on December 27, 2025, [https://odin.opendatawatch.com/Report/countryProfile/ROU?year=2020](https://odin.opendatawatch.com/Report/countryProfile/ROU?year=2020)  
20. Statistical DB \- TEMPO-Online time series \- INSSE, accessed on December 27, 2025, [http://statistici.insse.ro/tempoins/index.jsp?page=tempo3\&lang=en\&ind=POP105A](http://statistici.insse.ro/tempoins/index.jsp?page=tempo3&lang=en&ind=POP105A)
