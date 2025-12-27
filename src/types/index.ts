// INS Tempo API Types

export interface InsContext {
  id: number;
  code: string;
  name: string;
  level: number;
  parentCode: string | null;
  children?: InsContext[];
}

export interface InsMatrix {
  matrixName: string;
  matrixDescription: string;
  lastUpdate: string;
  startYear: number;
  endYear: number;
  dimensions: InsDimension[];
  matrixDetails: {
    nomJud: number;
    nomLoc: number;
  };
}

export interface InsDimension {
  dimensionId: number;
  dimensionName: string;
  options: InsDimensionOption[];
}

export interface InsDimensionOption {
  label: string;
  nomItemId: number;
  offset: number;
  parentId: number | null;
}

export interface InsQueryRequest {
  language: "ro" | "en";
  arr: InsDimensionOption[][];
  matrixName: string;
  matrixDetails: {
    nomJud: number;
    nomLoc: number;
  };
}

export interface InsDataCell {
  value: number | null;
  dimensions: Record<string, string>;
}

// SIRUTA Types

export interface SirutaEntry {
  siruta: string;
  denloc: string;
  jud: number;
  sirsup: string;
  tip: number;
  niv: number;
  med: number;
}
