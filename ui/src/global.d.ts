/// <reference types="vite/client" />

declare interface CoverageRow {
  symbol: string;
  total_required: number;
  received: number;
  latest_ts: string;
}
declare interface CoverageRow {
  symbol: string;
  total_required: number;
  received: number;
  latest_ts: string;
}

declare module "*.js?url" {
  const url: string;
  export default url;
}

declare module "lightweight-charts/dist/lightweight-charts.standalone.production.js?url" {
  const url: string;
  export default url;
}

declare module "lightweight-charts/dist/lightweight-charts.standalone.development.js?url" {
  const url: string;
  export default url;
}

declare type LightweightChartsNamespace = typeof import("lightweight-charts");

declare global {
  interface Window {
    LightweightCharts?: LightweightChartsNamespace;
  }
}

export {};
