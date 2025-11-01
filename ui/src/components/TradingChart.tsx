import { useEffect, useRef } from "react";
import type {
  HistogramSeriesPartialOptions,
  IChartApi,
  ISeriesApi,
  UTCTimestamp,
} from "lightweight-charts";
import type { ChartCandle } from "../lib/api";

type TradingChartProps = {
  candles: ChartCandle[];
  timeframe: string;
};

type LightweightChartsNamespace = typeof import("lightweight-charts");

function toTimestamp(value: string): UTCTimestamp {
  return Math.floor(new Date(value).getTime() / 1000) as UTCTimestamp;
}

function resolveCssVar(name: string, fallback: string): string {
  if (typeof window === "undefined") {
    return fallback;
  }
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

let lightweightChartsPromise: Promise<LightweightChartsNamespace> | null = null;

async function loadLightweightCharts(): Promise<LightweightChartsNamespace> {
  if (typeof window === "undefined") {
    throw new Error("LightweightCharts requires a browser environment");
  }

  if (window.LightweightCharts) {
    return window.LightweightCharts;
  }

  if (!lightweightChartsPromise) {
    lightweightChartsPromise = (async () => {
      const scriptUrl = import.meta.env.DEV
        ? "https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.development.js"
        : "https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.production.js";

      await new Promise<void>((resolve, reject) => {
        const existing = document.querySelector<HTMLScriptElement>("script[data-lightweight-charts]");
        if (existing) {
          if (existing.dataset.loaded === "true") {
            resolve();
            return;
          }
          existing.addEventListener(
            "load",
            () => {
              existing.dataset.loaded = "true";
              resolve();
            },
            { once: true },
          );
          existing.addEventListener(
            "error",
            () => reject(new Error("Failed to load lightweight-charts script")),
            { once: true },
          );
          return;
        }

        const script = document.createElement("script");
        script.src = scriptUrl;
        script.async = true;
        script.defer = true;
        script.dataset.lightweightCharts = "true";
        script.addEventListener(
          "load",
          () => {
            script.dataset.loaded = "true";
            resolve();
          },
          { once: true },
        );
        script.addEventListener(
          "error",
          () => reject(new Error("Failed to load lightweight-charts script")),
          { once: true },
        );
        document.head.appendChild(script);
      });

      if (!window.LightweightCharts) {
        throw new Error("LightweightCharts global missing after script load");
      }

      return window.LightweightCharts;
    })();
  }

  const namespace = await lightweightChartsPromise;
  if (!namespace) {
    throw new Error("Unable to load LightweightCharts");
  }
  return namespace;
}

export function TradingChart({ candles, timeframe }: TradingChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) {
      return;
    }

    let disposed = false;
    let chart: IChartApi | null = null;
    let ro: ResizeObserver | null = null;

    async function setup() {
      try {
        const { createChart, CrosshairMode, ColorType } = await loadLightweightCharts();

        if (disposed || !containerRef.current) {
          return;
        }

        const textColor = resolveCssVar("--muted", "#94a3b8");

        chart = createChart(containerRef.current, {
          width: containerRef.current.clientWidth,
          height: 420,
          layout: {
            background: { type: ColorType.Solid, color: "transparent" },
            textColor,
            fontSize: 12,
            fontFamily: "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
          },
          grid: {
            vertLines: { color: "rgba(255,255,255,0.05)" },
            horzLines: { color: "rgba(255,255,255,0.05)" },
          },
          crosshair: { mode: CrosshairMode.Normal },
          timeScale: {
            borderVisible: false,
            secondsVisible: timeframe.endsWith("s"),
          },
          rightPriceScale: {
            borderVisible: false,
          },
        });

        const candleSeries = chart.addCandlestickSeries({
          upColor: "#26a69a",
          borderUpColor: "#26a69a",
          wickUpColor: "#26a69a",
          downColor: "#ef5350",
          borderDownColor: "#ef5350",
          wickDownColor: "#ef5350",
          priceLineVisible: false,
        });

        const volumeOptions: HistogramSeriesPartialOptions = {
          color: "rgba(38,166,154,0.5)",
          priceFormat: { type: "volume" },
          priceScaleId: "volume",
        };
        const volumeSeries = chart.addHistogramSeries(volumeOptions);
        volumeSeries.priceScale().applyOptions({
          scaleMargins: { top: 0.8, bottom: 0 },
          borderVisible: false,
        });

        if (typeof ResizeObserver !== "undefined") {
          ro = new ResizeObserver((entries) => {
            const entry = entries[0];
            if (chart) {
              chart.applyOptions({
                width: entry.contentRect.width,
                height: entry.contentRect.height,
              });
              chart.timeScale().fitContent();
            }
          });
          ro.observe(containerRef.current);
        }

        chartRef.current = chart;
        candleSeriesRef.current = candleSeries;
        volumeSeriesRef.current = volumeSeries;
      } catch (error) {
        if (process.env.NODE_ENV !== "production") {
          // eslint-disable-next-line no-console
          console.error("[TradingChart] failed to initialize chart", error);
        }
      }
    }

    setup();

    return () => {
      disposed = true;
      if (ro) {
        ro.disconnect();
        ro = null;
      }
      if (chartRef.current && typeof chartRef.current.remove === "function") {
        chartRef.current.remove();
        chartRef.current = null;
      }
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
    };
  }, [timeframe]);

  useEffect(() => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    const volumeSeries = volumeSeriesRef.current;
    if (!chart || !candleSeries || !volumeSeries) {
      return;
    }
    if (!candles.length) {
      candleSeries.setData([]);
      volumeSeries.setData([]);
      return;
    }

    const candleData = candles.map((candle) => ({
      time: toTimestamp(candle.ts),
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
    }));
    candleSeries.setData(candleData);

    const volumeData = candles.map((candle) => ({
      time: toTimestamp(candle.ts),
      value: candle.volume,
      color: candle.close >= candle.open ? "rgba(38,166,154,0.6)" : "rgba(239,83,80,0.6)",
    }));
    volumeSeries.setData(volumeData);

    chart.timeScale().applyOptions({ secondsVisible: timeframe.endsWith("s") });
    chart.timeScale().fitContent();
  }, [candles, timeframe]);

  return <div ref={containerRef} className="chart-surface__canvas" />;
}

export default TradingChart;
