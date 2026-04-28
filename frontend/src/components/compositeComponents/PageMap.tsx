import { useCallback, useEffect, useMemo, useState } from "react";
import {
  MapContainer,
  TileLayer,
  CircleMarker,
  Popup,
  Polyline,
  Polygon,
} from "react-leaflet";
import SearchBar from "./SearchBar";
import Randomizer from "../pageComponents/Randomizer";
import QueryLayerToggle from "../pageComponents/QueryLayerToggle";
import type { SearchMode } from "../pageComponents/Search";
import "leaflet/dist/leaflet.css";
import "./PageMap.css";

interface DataPoint {
  id: number;
  mmsi: number;
  position: [number, number];
  name: string;
  location: string;
  timestamp: string;
  description?: string;
}

interface QueryFeature {
  type: "Feature";
  properties: {
    query_id: number;
    query_type: string;
    lat_min: number;
    lat_max: number;
    lon_min: number;
    lon_max: number;
    time_start: number;
    time_end: number;
  };
  geometry: {
    type: "Polygon";
    coordinates: [number, number][][];
  };
}

const QUERY_COLORS: Record<string, string> = {
  range: "#e67e22",
  intersection: "#9b59b6",
  nearest: "#16a085",
  aggregation: "#c0392b",
};

export default function PageMap() {
  // Default center position
  const defaultCenter: [number, number] = [55.6761, 12.5683]; // Copenhagen, Denmark
  const defaultZoom = 13;
  const maxBounds: [[number, number], [number, number]] = [
    [-85.051129, -180],
    [85.051129, 180],
  ];

  const [dataPoints, setDataPoints] = useState<DataPoint[]>([]);
  const [filteredDataPoints, setFilteredDataPoints] = useState<DataPoint[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [queryFeatures, setQueryFeatures] = useState<QueryFeature[]>([]);
  const [visibleQueryTypes, setVisibleQueryTypes] = useState<Set<string>>(
    new Set(),
  );
  const [queryLoadError, setQueryLoadError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    const apiBase = import.meta.env.VITE_API_BASE_URL ?? "";

    const loadData = async () => {
      setIsLoading(true);
      setLoadError(null);
      try {
        const res = await fetch(`${apiBase}/api/datapoints?limit=200`, {
          signal: controller.signal,
        });

        if (!res.ok) {
          throw new Error(`Request failed with ${res.status}`);
        }

        const payload = (await res.json()) as DataPoint[];
        console.log(`Loaded ${payload.length} data points`);
        setDataPoints(payload);
        setFilteredDataPoints(payload);
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") {
          return;
        }
        setDataPoints([]);
        setFilteredDataPoints([]);
        setLoadError(error instanceof Error ? error.message : "Unknown error");
      } finally {
        setIsLoading(false);
      }
    };

    void loadData();

    return () => {
      controller.abort();
    };
  }, []);

  useEffect(() => {
    const controller = new AbortController();

    const loadQueries = async () => {
      try {
        const res = await fetch("/query_borders_2026-02-05-1000.geojson", {
          signal: controller.signal,
        });
        if (!res.ok) {
          throw new Error(`Request failed with ${res.status}`);
        }
        const payload = (await res.json()) as {
          features: QueryFeature[];
        };
        const features = payload.features ?? [];
        setQueryFeatures(features);
        const types = new Set(features.map((f) => f.properties.query_type));
        setVisibleQueryTypes(types);
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") {
          return;
        }
        setQueryFeatures([]);
        setVisibleQueryTypes(new Set());
        setQueryLoadError(
          error instanceof Error ? error.message : "Unknown error",
        );
      }
    };

    void loadQueries();

    return () => {
      controller.abort();
    };
  }, []);

  const queryTypes = useMemo(
    () =>
      Array.from(
        new Set(queryFeatures.map((f) => f.properties.query_type)),
      ).sort(),
    [queryFeatures],
  );

  const visibleQueryFeatures = useMemo(
    () =>
      queryFeatures.filter((f) =>
        visibleQueryTypes.has(f.properties.query_type),
      ),
    [queryFeatures, visibleQueryTypes],
  );

  const handleRandomizer = useCallback(
    (mmsis: number[]) => {
      if (mmsis.length === 0) {
        setFilteredDataPoints(dataPoints);
        return;
      }
      setFilteredDataPoints(
        dataPoints.filter((point) => mmsis.includes(point.mmsi)),
      );
    },
    [dataPoints],
  );

  const handleSearch = useCallback(
    (query: string, mode: SearchMode) => {
      const trimmed = query.trim();

      if (trimmed === "") {
        setFilteredDataPoints(dataPoints);
        return;
      }

      if (mode === "MMSI") {
        const mmsi = Number.parseInt(trimmed, 10);
        if (Number.isNaN(mmsi)) {
          setFilteredDataPoints([]);
          return;
        }
        setFilteredDataPoints(
          dataPoints.filter((point) => point.mmsi === mmsi),
        );
        return;
      }

      if (mode === "Time") {
        const queryTime = Date.parse(trimmed);
        if (Number.isNaN(queryTime)) {
          setFilteredDataPoints([]);
          return;
        }
        setFilteredDataPoints(
          dataPoints.filter(
            (point) => Date.parse(point.timestamp) === queryTime,
          ),
        );
        return;
      }

      const lowerQuery = trimmed.toLowerCase();
      setFilteredDataPoints(
        dataPoints.filter((point) =>
          point.location.toLowerCase().includes(lowerQuery),
        ),
      );
    },
    [dataPoints],
  );

  const polylinesByMmsi: [number, number][][] = useMemo(() => {
    const grouped = new Map<
      number,
      { position: [number, number]; time: number }[]
    >();

    for (const point of filteredDataPoints) {
      const time = Date.parse(point.timestamp);
      const list = grouped.get(point.mmsi) ?? [];
      list.push({
        position: point.position,
        time: Number.isNaN(time) ? 0 : time,
      });
      grouped.set(point.mmsi, list);
    }

    return Array.from(grouped.values())
      .map((points) =>
        points.sort((a, b) => a.time - b.time).map((entry) => entry.position),
      )
      .filter((positions) => positions.length > 1);
  }, [filteredDataPoints]);

  return (
    <div className="page-map-container">
      <MapContainer
        center={defaultCenter}
        zoom={defaultZoom}
        className="map-container"
        scrollWheelZoom={true}
        minZoom={2}
        maxBounds={maxBounds}
        maxBoundsViscosity={1.0}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          noWrap={true}
        />
        {filteredDataPoints.map((point) => (
          <CircleMarker
            key={point.id}
            center={point.position}
            radius={6}
            pathOptions={{
              color: "#3388ff",
              fillColor: "#3388ff",
              fillOpacity: 0.8,
              weight: 2,
            }}
          >
            <Popup>
              <div>
                <h3>{point.name}</h3>
                <p>MMSI: {point.mmsi}</p>
                {point.description && <p>{point.description}</p>}
              </div>
            </Popup>
          </CircleMarker>
        ))}
        {polylinesByMmsi.map((positions, index) => (
          <Polyline
            key={`mmsi-line-${index}`}
            positions={positions}
            pathOptions={{ color: "#676767", weight: 2 }}
          />
        ))}
        {visibleQueryFeatures.map((feature) => {
          const ring = feature.geometry.coordinates[0].map<
            [number, number]
          >(([lon, lat]) => [lat, lon]);
          const color =
            QUERY_COLORS[feature.properties.query_type] ?? "#555";
          return (
            <Polygon
              key={`query-${feature.properties.query_id}`}
              positions={ring}
              pathOptions={{
                color,
                fillColor: color,
                fillOpacity: 0.15,
                weight: 2,
              }}
            >
              <Popup>
                <div>
                  <h3 style={{ textTransform: "capitalize", margin: 0 }}>
                    {feature.properties.query_type}
                  </h3>
                  <p>Query ID: {feature.properties.query_id}</p>
                  <p>
                    Time: {feature.properties.time_start} –{" "}
                    {feature.properties.time_end}
                  </p>
                </div>
              </Popup>
            </Polygon>
          );
        })}
      </MapContainer>
      {isLoading && (
        <div className="map-status-overlay">Loading DataPoints...</div>
      )}
      {loadError && (
        <div className="map-status-overlay map-status-overlay-error">
          Failed to load DataPoints: {loadError}
        </div>
      )}
      {queryLoadError && (
        <div className="map-status-overlay map-status-overlay-error">
          Failed to load queries: {queryLoadError}
        </div>
      )}
      <div className="search-bar-overlay">
        <SearchBar onSearch={handleSearch} />
      </div>
      <div className="randomizer-overlay">
        <Randomizer allDataPoints={dataPoints} onRandomize={handleRandomizer} />
      </div>
      <div className="query-layer-overlay">
        <QueryLayerToggle
          queryTypes={queryTypes}
          visibleTypes={visibleQueryTypes}
          colors={QUERY_COLORS}
          onChange={setVisibleQueryTypes}
        />
      </div>
    </div>
  );
}
