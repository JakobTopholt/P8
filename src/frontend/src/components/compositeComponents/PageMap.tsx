import { useState } from 'react'
import { MapContainer, TileLayer, CircleMarker, Popup, Polyline } from 'react-leaflet'
import SearchBar from './SearchBar'
import Randomizer from '../pageComponents/Randomizer'
import type { SearchMode } from '../pageComponents/Search'
import 'leaflet/dist/leaflet.css'
import './PageMap.css'

interface DataPoint {
  id: number
  mmsi: number
  position: [number, number]
  name: string
  location: string
  timestamp: string
  description?: string
}

export default function PageMap() {
  // Default center position
  const defaultCenter: [number, number] = [55.6761, 12.5683] // Copenhagen, Denmark
  const defaultZoom = 13
  const maxBounds: [[number, number], [number, number]] = [
    [-85.051129, -180],
    [85.051129, 180]
  ]

  // Sample data points - replace with actual data
  const [dataPoints] = useState<DataPoint[]>([
    {
      id: 1,
      mmsi: 211456780,
      position: [55.6761, 12.5683],
      name: 'Point 1',
      location: 'Copenhagen',
      timestamp: '2026-02-26T08:30:00Z',
      description: 'Sample data point 1'
    },
    {
      id: 2,
      mmsi: 211456780,
      position: [55.6861, 12.5783],
      name: 'Point 2',
      location: 'Copenhagen',
      timestamp: '2026-02-26T09:45:00Z',
      description: 'Sample data point 2'
    },
    {
      id: 3,
      mmsi: 219023456,
      position: [55.6661, 12.5583],
      name: 'Point 3',
      location: 'Copenhagen',
      timestamp: '2026-02-26T10:15:00Z',
      description: 'Sample data point 3'
    },
  ])
  const [rawDataPoints] = useState<DataPoint[]>([
    {
      id: 4,
      mmsi: 211456780,
      position: [55.6861, 12.6083],
      name: 'Point 4',
      location: 'Copenhagen Harbor',
      timestamp: '2026-02-26T08:10:00Z',
      description: 'Sample data point 1'
    },
    {
      id: 5,
      mmsi: 230451234,
      position: [55.6961, 12.5983],
      name: 'Point 5',
      location: 'Copenhagen Harbor',
      timestamp: '2026-02-26T09:20:00Z',
      description: 'Sample data point 2'
    },
    {
      id: 6,
      mmsi: 219023456,
      position: [55.6761, 12.5883],
      name: 'Point 6',
      location: 'Copenhagen Harbor',
      timestamp: '2026-02-26T10:05:00Z',
      description: 'Sample data point 3'
    },
  ])

  const [filteredDataPoints, setFilteredDataPoints] = useState<DataPoint[]>(dataPoints)
  const [bigDataPoints] = useState<DataPoint[]>(rawDataPoints)

  const handleRandomizer = (mmsis: number[]) => {
    if (mmsis.length === 0) {
      setFilteredDataPoints(dataPoints)
      return
    }
    setFilteredDataPoints(
      dataPoints.filter((point) => mmsis.includes(point.mmsi))
    )
  }

  const handleSearch = (query: string, mode: SearchMode) => {
    const trimmed = query.trim()

    if (trimmed === '') {
      setFilteredDataPoints(dataPoints)
      return
    }

    if (mode === 'ID') {
      const id = Number.parseInt(trimmed, 10)
      if (Number.isNaN(id)) {
        setFilteredDataPoints([])
        return
      }
      setFilteredDataPoints(dataPoints.filter((point) => point.id === id))
      return
    }

    if (mode === 'Time') {
      const queryTime = Date.parse(trimmed)
      if (Number.isNaN(queryTime)) {
        setFilteredDataPoints([])
        return
      }
      setFilteredDataPoints(
        dataPoints.filter((point) => Date.parse(point.timestamp) === queryTime)
      )
      return
    }

    const lowerQuery = trimmed.toLowerCase()
    setFilteredDataPoints(
      dataPoints.filter((point) => point.location.toLowerCase().includes(lowerQuery))
    )
  }

  // Build an array of positions for the polyline from bigDataPoints
  const bigDataPositions: [number, number][] = bigDataPoints.map((p) => p.position)
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
              color: '#3388ff',
              fillColor: '#3388ff',
              fillOpacity: 0.8,
              weight: 2
            }}
          >
            <Popup>
              <div>
                <h3>{point.name}</h3>
                {point.description && <p>{point.description}</p>}
              </div>
            </Popup>
          </CircleMarker>
        ))}
        {bigDataPoints.map((point) => (
          <CircleMarker
          key={point.id}
          center={point.position}
          radius={6}
          pathOptions={{
            color: '#676767',
            fillColor: '#676767',
            fillOpacity: 0.8,
            weight: 3,
            stroke: true
          }}>
            <Popup>
              <div>                
                <h3>{point.name}</h3>
                {point.description && <p>{point.description}</p>}
              </div>
            </Popup>
          </CircleMarker>
        ))}
        {bigDataPositions.length > 0 && (
          <Polyline positions={bigDataPositions} pathOptions={{ color: '#676767', weight: 2 }} />
        )}
      </MapContainer>
      <div className="search-bar-overlay">
        <SearchBar onSearch={handleSearch} />
      </div>
      <div className="randomizer-overlay">
        <Randomizer allDataPoints={dataPoints} onRandomize={handleRandomizer} />
      </div>
    </div>
  )
}
