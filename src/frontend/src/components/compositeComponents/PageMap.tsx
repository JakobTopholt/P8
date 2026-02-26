import { useState } from 'react'
import { MapContainer, TileLayer, CircleMarker, Popup, Polyline } from 'react-leaflet'
import SearchBar from './SearchBar'
import 'leaflet/dist/leaflet.css'
import './PageMap.css'

interface DataPoint {
  id: number
  position: [number, number]
  name: string
  description?: string
}

export default function PageMap() {
  // Default center position (you can change this to your preferred location)
  const defaultCenter: [number, number] = [55.6761, 12.5683] // Copenhagen, Denmark
  const defaultZoom = 13

  // Sample data points - replace with your actual data
  const [dataPoints] = useState<DataPoint[]>([
    { id: 1, position: [55.6761, 12.5683], name: 'Point 1', description: 'Sample data point 1' },
    { id: 2, position: [55.6861, 12.5783], name: 'Point 2', description: 'Sample data point 2' },
    { id: 3, position: [55.6661, 12.5583], name: 'Point 3', description: 'Sample data point 3' },
  ])
  const [rawDataPoints] = useState<DataPoint[]>([
    { id: 1, position: [55.6861, 12.6083], name: 'Point 1', description: 'Sample data point 1' },
    { id: 2, position: [55.6961, 12.5983], name: 'Point 2', description: 'Sample data point 2' },
    { id: 3, position: [55.6761, 12.5883], name: 'Point 3', description: 'Sample data point 3' },
  ])

  // When implementing search filtering, use: const [filteredDataPoints, setFilteredDataPoints] = useState<DataPoint[]>(dataPoints)
  const [filteredDataPoints] = useState<DataPoint[]>(dataPoints)
  const [bigDataPoints] = useState<DataPoint[]>(rawDataPoints)

  // Build an array of positions for the polyline from bigDataPoints
  const bigDataPositions: [number, number][] = bigDataPoints.map((p) => p.position)
  return (
    <div className="page-map-container">
      <MapContainer 
        center={defaultCenter} 
        zoom={defaultZoom} 
        className="map-container"
        scrollWheelZoom={true}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
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
        <SearchBar />
      </div>
    </div>
  )
}
