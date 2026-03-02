import { useState, useEffect } from 'react'
import { MapContainer, TileLayer, CircleMarker, Popup, Polyline } from 'react-leaflet'
import SearchBar from './SearchBar'
import Randomizer from '../pageComponents/Randomizer'
import type { SearchMode } from '../pageComponents/Search'
import { getDataPointsByMMSIs } from '../../utils/api'
import 'leaflet/dist/leaflet.css'
import './PageMap.css'

interface DataPoint {
  id: number
  mmsi: string
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

  // All data comes from backend; no local sample data
  const [dataPoints] = useState<DataPoint[]>([])
  const [filteredDataPoints, setFilteredDataPoints] = useState<DataPoint[]>([])
  const [bigDataPoints] = useState<DataPoint[]>([])
  const [selectedMMSIs, setSelectedMMSIs] = useState<string[]>([])
  const [isLoadingMMSIData, setIsLoadingMMSIData] = useState(false)

  // Fetch datapoints when MMSIs are selected
  useEffect(() => {
    console.log('selectedMMSIs updated:', selectedMMSIs)
    if (selectedMMSIs.length === 0) {
      console.log('No MMSIs selected, clearing map')
      setFilteredDataPoints([])
      return
    }

    console.log('Fetching datapoints for MMSIs:', selectedMMSIs)
    setIsLoadingMMSIData(true)
    getDataPointsByMMSIs(selectedMMSIs)
      .then((fetchedPoints) => {
        console.log('Fetched datapoints from API:', fetchedPoints)
        if (fetchedPoints.length > 0) {
          // Convert fetched points to DataPoint format
          const mappedPoints = fetchedPoints.map((p, idx) => ({
            id: idx,
            mmsi: p.mmsi,
            position: [p.lat, p.lon] as [number, number],
            name: `Point ${idx + 1}`,
            location: 'Database',
            timestamp: p.timestamp,
            description: `MMSI: ${p.mmsi}`,
          }))
          console.log('Mapped points for display:', mappedPoints)
          setFilteredDataPoints(mappedPoints)
        } else {
          console.log('No data returned from backend for MMSIs:', selectedMMSIs)
          setFilteredDataPoints([])
        }
      })
      .catch((err) => {
        console.error('Error fetching datapoints:', err)
        setFilteredDataPoints([])
      })
      .finally(() => {
        setIsLoadingMMSIData(false)
      })
  }, [selectedMMSIs])

  const handleRandomizer = (mmsis: number[]) => {
    if (mmsis.length === 0) {
      setFilteredDataPoints(dataPoints)
      return
    }
    const mmsiStrings = mmsis.map((m) => m.toString())
    setSelectedMMSIs(mmsiStrings)
  }

  const handleSearch = (query: string, mode: SearchMode) => {
    const trimmed = query.trim()

    if (trimmed === '') {
      setFilteredDataPoints([])
      setSelectedMMSIs([])
      return
    }

    if (mode === 'ID') {
      // For ID mode, wait for MMSI selection via dropdown
      return
    }

    if (mode === 'Time') {
      // Time and Location search would need to query the backend
      // For now, just clear the display
      console.log('Time/Location search not yet implemented')
      setFilteredDataPoints([])
      return
    }

    if (mode === 'Location') {
      // Location search would need to query the backend
      console.log('Location search not yet implemented')
      setFilteredDataPoints([])
      return
    }
  }

  const handleMMSISelect = (mmsis: string[]) => {
    console.log('handleMMSISelect called with:', mmsis)
    setSelectedMMSIs(mmsis)
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
        <SearchBar onSearch={handleSearch} onMMSISelect={handleMMSISelect} />
      </div>
      <div className="randomizer-overlay">
        <Randomizer allDataPoints={dataPoints} onRandomize={handleRandomizer} />
      </div>
      {isLoadingMMSIData && <div className="loading-indicator">Loading datapoints...</div>}
    </div>
  )
}
