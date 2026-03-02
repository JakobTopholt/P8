const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

console.log('API_BASE_URL:', API_BASE_URL)

export interface MmsiSearchResponse {
  query: string
  count: number
  results: string[]
}

export interface MmsiDataPoint {
  id: number
  mmsi: string
  lat: number
  lon: number
  timestamp: string
  [key: string]: unknown
}

export async function searchMMSIs(query: string, limit: number = 50): Promise<MmsiSearchResponse> {
  const params = new URLSearchParams({
    q: query,
    limit: limit.toString(),
  })
  const url = `${API_BASE_URL}/api/mmsi/search?${params}`
  console.log('Calling searchMMSIs:', url)
  try {
    const response = await fetch(url)
    console.log('searchMMSIs response status:', response.status)
    if (!response.ok) {
      throw new Error(`Failed to search MMSIs: ${response.statusText}`)
    }
    const data = await response.json()
    console.log('searchMMSIs response data:', data)
    return data
  } catch (error) {
    console.error('searchMMSIs error:', error)
    throw error
  }
}

export async function rebuildMMSILookup(): Promise<{ inserted: number; total: number }> {
  const response = await fetch(`${API_BASE_URL}/api/mmsi/rebuild`, {
    method: 'POST',
  })
  if (!response.ok) {
    throw new Error(`Failed to rebuild MMSI lookup: ${response.statusText}`)
  }
  return response.json()
}

export async function getDataPointsByMMSIs(mmsis: string[]): Promise<MmsiDataPoint[]> {
  if (mmsis.length === 0) {
    return []
  }
  const mmsiQuery = mmsis.join(',')
  const params = new URLSearchParams({
    mmsis: mmsiQuery,
  })
  const url = `${API_BASE_URL}/api/datapoints?${params}`
  console.log('Calling getDataPointsByMMSIs:', url)
  try {
    const response = await fetch(url)
    console.log('getDataPointsByMMSIs response status:', response.status)
    if (!response.ok) {
      console.warn(`Failed to fetch datapoints: ${response.statusText}`)
      return []
    }
    const data = await response.json()
    console.log('getDataPointsByMMSIs response data:', data)
    return data
  } catch (error) {
    console.warn('Error fetching datapoints:', error)
    return []
  }
}

export async function healthCheck(): Promise<boolean> {
  try {
    const response = await fetch(`${API_BASE_URL}/api/health`)
    return response.ok
  } catch {
    return false
  }
}
