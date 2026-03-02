import { useState } from 'react'
import './Randomizer.css'
import CasinoIcon from '@mui/icons-material/Casino';

interface DataPoint {
  id: number
  mmsi: string | number
  position: [number, number]
  name: string
  location: string
  timestamp: string
  description?: string
}

type RandomizerProps = {
  allDataPoints: DataPoint[]
  onRandomize: (mmsis: number[]) => void
}

export default function Randomizer({ allDataPoints, onRandomize }: RandomizerProps) {
  const [isOpen, setIsOpen] = useState(false)
  const [inputValue, setInputValue] = useState('')
  const [error, setError] = useState<string | null>(null)

  // Get unique MMSIs from all data points (convert to strings for consistency)
  const uniqueMMSIs = Array.from(new Set(allDataPoints.map((p) => String(p.mmsi))))
    .map((mmsi) => Number(mmsi))

  const hasData = uniqueMMSIs.length > 0

  const handleRandomize = () => {
    if (!hasData) {
      setError('No data available. Search for MMSIs first.')
      return
    }

    const trimmed = inputValue.trim()
    if (trimmed === '') {
      setError('Enter a number')
      return
    }

    const count = Number.parseInt(trimmed, 10)
    if (Number.isNaN(count) || count < 1) {
      setError('Enter a positive integer')
      return
    }

    if (count > uniqueMMSIs.length) {
      setError(`Maximum ${uniqueMMSIs.length} MMSIs available`)
      return
    }

    setError(null)

    // Shuffle and select random MMSIs
    const shuffled = [...uniqueMMSIs].sort(() => Math.random() - 0.5)
    const selectedMMSIs = shuffled.slice(0, count)

    onRandomize(selectedMMSIs)
    setInputValue('')
    setIsOpen(false)
  }

  const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter') {
      handleRandomize()
    }
  }

  return (
    <div className={`randomizer-container${isOpen ? ' is-open' : ''}`}>
      <button
        type="button"
        className={`randomizer-button${!hasData ? ' disabled' : ''}`}
        onClick={() => hasData && setIsOpen((prev) => !prev)}
        aria-label="Randomizer"
        aria-expanded={isOpen}
        disabled={!hasData}
        title={!hasData ? 'Search for MMSIs first' : 'Randomly select ships'}
      >
        <CasinoIcon className="randomizer-icon" />
      </button>
      {isOpen && (
        <div className="randomizer-dropdown">
          <div className="randomizer-content">
            <label htmlFor="randomizer-input" className="randomizer-label">
              Number of ships:
            </label>
            <input
              id="randomizer-input"
              type="number"
              min="1"
              max={uniqueMMSIs.length || 1}
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={uniqueMMSIs.length > 0 ? `1-${uniqueMMSIs.length}` : '0'}
              className="randomizer-input"
            />
            {error && <div className="randomizer-error">{error}</div>}
            <button
              type="button"
              className="randomizer-submit"
              onClick={handleRandomize}
            >
              Randomize
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
