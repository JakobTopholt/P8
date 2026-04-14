import { useState } from 'react'
import './Randomizer.css'
import CasinoIcon from '@mui/icons-material/Casino';

interface DataPoint {
  id: number
  mmsi: number
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
  const [isOpen, setIsOpen] = useState<boolean>(false)
  const [inputValue, setInputValue] = useState<string>('')
  const [error, setError] = useState<string | null>(null)

  // Get unique MMSIs from all data points
  const uniqueMMSIs = Array.from(new Set(allDataPoints.map((p) => p.mmsi)))

  const handleRandomize = () => {
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
        className="randomizer-button"
        onClick={() => setIsOpen((prev) => !prev)}
        aria-label="Randomizer"
        aria-expanded={isOpen}
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
              max={uniqueMMSIs.length}
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={`1-${uniqueMMSIs.length}`}
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
