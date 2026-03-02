import { useEffect, useState, useRef } from 'react'
import { searchMMSIs } from '../../utils/api'
import './Search.css'

export type SearchMode = 'ID' | 'Time' | 'Location'

type SearchProps = {
  searchMode: SearchMode
  onSearch: (query: string, mode: SearchMode) => void
  onMMSISelect?: (mmsis: string[]) => void
}

const placeholders: Record<SearchMode, string> = {
  ID: 'Enter MMSI (number)',
  Time: 'Enter timestamp (e.g. 2026-02-26T10:00:00Z)',
  Location: 'Enter location'
}

const validateInput = (value: string, mode: SearchMode): string | null => {
  const trimmed = value.trim()
  if (trimmed === '') {
    return null
  }

  if (mode === 'ID') {
    return /^\d+$/.test(trimmed) ? null : 'MMSI must be a number'
  }

  if (mode === 'Time') {
    return Number.isNaN(Date.parse(trimmed)) ? 'Enter a valid timestamp' : null
  }

  return null
}

interface MMSISuggestion {
  mmsi: string
  selected: boolean
}

export default function Search({ searchMode, onSearch, onMMSISelect }: SearchProps) {
  const [inputValue, setInputValue] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [mmsiSuggestions, setMmsiSuggestions] = useState<MMSISuggestion[]>([])
  const [isLoadingSuggestions, setIsLoadingSuggestions] = useState(false)
  const [showDropdown, setShowDropdown] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)

  // Handle MMSI search suggestions
  useEffect(() => {
    if (searchMode !== 'ID') {
      setMmsiSuggestions([])
      setShowDropdown(false)
      return
    }

    const handle = setTimeout(async () => {
      const validationError = validateInput(inputValue, searchMode)
      setError(validationError)

      if (validationError || inputValue.trim() === '') {
        setMmsiSuggestions([])
        setShowDropdown(false)
        return
      }

      setIsLoadingSuggestions(true)
      try {
        console.log('Fetching MMSI suggestions for:', inputValue.trim())
        const response = await searchMMSIs(inputValue.trim(), 10)
        console.log('MMSI search response:', response)
        const suggestions: MMSISuggestion[] = response.results.map((mmsi) => ({
          mmsi,
          selected: false,
        }))
        console.log('Mapped suggestions:', suggestions)
        setMmsiSuggestions(suggestions)
        setShowDropdown(suggestions.length > 0)
      } catch (err) {
        console.error('Error fetching MMSI suggestions:', err)
        setMmsiSuggestions([])
        setShowDropdown(false)
      } finally {
        setIsLoadingSuggestions(false)
      }
    }, 300)

    return () => clearTimeout(handle)
  }, [inputValue, searchMode])

  // Close dropdown on outside click
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setShowDropdown(false)
      }
    }

    if (showDropdown) {
      document.addEventListener('mousedown', handleClickOutside)
      return () => document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [showDropdown])

  const handleMMSIToggle = (mmsi: string) => {
    setMmsiSuggestions((prev) =>
      prev.map((s) => (s.mmsi === mmsi ? { ...s, selected: !s.selected } : s))
    )
  }

  const handleApplySelection = () => {
    const selected = mmsiSuggestions.filter((s) => s.selected).map((s) => s.mmsi)
    console.log('Applying MMSI selection:', selected)
    if (onMMSISelect) {
      onMMSISelect(selected)
    }
    setShowDropdown(false)
  }

  // Non-ID modes: pass through to onSearch
  useEffect(() => {
    if (searchMode !== 'ID') {
      const handle = setTimeout(() => {
        const validationError = validateInput(inputValue, searchMode)
        setError(validationError)

        if (!validationError) {
          onSearch(inputValue.trim(), searchMode)
        }
      }, 300)

      return () => clearTimeout(handle)
    }
  }, [inputValue, searchMode, onSearch])

  return (
    <div className="search-input" ref={dropdownRef}>
      <input
        className="search-input-field"
        type="text"
        value={inputValue}
        onChange={(event) => setInputValue(event.target.value)}
        onFocus={() => searchMode === 'ID' && mmsiSuggestions.length > 0 && setShowDropdown(true)}
        placeholder={placeholders[searchMode]}
        aria-label="Search"
        aria-invalid={Boolean(error)}
      />
      {error && <div className="search-input-error">{error}</div>}

      {searchMode === 'ID' && showDropdown && (
        <div className="mmsi-dropdown">
          {isLoadingSuggestions ? (
            <div className="mmsi-dropdown-loading">Loading...</div>
          ) : mmsiSuggestions.length === 0 ? (
            <div className="mmsi-dropdown-empty">No MMSIs found</div>
          ) : (
            <>
              <div className="mmsi-dropdown-list">
                {mmsiSuggestions.map((suggestion) => (
                  <label key={suggestion.mmsi} className="mmsi-dropdown-item">
                    <input
                      type="checkbox"
                      checked={suggestion.selected}
                      onChange={() => handleMMSIToggle(suggestion.mmsi)}
                    />
                    <span>{suggestion.mmsi}</span>
                  </label>
                ))}
              </div>
              <div className="mmsi-dropdown-actions">
                <button className="mmsi-button-apply" onClick={handleApplySelection}>
                  Apply Selection
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}
