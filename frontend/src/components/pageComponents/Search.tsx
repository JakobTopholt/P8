import { useEffect, useState } from 'react'

export type SearchMode = 'ID' | 'Time' | 'Location'

type SearchProps = {
  searchMode: SearchMode
  onSearch: (query: string, mode: SearchMode) => void
}

const placeholders: Record<SearchMode, string> = {
  ID: 'Enter ID (number)',
  Time: 'Enter timestamp (e.g. 2026-02-26T10:00:00Z)',
  Location: 'Enter location'
}

const validateInput = (value: string, mode: SearchMode): string | null => {
  const trimmed = value.trim()
  if (trimmed === '') {
    return null
  }

  if (mode === 'ID') {
    return /^\d+$/.test(trimmed) ? null : 'ID must be an integer'
  }

  if (mode === 'Time') {
    return Number.isNaN(Date.parse(trimmed)) ? 'Enter a valid timestamp' : null
  }

  return null
}

export default function Search({ searchMode, onSearch }: SearchProps) {
  const [inputValue, setInputValue] = useState<string>('')
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const handle = setTimeout(() => {
      const validationError = validateInput(inputValue, searchMode)
      setError(validationError)

      if (!validationError) {
        onSearch(inputValue.trim(), searchMode)
      }
    }, 300)

    return () => clearTimeout(handle)
  }, [inputValue, searchMode, onSearch])

  return (
    <div className="search-input">
      <input
        className="search-input-field"
        type="text"
        value={inputValue}
        onChange={(event) => setInputValue(event.target.value)}
        placeholder={placeholders[searchMode]}
        aria-label="Search"
        aria-invalid={Boolean(error)}
      />
      {error && <div className="search-input-error">{error}</div>}
    </div>
  )
}
