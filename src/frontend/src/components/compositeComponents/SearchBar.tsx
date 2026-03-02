import { useState } from 'react'
import Search, { SearchMode } from "../pageComponents/Search";
import SettingsTab from "../pageComponents/SettingsTab";
import './SearchBar.css';

type SearchBarProps = {
  onSearch: (query: string, mode: SearchMode) => void
  onMMSISelect?: (mmsis: string[]) => void
}

export default function SearchBar({ onSearch, onMMSISelect }: SearchBarProps) {
  const [isOpen, setIsOpen] = useState(false)
  const [searchMode, setSearchMode] = useState<SearchMode>('ID')

  const handleModeChange = (mode: SearchMode) => {
    setSearchMode(mode)
    // Clear selection when switching modes
    if (mode !== 'ID' && onMMSISelect) {
      onMMSISelect([])
    }
  }

  return (
    <div className={`search-bar-container${isOpen ? ' is-open' : ''}`}>
      <div className="search-bar-header">
        <div className="search-bar-input">
          <Search searchMode={searchMode} onSearch={onSearch} onMMSISelect={onMMSISelect} />
        </div>
        <SettingsTab
          isOpen={isOpen}
          onToggle={() => setIsOpen((prev) => !prev)}
        />
      </div>
      {isOpen && (
        <div className="search-settings" role="group" aria-label="Search settings">
          <p className="search-settings-title">Search by</p>
          <label className="search-settings-option">
            <input
              type="radio"
              name="search-mode"
              value="ID"
              checked={searchMode === 'ID'}
              onChange={() => handleModeChange('ID')}
            />
            MMSI ID
          </label>
          <label className="search-settings-option">
            <input
              type="radio"
              name="search-mode"
              value="Time"
              checked={searchMode === 'Time'}
              onChange={() => handleModeChange('Time')}
            />
            Time
          </label>
          <label className="search-settings-option">
            <input
              type="radio"
              name="search-mode"
              value="Location"
              checked={searchMode === 'Location'}
              onChange={() => handleModeChange('Location')}
            />
            Location
          </label>
        </div>
      )}
    </div>
  )
}