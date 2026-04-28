import { useState } from 'react'
import LayersIcon from '@mui/icons-material/Layers'
import './QueryLayerToggle.css'

type QueryLayerToggleProps = {
  queryTypes: string[]
  visibleTypes: Set<string>
  colors: Record<string, string>
  onChange: (next: Set<string>) => void
}

export default function QueryLayerToggle({
  queryTypes,
  visibleTypes,
  colors,
  onChange,
}: QueryLayerToggleProps) {
  const [isOpen, setIsOpen] = useState(false)

  const toggleType = (type: string) => {
    const next = new Set(visibleTypes)
    if (next.has(type)) {
      next.delete(type)
    } else {
      next.add(type)
    }
    onChange(next)
  }

  const setAll = (enabled: boolean) => {
    onChange(enabled ? new Set(queryTypes) : new Set())
  }

  return (
    <div className={`query-layer-container${isOpen ? ' is-open' : ''}`}>
      <button
        type="button"
        className="query-layer-button"
        onClick={() => setIsOpen((prev) => !prev)}
        aria-label="Query layers"
        aria-expanded={isOpen}
      >
        <LayersIcon className="query-layer-icon" />
      </button>
      {isOpen && (
        <div className="query-layer-dropdown">
          <div className="query-layer-content">
            <div className="query-layer-header">Query types</div>
            {queryTypes.length === 0 && (
              <div className="query-layer-empty">No queries loaded</div>
            )}
            {queryTypes.map((type) => {
              const checked = visibleTypes.has(type)
              const color = colors[type] ?? '#555'
              return (
                <label key={type} className="query-layer-option">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => toggleType(type)}
                  />
                  <span
                    className="query-layer-swatch"
                    style={{ backgroundColor: color }}
                  />
                  <span className="query-layer-name">{type}</span>
                </label>
              )
            })}
            {queryTypes.length > 0 && (
              <div className="query-layer-actions">
                <button
                  type="button"
                  className="query-layer-action"
                  onClick={() => setAll(true)}
                >
                  All
                </button>
                <button
                  type="button"
                  className="query-layer-action"
                  onClick={() => setAll(false)}
                >
                  None
                </button>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
