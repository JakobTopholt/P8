import SettingsIcon from '@mui/icons-material/Settings'

type SettingsTabProps = {
  isOpen: boolean
  onToggle: () => void
}

export default function SettingsTab({ isOpen, onToggle }: SettingsTabProps) {
  return (
    <button
      type="button"
      className="settings-tab-button"
      onClick={onToggle}
      aria-label="Search settings"
      aria-expanded={isOpen}
    >
      <SettingsIcon fontSize="medium" />
    </button>
  )
}
