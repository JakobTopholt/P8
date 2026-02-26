import Search from "../pageComponents/Search";
import SettingsTab from "../pageComponents/SettingsTab";
import './SearchBar.css';

export default function SearchBar() {
  return (
    <div className="search-bar-container">
      <Search />
      <SettingsTab />
    </div>
  )
}