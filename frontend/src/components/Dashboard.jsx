import { useState } from 'react'
import './Dashboard.css'

const PIPELINE_STEPS = [
  {
    name: 'Remove Duplications',
    description: 'Drops duplicate rows and removes unnecessary columns. Filters to Class A vessels only.',
  },
  {
    name: 'Remove Ship Types',
    description: 'Filters out SAR vessels and removes undefined ships with high SOG readings.',
  },
  {
    name: 'Remove Outliers',
    description: 'Identifies and removes statistical outliers from the dataset.',
  },
]

function Dashboard() {
  const [selectedStep, setSelectedStep] = useState(null)

  return (
    <div className="dashboard">
      <section className="info-section">
        <h2>Data Processing Pipeline</h2>
        <p>
          This project processes AIS (Automatic Identification System) data to
          clean and simplify maritime vessel queries. The pipeline performs
          several cleaning steps on raw AIS data.
        </p>
      </section>

      <section className="steps-section">
        <h2>Pipeline Steps</h2>
        <div className="steps-grid">
          {PIPELINE_STEPS.map((step, index) => (
            <div
              key={step.name}
              className={`step-card ${selectedStep === index ? 'selected' : ''}`}
              onClick={() => setSelectedStep(selectedStep === index ? null : index)}
            >
              <div className="step-number">{index + 1}</div>
              <h3>{step.name}</h3>
              <p>{step.description}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="status-section">
        <h2>Status</h2>
        <div className="status-card">
          <span className="status-indicator ready" />
          <span>Pipeline ready</span>
        </div>
      </section>
    </div>
  )
}

export default Dashboard
