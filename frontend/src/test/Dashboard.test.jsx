import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import Dashboard from '../components/Dashboard'

describe('Dashboard', () => {
  it('renders the pipeline description', () => {
    render(<Dashboard />)
    expect(screen.getByText('Data Processing Pipeline')).toBeInTheDocument()
  })

  it('renders all pipeline steps', () => {
    render(<Dashboard />)
    expect(screen.getByText('Remove Duplications')).toBeInTheDocument()
    expect(screen.getByText('Remove Ship Types')).toBeInTheDocument()
    expect(screen.getByText('Remove Outliers')).toBeInTheDocument()
  })

  it('renders the status section', () => {
    render(<Dashboard />)
    expect(screen.getByText('Pipeline ready')).toBeInTheDocument()
  })

  it('toggles step selection on click', () => {
    render(<Dashboard />)
    const stepCard = screen.getByText('Remove Duplications').closest('.step-card')
    expect(stepCard).not.toHaveClass('selected')

    fireEvent.click(stepCard)
    expect(stepCard).toHaveClass('selected')

    fireEvent.click(stepCard)
    expect(stepCard).not.toHaveClass('selected')
  })
})
