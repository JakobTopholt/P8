import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import App from '../App'

describe('App', () => {
  it('renders the header', () => {
    render(<App />)
    expect(screen.getByText('AIS Data Dashboard')).toBeInTheDocument()
  })

  it('renders the dashboard section', () => {
    render(<App />)
    expect(screen.getByText('Data Processing Pipeline')).toBeInTheDocument()
  })
})
