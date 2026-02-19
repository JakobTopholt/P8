import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import Header from '../components/Header'

describe('Header', () => {
  it('renders the title', () => {
    render(<Header />)
    expect(screen.getByText('AIS Data Dashboard')).toBeInTheDocument()
  })

  it('renders the subtitle', () => {
    render(<Header />)
    expect(
      screen.getByText('Maritime vessel data processing and query simplification')
    ).toBeInTheDocument()
  })
})
