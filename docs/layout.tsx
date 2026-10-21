import { type ReactNode, useEffect } from 'react'
import { LanguageSwitch } from './components/LanguageSwitch'
import { type LanguageId, languages, setLanguage, useLanguage } from './components/language'

const TAB = '[aria-label="Code group"] [role="tab"]'
const CONTENT = '#vocs-content'

// Below the sticky top bar, so that the probe hits the page and not the bar.
const PROBE_Y = 120

function labelOf(id: LanguageId): string {
  return languages.find((language) => language.id === id)?.label ?? ''
}

// The element that must not move on the screen when the tabs change: the
// control that the reader clicked, or else the content at the top of the view.
function pickAnchor(clicked: Element | null): Element | null {
  const content = document.querySelector(CONTENT)
  if (!content) return null
  if (clicked && content.contains(clicked)) return clicked

  const box = content.getBoundingClientRect()
  const probed = document.elementFromPoint(box.left + box.width / 2, PROBE_Y)

  return probed && content.contains(probed) ? probed : null
}

// Vocs code groups are uncontrolled Radix tabs, which switch on mousedown.
// Radix also focuses each tab that it selects, and the focus scrolls the
// page, so the anchor goes back to its place after the switch.
function selectTabs(label: string, clicked: Element | null) {
  const atTop = window.scrollY === 0
  const anchor = pickAnchor(clicked)
  const anchorTop = anchor?.getBoundingClientRect().top
  const focused = document.activeElement

  for (const tab of document.querySelectorAll<HTMLElement>(TAB)) {
    const isTarget = tab.textContent?.trim() === label
    const isSelected = tab.getAttribute('aria-selected') === 'true'

    if (isTarget && !isSelected) {
      tab.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, button: 0 }))
    }
  }

  if (focused instanceof HTMLElement && focused !== document.activeElement) {
    focused.focus({ preventScroll: true })
  }

  requestAnimationFrame(() => {
    if (atTop) {
      window.scrollTo(0, 0)
    } else if (anchor && anchorTop !== undefined && anchor.isConnected) {
      window.scrollBy(0, anchor.getBoundingClientRect().top - anchorTop)
    }
  })
}

let lastClicked: Element | null = null

export default function Layout({ children, path }: { children: ReactNode; path: string }) {
  const language = useLanguage()

  useEffect(() => {
    const onMouseDown = (event: MouseEvent) => {
      if (!event.isTrusted) return

      lastClicked = event.target as Element | null

      // A click on a language tab in any code group picks that language for the whole site.
      const tab = lastClicked?.closest?.(TAB)
      const label = tab?.textContent?.trim()
      const picked = languages.find((candidate) => candidate.label === label)

      if (picked) setLanguage(picked.id)
    }

    document.addEventListener('mousedown', onMouseDown, true)
    return () => document.removeEventListener('mousedown', onMouseDown, true)
  }, [])

  useEffect(() => {
    const frame = requestAnimationFrame(() => {
      selectTabs(labelOf(language), lastClicked)
      lastClicked = null
    })

    return () => cancelAnimationFrame(frame)
  }, [language, path])

  return children
}

export function TopNavEnd() {
  return <LanguageSwitch />
}
