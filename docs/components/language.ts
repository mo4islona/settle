import { useSyncExternalStore } from 'react'

// The order is the order of the tabs. `label` must equal the title of the
// code-group tabs (```ts [TypeScript]), because the layout matches on it.
export const languages = [
  { id: 'ts', label: 'TypeScript' },
  { id: 'rust', label: 'Rust' },
] as const

export type LanguageId = (typeof languages)[number]['id']

const DEFAULT: LanguageId = 'ts'
const STORAGE_KEY = 'settle-docs-language'
const CHANGE_EVENT = 'settle-docs-language'

export function getLanguage(): LanguageId {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    const known = languages.find((language) => language.id === stored)
    if (known) return known.id
  } catch {
    // Storage can be off, for example in a private window.
  }

  return DEFAULT
}

export function setLanguage(id: LanguageId) {
  try {
    localStorage.setItem(STORAGE_KEY, id)
  } catch {
    // The choice then lasts only until the next page load.
  }

  document.documentElement.dataset.language = id
  window.dispatchEvent(new Event(CHANGE_EVENT))
}

/**
 * Runs in <head> before the first paint, so that styles.css shows the API
 * names of the stored language at once. It must stay free of imports.
 */
export const EARLY_SCRIPT = `try{var l=localStorage.getItem('${STORAGE_KEY}');if(${JSON.stringify(
  languages.map((language) => language.id),
)}.indexOf(l)>=0)document.documentElement.dataset.language=l}catch(e){}`

function subscribe(onChange: () => void) {
  window.addEventListener(CHANGE_EVENT, onChange)
  window.addEventListener('storage', onChange)

  return () => {
    window.removeEventListener(CHANGE_EVENT, onChange)
    window.removeEventListener('storage', onChange)
  }
}

export function useLanguage(): LanguageId {
  return useSyncExternalStore(subscribe, getLanguage, () => DEFAULT)
}
