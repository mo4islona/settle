import { languages, setLanguage, useLanguage } from './language'

/** The language of the code on all pages: TypeScript, Rust and the others in `languages`. */
export function LanguageSwitch({ label }: { label?: string }) {
  const language = useLanguage()

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      {label && (
        <span style={{ color: 'var(--vocs-color_text3)', fontSize: 'var(--vocs-fontSize_13)' }}>
          {label}
        </span>
      )}

      <div
        role="radiogroup"
        aria-label="Code language"
        style={{
          display: 'inline-flex',
          gap: 2,
          padding: 2,
          border: '1px solid var(--vocs-color_border2)',
          borderRadius: 'var(--vocs-borderRadius_6)',
          background: 'var(--vocs-color_background2)',
        }}
      >
        {languages.map(({ id, label: name }) => {
          const active = id === language

          return (
            <button
              key={id}
              type="button"
              role="radio"
              aria-checked={active}
              onClick={() => setLanguage(id)}
              style={{
                padding: '4px 12px',
                border: 0,
                borderRadius: 'var(--vocs-borderRadius_4)',
                cursor: 'pointer',
                fontSize: 'var(--vocs-fontSize_14)',
                fontWeight: active ? 500 : 400,
                background: active ? 'var(--vocs-color_backgroundAccent)' : 'transparent',
                color: active ? 'var(--vocs-color_backgroundAccentText)' : 'var(--vocs-color_text2)',
              }}
            >
              {name}
            </button>
          )
        })}
      </div>
    </div>
  )
}
