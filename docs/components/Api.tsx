import { Fragment } from 'react'
import { languages, type LanguageId } from './language'

type Props = Partial<Record<LanguageId, string>>

// The look of inline code in Vocs. Its own component cannot be imported here:
// in server rendering it pulls Vocs virtual modules through the Node loader.
const style = {
  backgroundColor: 'var(--vocs-color_codeInlineBackground)',
  border: '1px solid var(--vocs-color_codeInlineBorder)',
  borderRadius: 'var(--vocs-borderRadius_4)',
  color: 'var(--vocs-color_codeInlineText)',
  fontSize: 'var(--vocs-fontSize_code)',
  padding: 'var(--vocs-space_3) var(--vocs-space_6)',
}

/**
 * An API name that differs by language: `<Api ts="handleFork" rust="handle_fork" />`.
 * The HTML holds the name of each language, and styles.css shows the one of
 * the language that the reader picked. The hidden separator keeps the names
 * apart for a reader of the HTML without styles, for example a crawler.
 * A language without a name falls back to the TypeScript name.
 */
export function Api(props: Props) {
  return (
    <code className="api-name" style={style}>
      {languages.map(({ id }, i) => (
        <Fragment key={id}>
          {i > 0 && <span className="api-separator"> / </span>}
          <span data-lang={id}>{props[id] ?? props.ts}</span>
        </Fragment>
      ))}
    </code>
  )
}
