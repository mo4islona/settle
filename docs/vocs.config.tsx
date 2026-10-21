// The config loader compiles JSX to React.createElement.
import React from 'react'
import { defineConfig } from 'vocs'
import { EARLY_SCRIPT } from './components/language'
import { sidebar } from './sidebar'

// The public URL of the site, for example https://docs.example.com. Without
// it, the build makes no canonical links, no og:url and no sitemap.
const baseUrl = process.env.DOCS_BASE_URL?.replace(/\/$/, '')

export default defineConfig({
  title: 'Settle',
  titleTemplate: '%s – Settle',
  description: 'Live tables from blockchain data that stay correct after a reorg.',
  rootDir: '.',
  baseUrl,
  aiCta: false,
  sidebar,
  topNav: [
    { text: 'Docs', link: '/introduction/what-is-settle', match: '/introduction' },
    { text: 'Reference', link: '/reference/pipeline', match: '/reference' },
  ],
  socials: [{ icon: 'github', link: 'https://github.com/mo4islona/settle' }],
  editLink: {
    pattern: 'https://github.com/mo4islona/settle/edit/master/docs/pages/:path',
    text: 'Suggest changes to this page',
  },
  theme: {
    accentColor: { light: '#0f766e', dark: '#2dd4bf' },
  },
  head({ path }) {
    const canonicalPath = path === '/' ? '/' : path.replace(/\/$/, '')

    return (
      <>
        <script dangerouslySetInnerHTML={{ __html: EARLY_SCRIPT }} />
        {baseUrl && <link rel="canonical" href={`${baseUrl}${canonicalPath}`} />}
      </>
    )
  },
})
