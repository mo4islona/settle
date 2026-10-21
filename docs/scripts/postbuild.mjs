#!/usr/bin/env node
// Runs after `vocs build`. Vocs copies the raw MDX into llms-full.txt, with
// imports, components and `[!include]` lines in place of the code. This
// script writes llms.txt and llms-full.txt again from the pages, in the order
// of the menu, with the code of each language. With DOCS_BASE_URL set, it also
// writes sitemap.xml and robots.txt.

import { readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const ROOT = fileURLToPath(new URL('..', import.meta.url))
const DIST = join(ROOT, 'dist')
const baseUrl = process.env.DOCS_BASE_URL?.replace(/\/$/, '')

const read = (path) => readFileSync(join(ROOT, path), 'utf8')
const url = (link) => `${baseUrl ?? ''}${link}`

const languages = [...read('components/language.ts').matchAll(/\{ id: '(\w+)', label: '([^']+)' \}/g)].map(
  ([, id, label]) => ({ id, label }),
)

// The sections and pages of sidebar.ts, in order.
const sections = []
for (const [, text, link] of read('sidebar.ts').matchAll(/text: '([^']+)',\s*(?:items|link: '([^']+)')/g)) {
  if (link) sections.at(-1).pages.push(link)
  else sections.push({ title: text, pages: [] })
}

// The lines of `file` between `// [!region name]` and `// [!endregion name]`,
// without the markers of other regions. The same rule as the Vocs include.
function includeRegion(file, region) {
  const lines = read(file).replace(/\n$/, '').split('\n')
  const marker = /\/\/ \[!(end)?region (.*)\]/
  let inside = !region
  const out = []

  for (const line of lines) {
    const match = line.match(marker)

    if (match) {
      if (match[2] === region) inside = !match[1]
      continue
    }
    if (inside) out.push(line)
  }

  return out.join('\n')
}

// One `<Api ts="…" rust="…" />` as text: the TypeScript name, and the name of
// each other language that differs from it.
function apiName(tag) {
  const names = Object.fromEntries([...tag.matchAll(/(\w+)=(?:"([^"]*)"|\{'([^']*)'\})/g)].map(([, k, a, b]) => [k, a ?? b]))
  const ts = names.ts
  const others = languages
    .filter(({ id }) => id !== 'ts' && names[id] && names[id] !== ts)
    .map(({ id, label }) => `${label}: \`${names[id]}\``)

  return others.length > 0 ? `\`${ts}\` (${others.join(', ')})` : `\`${ts}\``
}

function page(link) {
  const source = read(`pages${link}.mdx`)
  const heading = source.match(/^# (.*?)(?: \[(.*)\])?$/m)
  const out = []
  let fence = false

  for (const line of source.slice(heading.index + heading[0].length).split('\n')) {
    if (/^\s*(```|~~~)/.test(line)) {
      fence = !fence
      out.push(line)
      continue
    }

    if (fence) {
      const include = line.match(/\/\/ \[!include ~\/([^\]:\s]+)(?::([^\]\s]+))?\]/)
      out.push(include ? includeRegion(include[1], include[2]) : line)
      continue
    }

    if (/^\s*import\s.*from\s/.test(line)) continue
    if (/^\s*<LanguageSwitch\b.*\/>\s*$/.test(line)) continue
    if (/^\s*:{3,}/.test(line)) continue

    out.push(line.replace(/<Api\b[^>]*\/>/g, apiName))
  }

  return {
    title: heading[1],
    subtitle: heading[2],
    body: out.join('\n').replace(/\n{3,}/g, '\n\n').trim(),
  }
}

const config = read('vocs.config.tsx')
const siteTitle = config.match(/title: '([^']+)'/)[1]
const siteDescription = config.match(/description: '([^']+)'/)[1]

const index = [`# ${siteTitle}`, '', `> ${siteDescription}`, '']
const full = [`# ${siteTitle}`, '', `> ${siteDescription}`, '']

for (const section of sections) {
  index.push(`## ${section.title}`, '')

  for (const link of section.pages) {
    const { title, subtitle, body } = page(link)

    index.push(`- [${title}](${url(link)})${subtitle ? `: ${subtitle}` : ''}`)
    full.push('---', '', `# ${title}`, '')
    if (subtitle) full.push(`> ${subtitle}`, '')
    full.push(`Source: ${url(link)}`, '', body, '')
  }

  index.push('')
}

writeFileSync(join(DIST, 'llms.txt'), index.join('\n'))
writeFileSync(join(DIST, 'llms-full.txt'), full.join('\n'))

if (baseUrl) {
  const links = ['/', ...sections.flatMap((section) => section.pages)]
  const sitemap = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...links.map((link) => `  <url><loc>${url(link)}</loc></url>`),
    '</urlset>',
    '',
  ]

  writeFileSync(join(DIST, 'sitemap.xml'), sitemap.join('\n'))
  writeFileSync(join(DIST, 'robots.txt'), `User-agent: *\nAllow: /\n\nSitemap: ${url('/sitemap.xml')}\n`)
}

console.log(`postbuild: llms.txt, llms-full.txt${baseUrl ? ', sitemap.xml, robots.txt' : ' (no DOCS_BASE_URL: no sitemap)'}`)
