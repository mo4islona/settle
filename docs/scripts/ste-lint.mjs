#!/usr/bin/env node
// Checks the mechanical rules of ASD-STE100 in the docs prose. It cannot check
// the approved-word list or the meaning of a word: a human review does that.
//
//   node scripts/ste-lint.mjs pages
//
// Errors fail the run. Warnings (probable passive voice) need a human look.

import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join } from 'node:path'

const MAX_INSTRUCTION_WORDS = 20
const MAX_DESCRIPTIVE_WORDS = 25
const MAX_PARAGRAPH_SENTENCES = 6

// Words that end in -ing but are not the -ing form of a verb, and the -ing
// words that are part of a technical name ("sliding window").
const ING_WORDS = new Set([
  'bring', 'ceiling', 'during', 'king', 'nothing', 'anything', 'something', 'everything',
  'ping', 'ring', 'spring', 'string', 'strings', 'thing', 'things', 'wing',
  'sliding',
])

// The first words of an instruction: an imperative verb.
const IMPERATIVES = new Set([
  'ack', 'add', 'apply', 'call', 'change', 'check', 'copy', 'create', 'declare', 'do',
  'find', 'get', 'give', 'import', 'install', 'keep', 'let', 'look', 'make', 'open', 'pass',
  'put', 'read', 'remove', 'replace', 'return', 'run', 'see', 'send', 'set', 'show', 'start',
  'stop', 'store', 'tell', 'use', 'wait', 'write',
])

const MODAL = /\b(would|should|could|might|may|shall)\b/i
const CONTRACTION = /\b\w+(n't|'re|'ve|'ll|'m|'d)\b/i
const PERFECT = /\b(has|have|had)\s+(been|\w+ed|\w+en)\b/i
const PASSIVE = /\b(is|are|was|were|be|been)\s+(\w+ed|given|written|done|made|kept|sent|taken|known|shown|seen|built|found|lost|read|set|put)\b/i

function files(path) {
  if (statSync(path).isFile()) return [path]
  return readdirSync(path).flatMap((name) => files(join(path, name)))
}

// Blank out what is not prose, keep the line numbers.
function prose(text) {
  const lines = text.split('\n')
  let fence = false
  let front = lines[0] === '---'

  return lines.map((line, i) => {
    if (front) {
      if (i > 0 && line === '---') front = false
      return ''
    }
    if (/^\s*(```|~~~)/.test(line)) {
      fence = !fence
      return ''
    }
    if (fence) return ''
    if (/^\s*(import|export)\s/.test(line)) return ''
    if (/^\s*:::/.test(line)) return ''
    if (/^\s*\|?\s*:?-{3,}/.test(line)) return ''

    return line
      .replace(/<Api\b[^>]*\/>/g, 'CODE')
      .replace(/`[^`]*`/g, 'CODE')
      .replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1')
      .replace(/<[^>]+>/g, ' ')
      .replace(/https?:\/\/\S+/g, 'URL')
      .replace(/^\s*#{1,6}\s+/, '')
      .replace(/^\s*([-*+]|\d+\.)\s+/, '')
      .replace(/[*_]{1,2}/g, '')
  })
}

function sentences(text) {
  return text
    .split(/(?<=[.!?:])\s+(?=[A-Z0-9"(])/)
    .map((s) => s.trim())
    .filter((s) => /[a-z]/i.test(s))
}

let errors = 0
let warnings = 0

function report(kind, file, line, rule, text) {
  if (kind === 'error') errors++
  else warnings++
  console.log(`${file}:${line}: ${kind}: ${rule}: ${text}`)
}

for (const file of files(process.argv[2] ?? 'pages').filter((f) => /\.mdx?$/.test(f))) {
  const source = readFileSync(file, 'utf8')
  const original = source.split('\n')
  const lines = prose(source)
  let paragraph = []

  const flushParagraph = () => {
    const count = paragraph.reduce((n, p) => n + sentences(p.text).length, 0)
    if (count > MAX_PARAGRAPH_SENTENCES) {
      report('error', file, paragraph[0].line, `paragraph of ${count} sentences`, paragraph[0].text.slice(0, 60))
    }
    paragraph = []
  }

  lines.forEach((raw, i) => {
    const line = i + 1
    const isTableRow = /^\s*\|/.test(raw)
    const isListItem = /^\s*([-*+]|\d+\.)\s+/.test(original[i])

    if (!raw.trim()) {
      flushParagraph()
      return
    }

    const cells = isTableRow ? raw.split('|').map((c) => c.trim()).filter(Boolean) : [raw]
    for (const cell of cells) {
      for (const s of sentences(cell)) {
        const words = s.split(/\s+/).filter((w) => /[a-z0-9]/i.test(w))
        const first = words[0]?.toLowerCase().replace(/[^a-z]/g, '')
        const limit = IMPERATIVES.has(first) ? MAX_INSTRUCTION_WORDS : MAX_DESCRIPTIVE_WORDS
        if (words.length > limit) report('error', file, line, `${words.length} words (max ${limit})`, s)

        for (const w of s.match(/\b[a-z]+ing\b/gi) ?? []) {
          if (!ING_WORDS.has(w.toLowerCase())) report('error', file, line, `-ing form "${w}"`, s)
        }
        if (MODAL.test(s)) report('error', file, line, `modal "${s.match(MODAL)[0]}"`, s)
        if (CONTRACTION.test(s)) report('error', file, line, 'contraction', s)
        if (PERFECT.test(s)) report('error', file, line, 'perfect tense', s)
        if (PASSIVE.test(s)) report('warning', file, line, `passive? "${s.match(PASSIVE)[0]}"`, s)
      }
    }

    if (isTableRow || isListItem) flushParagraph()
    else paragraph.push({ line, text: raw })
  })
  flushParagraph()
}

console.log(`\n${errors} errors, ${warnings} warnings`)
process.exit(errors > 0 ? 1 : 0)
