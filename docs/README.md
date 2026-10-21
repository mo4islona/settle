# Settle docs

The docs site of Settle. It uses [Vocs](https://vocs.dev).

```bash
pnpm install
pnpm dev        # local server
pnpm build      # static site in dist/; fails on a dead link
pnpm lint:ste   # checks the writing rules below
```

For the production build, set the public URL of the site:

```bash
DOCS_BASE_URL=https://docs.example.com pnpm build
```

With this URL, the build adds a canonical link to each page and writes `sitemap.xml` and `robots.txt`. Vocs also adds a `<base>` tag with this URL, so do not set it for a preview build on another host. Each build writes `llms.txt` and `llms-full.txt` for language models, with the code of each language (`scripts/postbuild.mjs`).

## Structure

| Folder | Content |
|---|---|
| `pages/introduction/` | What Settle is, why to use it, the quickstart and the installation. |
| `pages/concepts/` | One page for each part of Settle. Read them in order. |
| `pages/guides/` | Tasks: a database, a block source, restarts, the browser. |
| `pages/reference/` | Lookup pages: each call, option, type and error. |
| `examples/` | The TypeScript programs that the pages show. |
| `examples/rust/` | The same programs in Rust, one binary for each (`src/bin/`). |
| `examples/checks/` | Programs that check facts on the pages. No page shows them. |
| `components/` | The language switch and the `Api` component. |

The menu is in `sidebar.ts`.

## Languages

The pages show each example in each language. A switch at the top of the site selects the language for all pages. Put the code in a code group, with one tab for each language:

````md
:::code-group

```ts [TypeScript]
// [!include ~/examples/quickstart.ts:ingest]
```

```rust [Rust]
// [!include ~/examples/rust/src/bin/quickstart.rs:ingest]
```

:::
````

The tab title must be the label of the language in `components/language.ts`. When the reader clicks a tab or the switch, `layout.tsx` selects the same tab in each code group.

The switch is in the top bar, but Vocs hides the top bar on a narrow screen. So put the switch also under the title of each page with code in two languages:

```mdx
import { LanguageSwitch } from '../../components/LanguageSwitch'

# Views [Aggregates for each group of rows]

<LanguageSwitch label="Code language" />
```

When an API name in the text differs between languages, use the `Api` component:

```mdx
import { Api } from '../../components/Api'

Call <Api ts="handleFork" rust="handle_fork" /> after a fork.
```

The HTML holds the code of each language, so that search engines see all of it. A patch of Vocs (`patches/vocs@1.4.1.patch`) renders each tab of a code group, and `styles.css` hides the tabs that are not selected. `Api` also renders each name, and `styles.css` shows one of them.

To add a language, for example Go:

1. Add it to `languages` in `components/language.ts`.
2. Add a rule for it to the `.api-name` rules in `styles.css`.
3. Add a prop for it to the calls of `Api`.
4. Add its programs, for example in `examples/go/`, and a `[Go]` tab to each code group.

## Examples

Code that shows a behavior comes from a program in `examples/`. A page includes a part of the program:

````md
```ts
// [!include ~/examples/quickstart.ts:ingest]
```
````

The part starts at `// [!region ingest]` and stops at `// [!endregion ingest]`. Each output on a page comes from a real run of the program. When you change the API, run the programs again and update the outputs:

```bash
cd examples
pnpm install
npx tsx quickstart.ts
npx tsc --noEmit

cd rust
cargo run --release --bin quickstart
```

## Rules for the text

Write the prose in ASD-STE100 Simplified Technical English:

- Use simple, common words. Give each word only one sense.
- Write a maximum of 20 words in an instruction and 25 words in a description.
- Write one instruction in each sentence. Use the imperative.
- Use the active voice and the present tense.
- Do not use the -ing form of a verb, except in a technical name.
- Do not leave out "the" and "a".
- Write a maximum of six sentences in a paragraph.

`pnpm lint:ste` checks the length of sentences and paragraphs, -ing forms, modal verbs, contractions and the perfect tense. It also shows the sentences that are possibly passive. It cannot check the approved words. A person must read each page for them.

Each page starts with the reason to read it. Then it shows a small example, then the details, then the next steps.
