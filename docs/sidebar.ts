import type { Sidebar } from 'vocs'

export const sidebar: Sidebar = [
  {
    text: 'Introduction',
    items: [
      { text: 'What is Settle?', link: '/introduction/what-is-settle' },
      { text: 'Why Settle?', link: '/introduction/why-settle' },
      { text: 'Quickstart', link: '/introduction/quickstart' },
      { text: 'Installation', link: '/introduction/installation' },
    ],
  },
  {
    text: 'Concepts',
    items: [
      { text: 'How Settle works', link: '/concepts/how-settle-works' },
      { text: 'Tables', link: '/concepts/tables' },
      { text: 'Views', link: '/concepts/views' },
      { text: 'Reducers', link: '/concepts/reducers' },
      { text: 'Change batches', link: '/concepts/change-batches' },
      { text: 'Forks and finality', link: '/concepts/forks-and-finality' },
    ],
  },
  {
    text: 'Guides',
    items: [
      { text: 'Write changes to Postgres', link: '/guides/postgres' },
      { text: 'Use with SQD Portal', link: '/guides/sqd-portal' },
      { text: 'Use with an RPC node', link: '/guides/rpc-node' },
      { text: 'Time windows and candles', link: '/guides/time-windows' },
      { text: 'Restarts and crash safety', link: '/guides/restarts' },
      { text: 'Run in a browser', link: '/guides/browser' },
    ],
  },
  {
    text: 'Reference',
    items: [
      { text: 'Pipeline', link: '/reference/pipeline' },
      { text: 'Settle', link: '/reference/settle' },
      { text: 'Configuration', link: '/reference/configuration' },
      { text: 'Column types', link: '/reference/column-types' },
      { text: 'Aggregate functions', link: '/reference/aggregates' },
      { text: 'Schema language', link: '/reference/schema' },
      { text: 'Lua reducers', link: '/reference/lua' },
      { text: 'Errors', link: '/reference/errors' },
      { text: 'Rust crate', link: '/reference/rust' },
    ],
  },
]
