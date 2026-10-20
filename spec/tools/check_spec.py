#!/usr/bin/env python3
"""Quality gate for the Settle specification suite.

Checks reference integrity, dead weight, traceability coverage, the decision log,
normative shape, freshness and the ASD-STE100 rules that a script can see.
Stdlib only. Run from anywhere:

    python3 spec/tools/check_spec.py [--format text|json|github] [--strict] ...

Exit codes: 0 clean (or only warnings without --strict), 1 findings that gate, 2 bad usage.
"""

import argparse
import fnmatch
import json
import re
import subprocess
import sys
from bisect import bisect_right
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path

# ─── Configuration: the conventions that spec/README.md declares ───────────────

HOME = {
    "G": "01-overview.md", "NG": "01-overview.md",
    "REQ": "02-requirements.md", "OQ": "02-requirements.md",
    "DEF": "03-data-model.md",
    "TR": "04-mutations.md", "WP": "04-mutations.md",
    "RP": "05-output-contract.md", "EC": "05-output-contract.md",
    "CN": "06-consistency-durability.md",
    "INV": "07-invariants.md",
    "LIV": "08-liveness.md",
    "FM": "09-failure-model.md",
    "RS": "10-retention-replay.md",
    "PF": "11-performance.md", "SLI": "11-performance.md",
    "SC": "11-performance.md", "HZ": "11-performance.md",
    "OB": "12-observability.md",
    "CT": "13-conformance-tdd.md", "FV": "13-conformance-tdd.md", "HC": "13-conformance-tdd.md",
    "MG": "13-conformance-tdd.md", "GAP": "13-conformance-tdd.md", "BO": "13-conformance-tdd.md",
    "IB": "14-interface-binding.md",
    "ADR": "decisions/",
}
PARAM_REGISTRY = "15-parameters.md"
BANDS = {
    "REQ": [(1, 9), (10, 19), (20, 29), (30, 39), (40, 49)],
    "DEF": [(1, 19), (20, 39), (40, 59), (60, 79), (80, 99)],
    "WP": [(1, 9), (10, 19), (20, 29), (30, 39), (40, 49), (50, 59), (60, 69), (70, 79)],
    "RP": [(1, 9), (10, 19), (20, 29), (30, 39)],
    "CN": [(1, 9), (10, 19), (20, 29), (30, 39), (40, 49)],
    "INV": [(1, 9), (10, 19), (20, 29), (30, 34), (35, 39), (40, 49)],
    "LIV": [(1, 9), (10, 19)],
    "FM": [(1, 9), (10, 29), (30, 39), (40, 49), (50, 59), (60, 69), (70, 79)],
    "RS": [(1, 9), (10, 19), (20, 29)],
    "IB": [(1, 19), (20, 29), (30, 39), (40, 49), (50, 59), (60, 79)],
}
MUTABLE_DOCS = {"13-conformance-tdd.md", "15-parameters.md"}
README = "README.md"
DECISION_LOG = "decisions/README.md"
CONFORMANCE = "13-conformance-tdd.md"
ACCEPTANCE_MATRIX = "Acceptance matrix"
PROPERTIES_MATRIX = "Properties matrix"
TEST_CLASSES = "Test classes"
MERGE_GATES = "Merge gates"
HARNESS_REGISTER = "Harness capability register"
GAP_REGISTER = "Gap register"
CLOSED_HEADING = re.compile(r"^#{2,6}\s+Closed\b")
NEXT_GAP_RE = re.compile(r"^\*\*Next gap number:\*\*\s*(\d+)\s*$")
ACCEPTANCE_PREFIXES = {"REQ"}
PROPERTY_PREFIXES = {"INV", "LIV", "FM", "SLI"}
COVERAGE_PREFIXES = {"INV", "LIV", "REQ"}
COVERAGE_FLOOR_FILE = "tools/coverage-floor"
IGNORE_FILE = ".spec-check-ignore"
# The binding doc names the concrete surface, so language keywords are allowed there.
SURFACE_DOCS = {"14-interface-binding.md"}
MANIFESTS = ["Cargo.toml", "bindings/typescript/settle/package.json"]
# Words that are also dependency names but are ordinary English here.
LEAK_ALLOW = {"anyhow", "tracing"}
LANGUAGE_KEYWORDS = [r"\bfn\b", r"\bimpl\b", r"&mut\b", r"\bVec<", r"\bOption<", r"\bResult<",
                     r"\bHashMap\b", r"\bunwrap\(", r"\bpanic!", r"\busize\b", r"\bBTreeMap\b",
                     r"\bArc<", r"\bconst\s+\w+\s*=", r"\.rs\b", r"\bsrc/"]
BARE_CONSTANT_SKIP_SECTIONS = re.compile(
    r"Objectives|Open questions|Explicitly unspecified|Workload model|Benchmark rules|"
    r"Hazards|Terminology|Lifecycle|Notation|Service level indicators", re.I)
WARN_ROUTE = re.compile(r"\b(ADR|OQ|GAP)-\d+|\b(draft|proposed|ratif\w*|waits? for)\b", re.I)
STE_DENY = {
    "pending": "unacked (DEF-67), or 'that waits'",
    "missing": "absent",
    "existing": "that exists",
    "utilize": "use",
    "via": "through",
    "e.g.": "for example",
    "i.e.": "that is",
    "etc.": "a full list",
    "prior to": "before",
    "in order to": "to",
}
# Nouns, prepositions and the technical names of the README vocabulary.
ING_STOPLIST = {"string", "strings", "nothing", "something", "anything", "everything", "during",
                "ceiling", "padding", "thing", "things", "bring", "spring", "sibling", "ring",
                "king", "wing", "bookkeeping", "timing", "timings", "warning", "warnings",
                "sliding", "tumbling", "binding", "bindings", "setting", "settings", "mapping",
                "meaning", "finding", "findings", "encoding", "hardening", "running", "saturating",
                "operating", "ascending", "descending", "expiring", "failing", "calling"}
ALLOWED_ADR_STATUS = re.compile(
    r"^(Proposed|Accepted \((historical|\d{4}-\d{2}-\d{2})\)|Rejected \(\d{4}-\d{2}-\d{2}\)|"
    r"Superseded by ADR-\d+)$")

# ─── Check catalog ─────────────────────────────────────────────────────────────

E, W, I = "error", "warning", "info"
CHECKS = {
    "id-undefined": (E, "Every referenced ID is defined in the home doc of its prefix."),
    "id-duplicate": (E, "No ID is defined twice."),
    "id-misplaced": (E, "An ID is defined only in the home doc of its prefix."),
    "param-undefined": (E, "Every P-* symbol has a row in the parameter registry."),
    "link-missing-file": (E, "Relative links resolve from the linking file."),
    "link-missing-anchor": (E, "A link fragment names a real heading."),
    "doc-unlisted": (E, "Every document is reachable from the README document map."),
    "doc-missing": (E, "Every document-map entry exists."),
    "adr-undefined": (E, "Every referenced ADR has a file or a log row."),
    "id-orphan": (W, "Every defined ID is referenced outside its own definition."),
    "param-unused": (W, "Every registry row is referenced by another document."),
    "trace-missing": (E, "Every REQ has an acceptance row; every INV/LIV/FM/SLI a properties row."),
    "trace-unknown": (E, "Every ID in a matrix row is defined."),
    "inv-check-unknown": (E, "Every CT in an invariant's Check line exists."),
    "inv-check-matrix-mismatch": (W, "The CT set of an invariant's Check equals its matrix row."),
    "gate-threshold-literal": (E, "Every merge-gate threshold is a P-* symbol."),
    "gate-no-capability": (E, "Every merge gate names at least one HC."),
    "gate-unknown-class": (E, "Every CT that a merge gate names exists."),
    "hc-orphan": (W, "Every HC is needed by some CT or MG."),
    "ct-unbuildable": (W, "No CT depends only on HCs at status U while a property it checks is C."),
    "ct-ungated": (I, "Every CT is reachable from a merge gate."),
    "coverage-regressed": (E, "Property coverage is not below the stored floor (MG-3)."),
    "coverage-floor-invalid": (E, "The coverage floor file exists and holds one integer (MG-3)."),
    "adr-missing-file": (E, "Every linked log row has a file."),
    "adr-missing-row": (E, "Every ADR file has a log row."),
    "adr-status-mismatch": (E, "The log status equals the file's Status line."),
    "adr-date-mismatch": (E, "The log date equals the date in the file's Status line."),
    "adr-id-mismatch": (E, "File name, heading and log row agree on the number."),
    "adr-bad-status": (E, "Every ADR has a status from the allowed set."),
    "adr-log-order": (W, "Accepted rows run in ascending number order. A proposal can be accepted after a later record."),
    "adr-title-drift": (I, "The log title equals the ADR heading."),
    "req-missing-tag": (E, "Every REQ carries an RFC 2119 tag."),
    "req-missing-acceptance": (E, "Every REQ carries an Acceptance field."),
    "inv-missing-scope": (E, "Every INV declares its scope."),
    "inv-missing-check": (E, "Every INV names a Check strategy."),
    "param-bare-constant": (W, "No bare dimensioned literal in normative text."),
    "impl-leakage": (W, "No dependency name, language keyword or source path in normative docs."),
    "heading-duplicate": (W, "No two headings of a file share a slug."),
    "warn-unratified": (W, "Every ⚠ routes to an ADR, OQ, GAP or a routed parameter."),
    "id-number-gap": (I, "No unused number inside a declared band."),
    "gap-kept": (E, "A gap row is only in the gap register: a closed gap is deleted, not kept."),
    "gap-next": (E, "The gap register has a next gap number above every gap number in it."),
    "stale-matrix": (W, "No normative doc is newer than the as-of date of the status doc."),
    "date-inconsistent": (W, "A mutable doc states one as-of date."),
    "commit-unknown": (W, "Every cited commit hash exists in the repository."),
    "ste-sentence-length": (W, "No sentence over 25 words, or 20 in a numbered step."),
    "ste-paragraph-length": (W, "No paragraph, item or field over six sentences."),
    "ste-modal-case": (W, "No lowercase must/should/may in a normative doc."),
    "ste-passive-agent": (W, "No passive with a named agent."),
    "ste-word-list": (W, "No word from the suite's deny-list."),
    "ste-ing-form": (I, "No -ing verb form outside the technical names."),
    "ignore-unjustified": (E, "Every ignore entry carries a written justification."),
}

# ─── Model ─────────────────────────────────────────────────────────────────────


@dataclass
class Finding:
    check: str
    severity: str
    file: str
    line: int
    message: str


@dataclass
class Doc:
    rel: str
    lines: list
    in_fence: list
    sections: list  # (line index, level, title) of each heading


PREFIXES = sorted(HOME, key=len, reverse=True)
PFX = "|".join(PREFIXES)
ID_RE = re.compile(rf"(?<![A-Za-z0-9-])({PFX})-(\d+)(?![\d])")
SLASH_RE = re.compile(rf"(?<![A-Za-z0-9-])({PFX})-(\d+)((?:/\d+)+)")
RANGE_RE = re.compile(rf"(?<![A-Za-z0-9-])({PFX})-(\d+)\s*(?:\.\.|\bto\b)\s*(?:({PFX})-)?(\d+)(?![\d])")
PARAM_RE = re.compile(r"(?<![A-Za-z0-9-])P-[A-Z0-9]+(?:-[A-Z0-9]+)*(?![A-Za-z0-9-])")
BOLD_DEF_RE = re.compile(rf"^\*\*((?:(?:{PFX})-\d+(?:, )?)+) — ")
MULTI_DEF_RE = re.compile(rf"({PFX})-(\d+) — ")
HEAD_DEF_RE = re.compile(rf"^#{{1,6}}\s+((?:{PFX})-\d+) — ")
ROW_DEF_RE = re.compile(rf"^\|\s*((?:{PFX})-\d+)\s*\|")
PARAM_ROW_RE = re.compile(r"^\|\s*(P-[A-Z0-9]+(?:-[A-Z0-9]+)*)\s*\|")
LINK_RE = re.compile(r"\]\(([^)\s]+)\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*$")
FENCE_RE = re.compile(r"^\s*(```|~~~)")
TABLE_SEP_RE = re.compile(r"^\|[\s:|-]+\|?\s*$")
ASOF_RE = re.compile(r"As of (\d{4}-\d{2}-\d{2})")


def load(spec: Path) -> dict:
    docs = {}
    for p in sorted(spec.rglob("*.md")):
        rel = p.relative_to(spec).as_posix()
        lines = p.read_text(encoding="utf-8").splitlines()
        in_fence, sections, fence = [], [], False
        for i, line in enumerate(lines):
            if FENCE_RE.match(line):
                fence = not fence
                in_fence.append(True)
                continue
            in_fence.append(fence)
            m = HEADING_RE.match(line)
            if m and not fence:
                sections.append((i, len(m.group(1)), m.group(2)))
        docs[rel] = Doc(rel, lines, in_fence, sections)
    return docs


def section_of(doc: Doc, i: int) -> str:
    """Title of the nearest level-2 heading at or above line i."""
    title = ""
    for idx, level, t in doc.sections:
        if idx > i:
            break
        if level <= 2:
            title = t
    return title


def section_lines(doc: Doc, title: str) -> range:
    for k, (idx, level, t) in enumerate(doc.sections):
        if t.strip() == title:
            end = len(doc.lines)
            for idx2, level2, _ in doc.sections[k + 1:]:
                if level2 <= level:
                    end = idx2
                    break
            return range(idx + 1, end)
    return range(0)


def expand_ids(text: str) -> tuple:
    """(hard, soft) sets of "PFX-n" references in text, with slash and range shorthands."""
    hard, soft = set(), set()
    for m in SLASH_RE.finditer(text):
        hard.add(f"{m.group(1)}-{m.group(2)}")
        for n in m.group(3).strip("/").split("/"):
            hard.add(f"{m.group(1)}-{n}")
    for m in RANGE_RE.finditer(text):
        pfx, a, pfx2, b = m.group(1), int(m.group(2)), m.group(3), int(m.group(4))
        if pfx2 and pfx2 != pfx:
            continue
        if b <= a or b - a > 200:
            continue
        hard.update({f"{pfx}-{a}", f"{pfx}-{b}"})
        soft.update(f"{pfx}-{n}" for n in range(a + 1, b))
    for m in ID_RE.finditer(text):
        hard.add(f"{m.group(1)}-{m.group(2)}")
    return hard, soft - hard


def read_floor(path: Path):
    """The integer of the floor file, or None when it is missing or malformed."""
    try:
        text = path.read_text().strip()
    except OSError:
        return None
    return int(text) if re.fullmatch(r"\d+", text) else None


def slugify(heading: str) -> str:
    s = re.sub(r"`([^`]*)`", r"\1", heading)
    s = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", s)
    s = re.sub(r"[*_~]", "", s).strip().lower()
    s = re.sub(r"[^\w\- ]", "", s)
    return s.replace(" ", "-")


def table_cells(line: str) -> list:
    return [c.strip() for c in line.replace("\\|", "\0").strip().strip("|").split("|")]


def plain_cell(cell: str) -> str:
    cell = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", cell)
    return re.sub(r"[*_`]", "", cell).replace("\0", "|").strip()


def table_rows(doc: Doc, rng: range):
    """(line index, cells) of the body rows of every table in a line range."""
    for i in rng:
        line = doc.lines[i]
        if doc.in_fence[i] or not line.lstrip().startswith("|"):
            continue
        if TABLE_SEP_RE.match(line):
            continue
        nxt = doc.lines[i + 1] if i + 1 < len(doc.lines) else ""
        if TABLE_SEP_RE.match(nxt):
            continue  # header row
        yield i, table_cells(line)


# ─── The checker ───────────────────────────────────────────────────────────────


class Checker:
    def __init__(self, spec: Path, repo: Path):
        self.spec, self.repo = spec, repo
        self.docs = load(spec)
        self.findings = []
        self.defs = {}       # id -> (rel, line)
        self.closed = set()
        self.refs = {}       # id -> [(rel, line)] outside its own definition
        self.soft = set()
        self.params = {}     # P-* -> line in registry
        self.param_refs = {}
        self.metrics = {}

    def add(self, check, rel, line, message):
        self.findings.append(Finding(check, CHECKS[check][0], rel, line, message))

    def home_of(self, pfx):
        return HOME.get(pfx)

    def is_home(self, pfx, rel):
        home = self.home_of(pfx)
        return rel == home or (home.endswith("/") and rel.startswith(home) and rel != DECISION_LOG)

    # ── definitions and references ──

    def collect(self):
        strong = {}
        rows = {}
        for rel, doc in self.docs.items():
            in_closed = False
            for i, line in enumerate(doc.lines):
                if doc.in_fence[i]:
                    continue
                if HEADING_RE.match(line):
                    in_closed = bool(CLOSED_HEADING.match(line))
                found = []
                m = BOLD_DEF_RE.match(line) or (re.match(r"^[-*]\s+", line) and
                                                BOLD_DEF_RE.match(re.sub(r"^[-*]\s+", "", line)))
                if m:
                    found += [f"{p}-{n}" for p, n in MULTI_DEF_RE.findall(m.group(0))]
                    if not found:
                        found += re.findall(rf"(?:{PFX})-\d+", m.group(1))
                m = HEAD_DEF_RE.match(line)
                if m:
                    found.append(m.group(1))
                for ident in found:
                    pfx = ident.split("-")[0]
                    if not self.is_home(pfx, rel):
                        self.add("id-misplaced", rel, i + 1,
                                 f"{ident} is defined here, but its home is {self.home_of(pfx)}")
                        continue
                    if ident in strong:
                        prev = strong[ident]
                        self.add("id-duplicate", rel, i + 1,
                                 f"{ident} is defined again (first at {prev[0]}:{prev[1]})")
                        continue
                    strong[ident] = (rel, i + 1)
                    if in_closed:
                        self.closed.add(ident)
                m = ROW_DEF_RE.match(line)
                if m and not found:
                    ident = m.group(1)
                    pfx = ident.split("-")[0]
                    # A row defines its ID in the first home-doc section that lists it; rows of the
                    # same ID in later tables (an SLO table over the SLI table) are references.
                    if self.is_home(pfx, rel):
                        sect = section_of(doc, i)
                        if ident not in rows:
                            rows[ident] = (sect, i + 1)
                            strong.setdefault(ident, (rel, i + 1))
                        elif rows[ident][0] == sect and strong.get(ident) == (rel, rows[ident][1]):
                            self.add("id-duplicate", rel, i + 1,
                                     f"{ident} has two rows in section '{sect}' "
                                     f"(first at line {rows[ident][1]})")
                if rel == PARAM_REGISTRY:
                    pm = PARAM_ROW_RE.match(line)
                    if pm:
                        if pm.group(1) in self.params:
                            self.add("id-duplicate", rel, i + 1, f"{pm.group(1)} has two registry rows")
                        self.params.setdefault(pm.group(1), i + 1)
        for p in (self.spec / "decisions").glob("ADR-*.md"):
            m = re.match(r"ADR-(\d+)-", p.name)
            if m:
                strong.setdefault(f"ADR-{m.group(1)}", (f"decisions/{p.name}", 1))
        self.defs = strong

        for rel, doc in self.docs.items():
            for i, line in enumerate(doc.lines):
                hard, soft = expand_ids(line)
                self.soft |= soft
                for ident in hard:
                    if self.defs.get(ident) == (rel, i + 1):
                        continue  # its own definition line
                    self.refs.setdefault(ident, []).append((rel, i + 1))
                for prm in PARAM_RE.findall(line):
                    if not (rel == PARAM_REGISTRY and self.params.get(prm) == i + 1):
                        self.param_refs.setdefault(prm, []).append((rel, i + 1))

    def check_references(self):
        for ident, where in sorted(self.refs.items()):
            if ident in self.defs:
                continue
            check = "adr-undefined" if ident.startswith("ADR-") else "id-undefined"
            rel, line = where[0]
            home = self.home_of(ident.split("-")[0])
            self.add(check, rel, line, f"{ident} is referenced but {home} does not define it")
        for prm, where in sorted(self.param_refs.items()):
            if prm not in self.params:
                rel, line = where[0]
                self.add("param-undefined", rel, line,
                         f"{prm} has no row in {PARAM_REGISTRY}; add it to the registry")
        for ident, (rel, line) in sorted(self.defs.items()):
            if ident in self.closed or ident in self.refs or ident in self.soft:
                continue
            self.add("id-orphan", rel, line, f"{ident} is defined but nothing refers to it")
        for prm, line in sorted(self.params.items()):
            others = [r for r in self.param_refs.get(prm, []) if r[0] != PARAM_REGISTRY]
            if not others:
                self.add("param-unused", PARAM_REGISTRY, line,
                         f"{prm} is in the registry, but no other document cites it")

    def check_links(self):
        anchors = {rel: {slugify(t) for _, _, t in d.sections} for rel, d in self.docs.items()}
        reachable = {README}
        for rel, doc in self.docs.items():
            base = (self.spec / rel).parent
            for i, line in enumerate(doc.lines):
                if doc.in_fence[i]:
                    continue
                for target in LINK_RE.findall(line):
                    if re.match(r"^[a-z]+://|^mailto:", target):
                        continue
                    path, _, frag = target.partition("#")
                    dest = (base / path).resolve() if path else (self.spec / rel).resolve()
                    if not dest.exists():
                        self.add("link-missing-file", rel, i + 1, f"the link target {target} does not exist")
                        continue
                    if dest.is_dir() and (dest / "README.md").exists():
                        dest = dest / "README.md"
                    try:
                        drel = dest.relative_to(self.spec.resolve()).as_posix()
                    except ValueError:
                        continue
                    if rel in (README, DECISION_LOG):
                        reachable.add(drel)
                    if frag and drel in anchors and frag not in anchors[drel]:
                        self.add("link-missing-anchor", rel, i + 1,
                                 f"{drel} has no heading with the anchor #{frag}")
        for rel in self.docs:
            if rel not in reachable:
                self.add("doc-unlisted", rel, 1,
                         f"{rel} is not in the document map of {README} or in {DECISION_LOG}")
        readme = self.docs.get(README)
        if readme:
            for i, cells in table_rows(readme, section_lines(readme, "Document map")):
                for target in LINK_RE.findall(cells[0] if cells else ""):
                    if not (self.spec / target.partition("#")[0]).exists():
                        self.add("doc-missing", README, i + 1, f"the document map lists {target}, "
                                 "which does not exist")

    def check_headings(self):
        for rel, doc in self.docs.items():
            seen = {}
            for idx, _, title in doc.sections:
                slug = slugify(title)
                if slug in seen:
                    self.add("heading-duplicate", rel, idx + 1,
                             f"the heading '{title}' has the same anchor as line {seen[slug]}")
                else:
                    seen[slug] = idx + 1

    # ── entries ──

    def entries(self, pfx):
        """(id, rel, start, block text) of each bold-defined entry with this prefix."""
        rel = self.home_of(pfx)
        doc = self.docs.get(rel)
        if not doc:
            return
        starts = []
        for i, line in enumerate(doc.lines):
            if doc.in_fence[i]:
                continue
            m = BOLD_DEF_RE.match(line)
            if m or HEADING_RE.match(line):
                ids = [f"{p}-{n}" for p, n in MULTI_DEF_RE.findall(m.group(0))] if m else []
                starts.append((i, ids))
        for k, (i, ids) in enumerate(starts):
            end = starts[k + 1][0] if k + 1 < len(starts) else len(doc.lines)
            for ident in ids:
                if ident.startswith(pfx + "-"):
                    yield ident, rel, i, doc.lines[i:end]

    def check_shape(self):
        for ident, rel, i, block in self.entries("REQ"):
            if not re.search(r"\[(MUST|SHOULD|MAY)\b[^\]]*\]", block[0]):
                self.add("req-missing-tag", rel, i + 1, f"{ident} has no [MUST], [SHOULD] or [MAY] tag")
            if not any(line.startswith("*Acceptance:*") for line in block):
                self.add("req-missing-acceptance", rel, i + 1, f"{ident} has no *Acceptance:* field")
        self.inv_checks = {}
        for ident, rel, i, block in self.entries("INV"):
            if not re.search(r"\[(state|transition|response|recovery)\]", block[0]):
                self.add("inv-missing-scope", rel, i + 1,
                         f"{ident} has no scope tag [state], [transition], [response] or [recovery]")
            check = next((line for line in block if line.startswith("*Check:*")), None)
            if check is None:
                self.add("inv-missing-check", rel, i + 1, f"{ident} has no *Check:* field")
                continue
            hard, _ = expand_ids(check)
            cts = {x for x in hard if x.startswith("CT-")}
            self.inv_checks[ident] = (rel, i + block.index(check) + 1, cts)
            for ct in sorted(cts):
                if ct not in self.defs:
                    self.add("inv-check-unknown", rel, i + 1, f"{ident} names {ct}, which the test-class "
                             f"table of {CONFORMANCE} does not define")

    # ── traceability ──

    def matrix(self, title):
        doc = self.docs.get(CONFORMANCE)
        rows = {}
        if not doc:
            return rows
        for i, cells in table_rows(doc, section_lines(doc, title)):
            hard, soft = expand_ids(cells[0])
            classes = expand_ids(cells[1])[0] if len(cells) > 1 else set()
            status = plain_cell(cells[2]) if len(cells) > 2 else ""
            if title == ACCEPTANCE_MATRIX:
                status, classes = plain_cell(cells[1]) if len(cells) > 1 else "", set()
            for ident in hard | soft:
                rows[ident] = (i + 1, {c for c in classes if c.startswith("CT-")}, status)
        return rows

    def check_traceability(self):
        acc = self.matrix(ACCEPTANCE_MATRIX)
        props = self.matrix(PROPERTIES_MATRIX)
        for ident, (rel, line) in sorted(self.defs.items()):
            pfx = ident.split("-")[0]
            if ident in self.closed:
                continue
            if pfx in ACCEPTANCE_PREFIXES and ident not in acc:
                self.add("trace-missing", rel, line,
                         f"{ident} has no row in the {ACCEPTANCE_MATRIX.lower()} of {CONFORMANCE}")
            if pfx in PROPERTY_PREFIXES and ident not in props:
                self.add("trace-missing", rel, line,
                         f"{ident} has no row in the {PROPERTIES_MATRIX.lower()} of {CONFORMANCE}")
        for rows in (acc, props):
            for ident, (line, classes, _) in rows.items():
                for x in {ident} | classes:
                    if x not in self.defs and x not in self.soft:
                        self.add("trace-unknown", CONFORMANCE, line, f"the matrix row names {x}, "
                                 "which no document defines")
        for ident, (rel, line, cts) in sorted(self.inv_checks.items()):
            row = props.get(ident)
            if not row:
                continue
            only_check, only_row = cts - row[1], row[1] - cts
            if only_check or only_row:
                parts = []
                if only_check:
                    parts.append(f"the matrix row lacks {', '.join(sorted(only_check))}")
                if only_row:
                    parts.append(f"the Check line lacks {', '.join(sorted(only_row))}")
                self.add("inv-check-matrix-mismatch", rel, line, f"{ident}: " + "; ".join(parts))

        doc = self.docs.get(CONFORMANCE)
        if not doc:
            return
        ct_needs, hc_status, hc_used, gated = {}, {}, set(), set()
        for i, cells in table_rows(doc, section_lines(doc, TEST_CLASSES)):
            m = re.match(r"CT-\d+$", cells[0])
            if m and len(cells) >= 4:
                ct_needs[cells[0]] = {x for x in expand_ids(cells[3])[0] if x.startswith("HC-")}
                hc_used |= ct_needs[cells[0]]
        for i, cells in table_rows(doc, section_lines(doc, HARNESS_REGISTER)):
            if re.match(r"HC-\d+$", cells[0]) and len(cells) >= 4:
                hc_status[cells[0]] = (i + 1, plain_cell(cells[3]))
        for i, cells in table_rows(doc, section_lines(doc, MERGE_GATES)):
            if not re.match(r"MG-\d+$", cells[0]) or len(cells) < 5:
                continue
            threshold = plain_cell(cells[2])
            rest = PARAM_RE.sub("", threshold)
            rest = re.sub(r"\b(and|or)\b|[,;\s]", "", rest)
            if not PARAM_RE.search(threshold) or rest:
                self.add("gate-threshold-literal", CONFORMANCE, i + 1,
                         f"{cells[0]} has the threshold '{threshold}'; use P-* symbols only")
            hcs = {x for x in expand_ids(cells[4])[0] if x.startswith("HC-")}
            if not hcs:
                self.add("gate-no-capability", CONFORMANCE, i + 1, f"{cells[0]} names no HC in 'Enforced by'")
            hc_used |= hcs
            named = expand_ids(" ".join(cells))[0]
            for ct in sorted(x for x in named if x.startswith("CT-")):
                if ct not in self.defs:
                    self.add("gate-unknown-class", CONFORMANCE, i + 1, f"{cells[0]} names {ct}, which "
                             "is not in the test-class table")
                gated.add(ct)
            for ct, needs in ct_needs.items():
                if needs & hcs:
                    gated.add(ct)
        for hc, (line, _) in sorted(hc_status.items()):
            if hc not in hc_used:
                self.add("hc-orphan", CONFORMANCE, line, f"{hc} is needed by no CT and no MG")
        covered_by = {}
        for ident, (line, classes, status) in props.items():
            for ct in classes:
                if status.startswith("C"):
                    covered_by.setdefault(ct, []).append(ident)
        for ct, needs in sorted(ct_needs.items()):
            statuses = [hc_status.get(h, (0, "U"))[1] for h in needs]
            if needs and all(s.startswith("U") for s in statuses) and covered_by.get(ct):
                self.add("ct-unbuildable", CONFORMANCE, self.defs.get(ct, (CONFORMANCE, 1))[1],
                         f"{ct} needs only capabilities at U, but {', '.join(sorted(covered_by[ct]))} "
                         "claim C through it")
            if ct not in gated:
                self.add("ct-ungated", CONFORMANCE, self.defs.get(ct, (CONFORMANCE, 1))[1],
                         f"{ct} is reachable from no merge gate; mark it as outside CI or gate it")

        defined = [x for x in self.defs if x.split("-")[0] in COVERAGE_PREFIXES and x not in self.closed]
        status = {**{k: v[2] for k, v in acc.items()}, **{k: v[2] for k, v in props.items()}}
        covered = sorted(x for x in defined if status.get(x, "").startswith("C"))
        self.metrics["property_coverage"] = {"covered": len(covered), "total": len(defined)}
        floor = read_floor(self.spec / COVERAGE_FLOOR_FILE)
        if floor is None:
            # A missing floor must not turn the gate off.
            self.add("coverage-floor-invalid", COVERAGE_FLOOR_FILE, 1,
                     f"{COVERAGE_FLOOR_FILE} is missing or does not hold one integer; "
                     f"store the current count, {len(covered)}")
            return
        self.metrics["property_coverage"]["floor"] = floor
        if len(covered) < floor:
            self.add("coverage-regressed", CONFORMANCE, 1,
                     f"{len(covered)} properties are at C, below the floor of {floor} in "
                     f"{COVERAGE_FLOOR_FILE}; restore the coverage, or lower the floor only "
                     "to correct a wrong C (MG-3)")

    # ── decision log ──

    def check_decisions(self):
        log = self.docs.get(DECISION_LOG)
        files = {}
        for rel, doc in self.docs.items():
            m = re.match(r"decisions/ADR-(\d+)-[\w.-]+\.md$", rel)
            if not m:
                continue
            n = int(m.group(1))
            files[n] = rel
            head = HEADING_RE.match(doc.lines[0]) if doc.lines else None
            hn = re.match(r"ADR-(\d+) — (.*)", head.group(2)) if head else None
            if not hn or int(hn.group(1)) != n:
                self.add("adr-id-mismatch", rel, 1, f"the heading of {rel} does not start with 'ADR-{n} — '")
            status = next((ln[len("Status:"):].strip() for ln in doc.lines if ln.startswith("Status:")), None)
            if status is None or not ALLOWED_ADR_STATUS.match(status):
                self.add("adr-bad-status", rel, 1, f"{rel} has the status '{status}'; use Proposed, "
                         "Accepted (date|historical), Rejected (date) or Superseded by ADR-n")
            files[n] = (rel, status, hn.group(2).strip() if hn else "")
        if not log:
            return
        rows, accepted = {}, []
        for i, cells in table_rows(log, range(len(log.lines))):
            if len(cells) < 4:
                continue
            link = LINK_RE.search(cells[0])
            num = re.search(r"ADR-(\d+)", cells[0])
            if not num:
                continue
            n = int(num.group(1))
            rows[n] = i + 1
            title, status, when = plain_cell(cells[1]), plain_cell(cells[2]), plain_cell(cells[3])
            if not link:
                continue  # an external decision
            target = link.group(1)
            m = re.match(r"ADR-(\d+)-", Path(target).name)
            if not m or int(m.group(1)) != n:
                self.add("adr-id-mismatch", DECISION_LOG, i + 1, f"the row ADR-{n} links to {target}")
            if n not in files:
                self.add("adr-missing-file", DECISION_LOG, i + 1, f"ADR-{n} has a log row but no file")
                continue
            rel, fstatus, ftitle = files[n]
            if fstatus and status != fstatus:
                self.add("adr-status-mismatch", DECISION_LOG, i + 1,
                         f"ADR-{n}: the log says '{status}', {rel} says '{fstatus}'")
            fdate = re.search(r"\d{4}-\d{2}-\d{2}", fstatus or "")
            ldate = re.search(r"\d{4}-\d{2}-\d{2}", when)
            if (fdate.group(0) if fdate else None) != (ldate.group(0) if ldate else None):
                self.add("adr-date-mismatch", DECISION_LOG, i + 1,
                         f"ADR-{n}: the log date '{when}' differs from the status date of {rel}")
            strip = lambda s: re.sub(r"\s+", " ", re.sub(r"\([^)]*\)", "", s)).strip().lower()
            if strip(title) != strip(ftitle):
                self.add("adr-title-drift", DECISION_LOG, i + 1,
                         f"ADR-{n}: the log title differs from the heading of {rel}")
            if status.startswith(("Accepted", "Superseded")):
                accepted.append((n, ldate.group(0) if ldate else None, i + 1))
        for n, (rel, _, _) in sorted(files.items()):
            if n not in rows:
                self.add("adr-missing-row", rel, 1, f"ADR-{n} has no row in {DECISION_LOG}")
        for (n1, _, _), (n2, _, line) in zip(accepted, accepted[1:]):
            if n2 < n1:
                self.add("adr-log-order", DECISION_LOG, line,
                         f"ADR-{n2} follows ADR-{n1} out of number order")

    # ── normative text ──

    def normative(self):
        for rel, doc in self.docs.items():
            if rel.startswith("decisions/") or rel in MUTABLE_DOCS:
                continue
            yield rel, doc

    def check_constants_and_leakage(self):
        unit = (r"(?<![\w.^-])\d[\d,.]*\s?(?:ms|µs|us|ns|s|min|h|KiB|MiB|GiB|KB|MB|GB|TB|%|"
                r"rows/s|blocks|bytes|seconds|minutes|hours|days)\b")
        unit_re = re.compile(unit)
        deps = self.dependencies()
        dep_re = re.compile(r"(?<![\w-])(" + "|".join(re.escape(d) for d in sorted(deps, key=len, reverse=True))
                            + r")(?![\w-])", re.I) if deps else None
        kw_re = [re.compile(k) for k in LANGUAGE_KEYWORDS]
        for rel, doc in self.normative():
            for i, line in enumerate(doc.lines):
                if doc.in_fence[i]:
                    continue
                text = re.sub(r"`[^`]*`", " ", line)
                text = PARAM_RE.sub(" ", text)
                if not BARE_CONSTANT_SKIP_SECTIONS.search(section_of(doc, i)):
                    for m in unit_re.finditer(text):
                        self.add("param-bare-constant", rel, i + 1,
                                 f"the literal '{m.group(0)}' needs a P-* symbol in {PARAM_REGISTRY}")
                if dep_re:
                    for m in dep_re.finditer(text):
                        self.add("impl-leakage", rel, i + 1, f"'{m.group(1)}' is a dependency name; "
                                 "describe the mechanism, or move the choice to an ADR")
                if rel not in SURFACE_DOCS:
                    for k in kw_re:
                        m = k.search(line)
                        if m:
                            self.add("impl-leakage", rel, i + 1, f"'{m.group(0)}' is code or a path; "
                                     "normative text names behavior only")

    def dependencies(self) -> set:
        names = set()
        for manifest in MANIFESTS:
            p = self.repo / manifest
            if not p.exists():
                continue
            if p.suffix == ".json":
                data = json.loads(p.read_text())
                for key in ("dependencies", "devDependencies", "optionalDependencies"):
                    for name in data.get(key, {}):
                        if not name.startswith("@types/"):  # type stubs, named after their runtime
                            names.add(name.split("/")[-1])
            else:
                section = None
                for line in p.read_text().splitlines():
                    s = re.match(r"^\[([\w.-]+)\]", line)
                    if s:
                        section = s.group(1)
                        continue
                    if section and section.endswith("dependencies"):
                        m = re.match(r"^([A-Za-z0-9_-]+)\s*=", line)
                        if m:
                            names.add(m.group(1))
        return {n for n in names if len(n) > 3 and n.lower() not in LEAK_ALLOW}

    def check_warn_markers(self):
        routed = {prm for prm, line in self.params.items()
                  if WARN_ROUTE.search(self.docs[PARAM_REGISTRY].lines[line - 1])}
        for rel, doc in self.docs.items():
            if rel == README or re.match(r"decisions/ADR-\d+-", rel):
                continue  # README defines the marker; an ADR is itself the route
            for i, line in enumerate(doc.lines):
                if "⚠" not in line or doc.in_fence[i]:
                    continue
                if line.lstrip().startswith("|"):
                    nxt = doc.lines[i + 1] if i + 1 < len(doc.lines) else ""
                    if TABLE_SEP_RE.match(nxt):
                        continue
                    unit = line
                else:
                    a = i
                    while a > 0 and doc.lines[a - 1].strip():
                        a -= 1
                    b = i
                    while b + 1 < len(doc.lines) and doc.lines[b + 1].strip():
                        b += 1
                    unit = " ".join(doc.lines[a:b + 1])
                if WARN_ROUTE.search(unit) or set(PARAM_RE.findall(unit)) & routed:
                    continue
                self.add("warn-unratified", rel, i + 1,
                         "this ⚠ routes nowhere; name the ADR, OQ or GAP that ratifies it")

    def check_number_gaps(self):
        nums = {}
        for ident in self.defs:
            pfx, n = ident.split("-")
            nums.setdefault(pfx, set()).add(int(n))
        for pfx, ns in sorted(nums.items()):
            # The numbers of deleted gaps stay unused.
            if pfx == "GAP":
                continue
            bands = BANDS.get(pfx, [(min(ns), max(ns))])
            for lo, hi in bands:
                inside = sorted(n for n in ns if lo <= n <= hi)
                for a, b in zip(inside, inside[1:]):
                    if b > a + 1:
                        rel, line = self.defs[f"{pfx}-{b}"]
                        unused = f"{pfx}-{a + 1}" if b == a + 2 else f"{pfx}-{a + 1}..{pfx}-{b - 1}"
                        self.add("id-number-gap", rel, line, f"{unused} unused inside the band {lo}–{hi}")

    def check_gaps(self):
        gaps = {ident: where for ident, where in self.defs.items() if ident.startswith("GAP-")}
        for ident, (rel, line) in sorted(gaps.items()):
            section = section_of(self.docs[rel], line - 1)
            if section != GAP_REGISTER:
                self.add("gap-kept", rel, line, f"{ident} is in '{section}', not in the {GAP_REGISTER.lower()}: "
                         "delete a closed gap and every reference to it")

        lines = self.docs[CONFORMANCE].lines
        found = [(i, NEXT_GAP_RE.match(line)) for i, line in enumerate(lines)]
        found = [(i, int(m.group(1))) for i, m in found if m]
        if not found:
            self.add("gap-next", CONFORMANCE, 1, "the gap register has no **Next gap number:** line")
            return

        line, next_number = found[0]
        top = max((int(ident.split("-")[1]) for ident in gaps), default=0)
        if top >= next_number:
            self.add("gap-next", CONFORMANCE, line + 1,
                     f"GAP-{top} is not below the next gap number {next_number}; raise the number")

    # ── freshness ──

    def git(self, *args):
        try:
            out = subprocess.run(["git", "-C", str(self.repo), *args], capture_output=True, text=True,
                                 timeout=20)
            return out.stdout if out.returncode == 0 else None
        except (OSError, subprocess.SubprocessError):
            return None

    def check_freshness(self):
        asof = {}
        for rel in MUTABLE_DOCS:
            doc = self.docs.get(rel)
            if not doc:
                continue
            dates = sorted({d for line in doc.lines for d in ASOF_RE.findall(line)})
            if len(dates) > 1:
                self.add("date-inconsistent", rel, 1, f"{rel} states several as-of dates: {', '.join(dates)}")
            if dates:
                asof[rel] = dates[-1]

        tracked = self.git("ls-files", "--", str(self.spec))
        if tracked is None:
            self.metrics["freshness"] = "skipped: no git repository"
            return
        tracked = {Path(p).as_posix() for p in tracked.split()}
        spec_rel = self.spec.resolve().relative_to(self.repo.resolve()).as_posix()
        bar = asof.get(CONFORMANCE)
        if tracked and bar:
            status = self.git("status", "--porcelain", "--", str(self.spec)) or ""
            dirty = {line[3:].strip() for line in status.splitlines()}
            conf_dirty = f"{spec_rel}/{CONFORMANCE}" in dirty
            for rel, _ in self.normative():
                path = f"{spec_rel}/{rel}"
                if path not in tracked:
                    continue
                when = (self.git("log", "-1", "--format=%cs", "--", path) or "").strip()
                if when and when > bar:
                    self.add("stale-matrix", rel, 1, f"{rel} changed on {when}, after the as-of date "
                             f"{bar} of {CONFORMANCE}; update the matrices in the same change")
                if path in dirty and not conf_dirty:
                    self.add("stale-matrix", rel, 1, f"{rel} has local changes, but {CONFORMANCE} "
                             "does not; they change together")
        elif not tracked:
            self.metrics["freshness"] = "skipped: the suite is not tracked by git"

        hashes = {}
        for rel, doc in self.docs.items():
            if not (rel.startswith("decisions/") or rel in MUTABLE_DOCS):
                continue
            for i, line in enumerate(doc.lines):
                for h in re.findall(r"(?<![\w-])([0-9a-f]{7,12})(?![\w-])", line):
                    if re.search(r"\d", h) and re.search(r"[a-f]", h):
                        hashes.setdefault(h, (rel, i + 1))
        if hashes:
            out = subprocess.run(["git", "-C", str(self.repo), "cat-file", "--batch-check"],
                                 input="".join(f"{h}^{{commit}}\n" for h in hashes),
                                 capture_output=True, text=True)
            if out.returncode == 0:
                for h, res in zip(hashes, out.stdout.splitlines()):
                    if res.endswith("missing"):
                        rel, line = hashes[h]
                        self.add("commit-unknown", rel, line, f"the commit {h} is not in this repository; "
                                 "cite it by its subject if history changed")

    # ── STE ──

    def check_ste(self):
        for rel, doc in self.docs.items():
            normative = not rel.startswith("decisions/")
            for kind, parts in prose_units(doc):
                ste_unit(self, rel, kind, parts, normative)

    def run(self):
        self.collect()
        self.check_references()
        self.check_links()
        self.check_headings()
        self.check_shape()
        self.check_traceability()
        self.check_decisions()
        self.check_constants_and_leakage()
        self.check_warn_markers()
        self.check_number_gaps()
        self.check_gaps()
        self.check_freshness()
        self.check_ste()
        return self.findings


# ─── STE helpers ───────────────────────────────────────────────────────────────

LIST_ITEM_RE = re.compile(r"^(?:[-*+]|(\d+)[.)])\s+")
FIELD_RE = re.compile(r"^(\*{1,2}|_)[A-Z][^*_]{0,30}:\1\s*")
RUN_IN_RE = re.compile(r"^\*\*[^*]+\*\*\s*(?:\[[^\]]*\]\s*)*(?:—\s*known-(?:violated|suspect)\s*\([^)]*\)\s*)?")
SENTENCE_END_RE = re.compile(r"(?<=[.!?])[\"')\]]*\s+(?=[A-Z0-9(\"“])")
MODAL_RE = re.compile(r"\b(must|should|may)\b")
QUOTED_RE = re.compile(r"\"[^\"]*\"|“[^”]*”|'[^']*'")
PASSIVE_RE = re.compile(
    r"\b(?:is|are|was|were|be|been|being)\s+(?:\w+ly\s+)?"
    r"(\w+(?:ed|en)|built|done|found|held|kept|known|made|put|read|run|sent|set|shown)"
    r"\s+by\s+(?=(.{0,20}))", re.I)
KEY_PARTICIPLES = {"ordered", "sorted", "grouped", "keyed", "indexed", "partitioned", "divided",
                   "multiplied", "bounded", "increased", "decreased", "followed", "given"}
NOT_AGENT_RE = re.compile(r"(?:\d|P-|CODE|default|design|construction|definition|convention|value|"
                          r"reference|name\b|hand\b|then\b|now\b|at (?:most|least)|up to|one\b|"
                          r"a factor|itself|themselves|position)", re.I)


def prose_units(doc: Doc):
    units, cur = [], None
    comment = False
    for i, raw in enumerate(doc.lines):
        if doc.in_fence[i]:
            cur = None
            continue
        line = re.sub(r"<!--.*?-->", " ", raw)
        if comment:
            if "-->" not in line:
                continue
            comment, line = False, line.split("-->", 1)[1]
        if "<!--" in line:
            comment, line = True, line.split("<!--", 1)[0]
        text = line.strip().lstrip(">").strip()
        if not text or text.startswith("#") or re.match(r"^[-=*_]{3,}$", text):
            cur = None
            continue
        if text.startswith("|"):
            cur = None
            nxt = doc.lines[i + 1].strip() if i + 1 < len(doc.lines) else ""
            if TABLE_SEP_RE.match(nxt) or TABLE_SEP_RE.match(text):
                continue
            for cell in table_cells(text):
                units.append(["cell", [(i + 1, cell)]])
            continue
        item = LIST_ITEM_RE.match(text)
        if item or FIELD_RE.match(text) or cur is None:
            kind = "step" if item and item.group(1) else "para"
            cur = [kind, [(i + 1, text[item.end():] if item else text)]]
            units.append(cur)
        else:
            cur[1].append((i + 1, text))
    return units


def plain(text: str, first: bool) -> str:
    text = re.sub(r"`[^`]*`", "CODE", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)
    if first:
        text = RUN_IN_RE.sub("", text.strip(), count=1)
        text = FIELD_RE.sub("", text, count=1)
    return text.replace("*", "").strip()


def words(s: str) -> int:
    return sum(1 for t in s.split() if re.search(r"[A-Za-z0-9]", t))


def split_sentences(prose: str):
    spans, start = [], 0
    for m in SENTENCE_END_RE.finditer(prose):
        spans.append((start, m.start()))
        start = m.end()
    spans.append((start, len(prose)))
    for a, b in spans:
        s = prose[a:b]
        if words(s):
            yield a + len(s) - len(s.lstrip()), s.strip()


def ste_unit(ck: Checker, rel, kind, parts, normative):
    offsets, lines, chunks, pos = [], [], [], 0
    for k, (line_no, raw) in enumerate(parts):
        chunk = plain(raw, k == 0)
        offsets.append(pos)
        lines.append(line_no)
        chunks.append(chunk)
        pos += len(chunk) + 1
    prose = " ".join(chunks)

    def at(off):
        return lines[bisect_right(offsets, off) - 1]

    found = list(split_sentences(prose))
    limit = 20 if kind == "step" else 25
    for off, s in found:
        n = words(s)
        if n > limit:
            preview = " ".join(s.split()[:8])
            ck.add("ste-sentence-length", rel, at(off),
                   f"{n} words (limit {limit}); split it or use a vertical list: \"{preview} …\"")
    if kind != "cell" and len(found) > 6:
        ck.add("ste-paragraph-length", rel, at(found[0][0]),
               f"{len(found)} sentences in one unit (limit 6); split it by topic")
    unquoted = QUOTED_RE.sub(lambda m: " " * len(m.group(0)), prose)
    if normative:
        for m in MODAL_RE.finditer(unquoted):
            ck.add("ste-modal-case", rel, at(m.start()), f"lowercase \"{m.group(1)}\" has no RFC 2119 force; "
                   f"write {m.group(1).upper()}, or rephrase (\"can\" for a possibility)")
    for m in PASSIVE_RE.finditer(prose):
        if m.group(1).lower() not in KEY_PARTICIPLES and not NOT_AGENT_RE.match(m.group(2)):
            ck.add("ste-passive-agent", rel, at(m.start()),
                   f"passive with an agent \"{m.group(0)}{m.group(2).split(',')[0]}\"; make the agent the subject")
    low = unquoted.lower()
    for word, repl in STE_DENY.items():
        for m in re.finditer(r"(?<![\w-])" + re.escape(word) + r"(?![\w-])", low):
            ck.add("ste-word-list", rel, at(m.start()), f"\"{word}\": write {repl}")
    for m in re.finditer(r"(?<![\w-])([a-z]{3,}ing)(?![\w-])", unquoted):
        if m.group(1) not in ING_STOPLIST:
            ck.add("ste-ing-form", rel, at(m.start()), f"\"{m.group(1)}\" is an -ing form; rephrase it")


# ─── Ignore file, output, CLI ──────────────────────────────────────────────────


def load_ignores(spec: Path, findings: list):
    path = spec / IGNORE_FILE
    entries = []
    if not path.exists():
        return entries
    for n, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        body, _, why = line.partition("#")
        parts = [p.strip() for p in body.split("|")]
        if len(parts) != 3 or not why.strip():
            findings.append(Finding("ignore-unjustified", E, IGNORE_FILE, n,
                                    "an ignore entry needs 'check | file-glob | message-regex  # why'"))
            continue
        entries.append((parts[0], parts[1], re.compile(parts[2])))
    return entries


def ignored(f: Finding, entries) -> bool:
    return any((c == f.check or c == "*") and fnmatch.fnmatch(f.file, g) and r.search(f.message)
               for c, g, r in entries)


def main() -> int:
    ap = argparse.ArgumentParser(description="Quality gate for the Settle specification suite.")
    ap.add_argument("--spec", type=Path, default=Path(__file__).resolve().parent.parent,
                    help="the suite folder (default: the parent of tools/)")
    ap.add_argument("--format", choices=["text", "json", "github"], default="text")
    ap.add_argument("--severity", choices=[E, W, I], default=W, help="lowest severity to show")
    ap.add_argument("--only", action="append", default=[], metavar="CHECK", help="run only these checks")
    ap.add_argument("--strict", action="store_true", help="warnings also gate")
    ap.add_argument("--no-ignore", action="store_true", help=f"do not read {IGNORE_FILE}")
    ap.add_argument("--list-checks", action="store_true", help="print the check catalog and exit")
    args = ap.parse_args()

    if args.list_checks:
        for name, (sev, text) in CHECKS.items():
            print(f"{name:28} {sev:8} {text}")
        return 0
    unknown = [c for c in args.only if c not in CHECKS]
    if unknown:
        ap.error(f"unknown check(s): {', '.join(unknown)}; see --list-checks")
    spec = args.spec.resolve()
    if not (spec / README).exists():
        ap.error(f"{spec} has no {README}")
    repo_out = subprocess.run(["git", "-C", str(spec), "rev-parse", "--show-toplevel"],
                              capture_output=True, text=True)
    repo = Path(repo_out.stdout.strip()) if repo_out.returncode == 0 else spec.parent

    ck = Checker(spec, repo)
    findings = ck.run()
    entries = [] if args.no_ignore else load_ignores(spec, findings)
    rank = {E: 0, W: 1, I: 2}
    shown = [f for f in findings
             if rank[f.severity] <= rank[args.severity]
             and (not args.only or f.check in args.only)
             and not (f.check != "ignore-unjustified" and ignored(f, entries))]
    shown.sort(key=lambda f: (rank[f.severity], f.file, f.line, f.check))

    try:
        prefix = spec.relative_to(repo).as_posix() + "/"
    except ValueError:
        prefix = ""
    if args.format == "json":
        print(json.dumps({"findings": [asdict(f) for f in shown], "metrics": ck.metrics}, indent=2))
    elif args.format == "github":
        level = {E: "error", W: "warning", I: "notice"}
        for f in shown:
            print(f"::{level[f.severity]} file={prefix}{f.file},line={f.line},title={f.check}::{f.message}")
    else:
        for f in shown:
            print(f"{f.file}:{f.line}: {f.severity} [{f.check}] {f.message}")
        counts = {s: sum(1 for f in shown if f.severity == s) for s in (E, W, I)}
        cov = ck.metrics.get("property_coverage", {})
        print(f"\n{counts[E]} errors, {counts[W]} warnings, {counts[I]} notices in {len(ck.docs)} files; "
              f"{len(ck.defs)} IDs, {len(ck.params)} parameters; property coverage "
              f"{cov.get('covered', 0)}/{cov.get('total', 0)} at C"
              + (f" (floor {cov['floor']})" if "floor" in cov else ""))
        if "freshness" in ck.metrics:
            print(f"freshness checks {ck.metrics['freshness']}")

    gating = {E, W} if args.strict else {E}
    return 1 if any(f.severity in gating for f in shown) else 0


if __name__ == "__main__":
    sys.exit(main())
