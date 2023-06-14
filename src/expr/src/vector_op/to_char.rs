// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Debug, Write};
use std::sync::LazyLock;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use chrono::format::StrftimeItems;
use risingwave_common::types::Timestamp;
use static_assertions::const_assert_eq;

type Pattern<'a> = Vec<chrono::format::Item<'a>>;

self_cell::self_cell! {
    pub struct ChronoPattern {
        owner: String,
        #[covariant]
        dependent: Pattern,
    }
}

impl Debug for ChronoPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChronoPattern")
            .field("tmpl", self.borrow_owner())
            .finish()
    }
}

/// Compile the pg pattern to chrono pattern.
// TODO: Chrono can not fully support the pg format, so consider using other implementations later.
pub fn compile_pattern_to_chrono(tmpl: &str) -> ChronoPattern {
    // https://www.postgresql.org/docs/current/functions-formatting.html
    const PG_PATTERNS: &[&str] = &[
        "HH24", "hh24", "HH12", "hh12", "HH", "hh", "MI", "mi", "SS", "ss", "YYYY", "yyyy", "YY",
        "yy", "IYYY", "iyyy", "IY", "iy", "MM", "mm", "Month", "Mon", "DD", "dd", "US", "us", "MS",
        "ms", "TZH:TZM", "tzh:tzm", "TZHTZM", "tzhtzm", "TZH", "tzh",
    ];
    // https://docs.rs/chrono/latest/chrono/format/strftime/index.html
    const CHRONO_PATTERNS: &[&str] = &[
        "%H", "%H", "%I", "%I", "%I", "%I", "%M", "%M", "%S", "%S", "%Y", "%Y", "%y", "%y", "%G",
        "%G", "%g", "%g", "%m", "%m", "%B", "%b", "%d", "%d", "%6f", "%6f", "%3f", "%3f", "%:z",
        "%:z", "%z", "%z", "%#z", "%#z",
    ];
    const_assert_eq!(PG_PATTERNS.len(), CHRONO_PATTERNS.len());
    static AC: LazyLock<AhoCorasick> = LazyLock::new(|| {
        AhoCorasickBuilder::new()
            .ascii_case_insensitive(false)
            .match_kind(aho_corasick::MatchKind::LeftmostLongest)
            .build(PG_PATTERNS)
            .expect("failed to build an Aho-Corasick automaton")
    });

    let mut chrono_tmpl = String::new();
    AC.replace_all_with(tmpl, &mut chrono_tmpl, |mat, _, dst| {
        dst.push_str(CHRONO_PATTERNS[mat.pattern()]);
        true
    });
    tracing::debug!(tmpl, chrono_tmpl, "compile_pattern_to_chrono");
    ChronoPattern::new(chrono_tmpl, |tmpl| {
        StrftimeItems::new(tmpl).collect::<Vec<_>>()
    })
}

// #[function("to_char(timestamp, varchar) -> varchar")]
pub fn to_char_timestamp(data: Timestamp, tmpl: &str, writer: &mut dyn Write) {
    let pattern = compile_pattern_to_chrono(tmpl);
    let format = data.0.format_with_items(pattern.borrow_dependent().iter());
    write!(writer, "{}", format).unwrap();
}
