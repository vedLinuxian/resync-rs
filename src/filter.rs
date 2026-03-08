//! Filter engine — rsync-compatible exclude/include pattern matching.
//!
//! Supports glob patterns (*, **, ?, [chars]), directory patterns ending in /,
//! and first-match-wins rule ordering identical to rsync's `--exclude`/`--include`.

use std::fs;
use std::path::Path;

/// Whether a rule includes or excludes matched paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterKind {
    Include,
    Exclude,
}

/// A single include/exclude rule with a glob pattern.
#[derive(Debug, Clone)]
pub struct FilterRule {
    pub kind: FilterKind,
    /// Original pattern string (e.g. "*.log", "tmp/", "src/**/*.rs").
    pub pattern: String,
    /// If true, the pattern only matches directories (trailing `/` was present).
    pub dir_only: bool,
    /// If true, the pattern contains a `/` and must match against the full
    /// relative path.  Otherwise it is matched against the filename component
    /// only (rsync behaviour).
    pub anchored: bool,
}

/// Outcome of evaluating a path against the rule chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterResult {
    /// Explicitly included by a rule.
    Included,
    /// Explicitly excluded by a rule.
    Excluded,
    /// No rule matched — default is to include.
    Neutral,
}

/// The filter engine holds an ordered list of rules and evaluates paths
/// against them with first-match-wins semantics.
#[derive(Debug, Clone)]
pub struct FilterEngine {
    rules: Vec<FilterRule>,
}

impl FilterEngine {
    // ── construction ─────────────────────────────────────────────────────

    /// Create an empty filter (everything passes).
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Build from pre-parsed rules.
    pub fn from_rules(rules: Vec<FilterRule>) -> Self {
        Self { rules }
    }

    /// Convenience: build from CLI vectors.
    ///
    /// `excludes` and `includes` are interleaved in the order the user
    /// supplied them.  Because clap collects each flag separately we follow
    /// rsync convention: **includes are prepended** so they can override
    /// later excludes.  For full ordering control, callers should use
    /// [`from_rules`].
    pub fn from_cli_patterns(
        excludes: &[String],
        includes: &[String],
        exclude_files: &[PathBuf],
    ) -> anyhow::Result<Self> {
        let mut rules = Vec::new();

        // Includes first — gives them higher priority (first-match-wins).
        for pat in includes {
            rules.push(FilterRule::parse(pat, FilterKind::Include));
        }

        // Then explicit excludes.
        for pat in excludes {
            rules.push(FilterRule::parse(pat, FilterKind::Exclude));
        }

        // Then file-sourced excludes.
        for path in exclude_files {
            let content = fs::read_to_string(path).map_err(|e| {
                anyhow::anyhow!("failed to read exclude-from file {}: {}", path.display(), e)
            })?;
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                rules.push(FilterRule::parse(line, FilterKind::Exclude));
            }
        }

        Ok(Self { rules })
    }

    // ── queries ──────────────────────────────────────────────────────────

    /// Returns `true` if `rel_path` should be excluded from the transfer.
    ///
    /// `is_dir` should be `true` when the entry is a directory so that
    /// directory-only patterns (trailing `/`) are honoured.
    pub fn is_excluded(&self, rel_path: &Path, is_dir: bool) -> bool {
        self.evaluate(rel_path, is_dir) == FilterResult::Excluded
    }

    /// Evaluate the full rule chain and return the outcome.
    pub fn evaluate(&self, rel_path: &Path, is_dir: bool) -> FilterResult {
        for rule in &self.rules {
            if rule.matches(rel_path, is_dir) {
                return match rule.kind {
                    FilterKind::Include => FilterResult::Included,
                    FilterKind::Exclude => FilterResult::Excluded,
                };
            }
        }
        FilterResult::Neutral
    }

    /// Number of rules loaded.
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

impl Default for FilterEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ── FilterRule implementation ────────────────────────────────────────────────

impl FilterRule {
    /// Parse a user-supplied pattern string into a `FilterRule`.
    ///
    /// * Trailing `/` → `dir_only = true` (stripped from the stored pattern).
    /// * If the pattern contains a `/` (other than a trailing one) it is
    ///   *anchored*: matched against the full relative path.  Otherwise it
    ///   is matched against the filename component only.
    pub fn parse(pattern: &str, kind: FilterKind) -> Self {
        let mut pat = pattern.to_string();
        let dir_only = pat.ends_with('/');
        if dir_only {
            pat.pop(); // remove trailing /
        }

        // A pattern is anchored if it contains '/' after stripping the
        // trailing one.  Leading `./' is normalised away.
        let stripped = pat.strip_prefix("./").unwrap_or(&pat).to_string();
        let anchored = stripped.contains('/');
        Self {
            kind,
            pattern: stripped,
            dir_only,
            anchored,
        }
    }

    /// Test whether `rel_path` matches this rule.
    pub fn matches(&self, rel_path: &Path, is_dir: bool) -> bool {
        // Directory-only rules never match files.
        if self.dir_only && !is_dir {
            return false;
        }

        let path_str = rel_path.to_string_lossy();

        if self.anchored {
            // Match against the full relative path.
            glob_match(&self.pattern, &path_str)
        } else {
            // Match against the last component (filename or dir name).
            let name = rel_path
                .file_name()
                .map(|n| n.to_string_lossy())
                .unwrap_or_default();
            glob_match(&self.pattern, &name)
        }
    }
}

// ── Glob matching engine ─────────────────────────────────────────────────────
//
// Supports: `*`, `**`, `?`, `[abc]`, `[a-z]`, `[!a-z]`.
// `*`  matches anything except `/`.
// `**` matches everything including `/` (recursive wildcard).

/// Match `pattern` against `text` using rsync-style glob rules.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_inner(pattern.as_bytes(), text.as_bytes(), 0)
}

/// Maximum recursion depth for `**` glob expansion.
///
/// Each `**` segment in a pattern adds one level of recursion for every
/// possible split point in the text.  With k `**` segments and a text of
/// length n, the worst case is O(n^k) calls.  Capping depth at 32 prevents
/// stack overflow and limits pathological patterns to O(n^32) which in
/// practice terminates instantly for sane path lengths (< 4096 chars).
const MAX_GLOB_DEPTH: usize = 32;

fn glob_match_inner(pat: &[u8], txt: &[u8], depth: usize) -> bool {
    if depth > MAX_GLOB_DEPTH {
        return false;
    }
    let mut pi = 0;
    let mut ti = 0;

    // Saved positions for back-tracking on `*`.
    let mut star_pi: Option<usize> = None;
    let mut star_ti: usize = 0;

    while ti < txt.len() {
        if pi < pat.len() && pat[pi] == b'*' {
            // Check for `**` (recursive glob).
            if pi + 1 < pat.len() && pat[pi + 1] == b'*' {
                // `**` — match everything including `/`.
                // Skip the `**` and optional following `/`.
                let mut np = pi + 2;
                if np < pat.len() && pat[np] == b'/' {
                    np += 1;
                }
                // Try matching the rest of the pattern at every position.
                for start in ti..=txt.len() {
                    if glob_match_inner(&pat[np..], &txt[start..], depth + 1) {
                        return true;
                    }
                }
                return false;
            }

            // Single `*` — matches any number of chars except `/`.
            star_pi = Some(pi);
            star_ti = ti;
            pi += 1;
            continue;
        }

        if pi < pat.len() && pat[pi] == b'?' {
            // `?` matches one char that is not `/`.
            if txt[ti] == b'/' {
                // Back-track.
                if let Some(sp) = star_pi {
                    pi = sp + 1;
                    star_ti += 1;
                    ti = star_ti;
                    continue;
                }
                return false;
            }
            pi += 1;
            ti += 1;
            continue;
        }

        if pi < pat.len() && pat[pi] == b'[' {
            // Character class.
            if let Some((matched, end)) = match_char_class(&pat[pi..], txt[ti]) {
                if matched {
                    pi += end;
                    ti += 1;
                    continue;
                }
            }
            // No match — try back-track.
            if let Some(sp) = star_pi {
                pi = sp + 1;
                star_ti += 1;
                ti = star_ti;
                continue;
            }
            return false;
        }

        if pi < pat.len() && pat[pi] == txt[ti] {
            pi += 1;
            ti += 1;
            continue;
        }

        // Single `*` back-tracking: advance the text pointer.
        if let Some(sp) = star_pi {
            // `*` does not match `/`.
            if txt[star_ti] == b'/' {
                return false;
            }
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
            continue;
        }

        return false;
    }

    // Consume trailing stars.
    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }

    pi == pat.len()
}

/// Try to match a `[…]` character class at the start of `pat` against `ch`.
/// Returns `Some((matched, bytes_consumed))` or `None` if the bracket
/// expression is malformed.
fn match_char_class(pat: &[u8], ch: u8) -> Option<(bool, usize)> {
    debug_assert!(pat[0] == b'[');
    let mut i = 1;
    let negate = if i < pat.len() && pat[i] == b'!' {
        i += 1;
        true
    } else {
        false
    };

    let mut matched = false;
    let start = i;
    while i < pat.len() && (pat[i] != b']' || i == start) {
        if i + 2 < pat.len() && pat[i + 1] == b'-' && pat[i + 2] != b']' {
            // Range: [a-z]
            let lo = pat[i];
            let hi = pat[i + 2];
            if ch >= lo && ch <= hi {
                matched = true;
            }
            i += 3;
        } else {
            if pat[i] == ch {
                matched = true;
            }
            i += 1;
        }
    }

    if i >= pat.len() {
        return None; // unclosed bracket
    }
    // pat[i] == b']'
    let consumed = i + 1;
    Some((matched ^ negate, consumed))
}

// ── Rate limiter (token bucket) ──────────────────────────────────────────────

use std::sync::Mutex;
use std::time::Instant;

/// A thread-safe token-bucket rate limiter for bandwidth throttling.
///
/// Wraps network writes to enforce a maximum throughput in bytes/second.
pub struct RateLimiter {
    inner: Mutex<RateLimiterState>,
}

struct RateLimiterState {
    /// Maximum bytes per second (0 = unlimited).
    rate_bps: u64,
    /// Available tokens (bytes we are allowed to send right now).
    tokens: f64,
    /// Maximum bucket size (burst allowance = 1 second's worth).
    capacity: f64,
    /// When we last refilled.
    last_refill: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter.
    ///
    /// `rate_kbps` is the limit in **kilobytes per second** (like rsync
    /// `--bwlimit`).  Pass `0` for unlimited.
    pub fn new(rate_kbps: u64) -> Self {
        let rate_bps = rate_kbps * 1024;
        let capacity = rate_bps as f64; // 1 second burst
        Self {
            inner: Mutex::new(RateLimiterState {
                rate_bps,
                tokens: capacity,
                capacity,
                last_refill: Instant::now(),
            }),
        }
    }

    /// Returns `true` when the limiter is effectively unlimited.
    pub fn is_unlimited(&self) -> bool {
        let guard = self.inner.lock().unwrap();
        guard.rate_bps == 0
    }

    /// Wait until we are allowed to send `bytes` bytes, then consume the
    /// tokens.  This may sleep the calling thread.
    pub fn acquire(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        loop {
            let sleep_dur = {
                let mut state = self.inner.lock().unwrap();
                if state.rate_bps == 0 {
                    return; // unlimited
                }

                // Refill tokens based on elapsed time.
                let now = Instant::now();
                let elapsed = now.duration_since(state.last_refill).as_secs_f64();
                state.tokens += elapsed * state.rate_bps as f64;
                if state.tokens > state.capacity {
                    state.tokens = state.capacity;
                }
                state.last_refill = now;

                let needed = bytes as f64;
                if state.tokens >= needed {
                    state.tokens -= needed;
                    return;
                }

                // Not enough tokens — figure out how long to sleep.
                let deficit = needed - state.tokens;
                let secs = deficit / state.rate_bps as f64;
                std::time::Duration::from_secs_f64(secs)
            };

            std::thread::sleep(sleep_dur);
        }
    }

    /// Async variant — yields to the tokio runtime instead of blocking.
    pub async fn acquire_async(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        loop {
            let sleep_dur = {
                let mut state = self.inner.lock().unwrap();
                if state.rate_bps == 0 {
                    return;
                }

                let now = Instant::now();
                let elapsed = now.duration_since(state.last_refill).as_secs_f64();
                state.tokens += elapsed * state.rate_bps as f64;
                if state.tokens > state.capacity {
                    state.tokens = state.capacity;
                }
                state.last_refill = now;

                let needed = bytes as f64;
                if state.tokens >= needed {
                    state.tokens -= needed;
                    return;
                }

                let deficit = needed - state.tokens;
                let secs = deficit / state.rate_bps as f64;
                std::time::Duration::from_secs_f64(secs)
            };

            tokio::time::sleep(sleep_dur).await;
        }
    }
}

// ── Itemize-changes formatter ────────────────────────────────────────────────

/// What kind of update was performed on a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChangeType {
    /// Sent/received to remote.
    #[default]
    Transfer,
    /// Created locally.
    Create,
    /// Deleted.
    Delete,
    /// Metadata-only change.
    MetadataOnly,
}

/// Flags describing which attributes changed.
#[derive(Debug, Clone, Default)]
pub struct ItemizedChange {
    /// Kind of change (send, create, delete …).
    pub change_type: ChangeType,
    /// Is the entry a directory?
    pub is_dir: bool,
    /// Size changed.
    pub size_changed: bool,
    /// Modification time changed.
    pub time_changed: bool,
    /// Permissions changed.
    pub perms_changed: bool,
    /// Owner changed.
    pub owner_changed: bool,
    /// Group changed.
    pub group_changed: bool,
    /// Checksum changed.
    pub checksum_changed: bool,
    /// Relative path.
    pub path: String,
}

impl std::fmt::Display for ItemizedChange {
    /// Format in rsync `-i` style:
    ///
    /// ```text
    /// >f.st...... file.txt
    /// cd+++++++++ new_dir/
    /// *deleting   old.log
    /// .f....o.... secret.key
    /// ```
    ///
    /// Columns: `XY c s t p o g   PATH`
    ///   X = transfer direction (`>` sent, `c` created, `.` meta-only, `*` special)
    ///   Y = file type (`f` file, `d` directory)
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.change_type == ChangeType::Delete {
            return write!(f, "*deleting   {}", self.path);
        }

        let dir_char = if self.is_dir { 'd' } else { 'f' };

        let xfer_char = match self.change_type {
            ChangeType::Transfer => '>',
            ChangeType::Create => 'c',
            ChangeType::MetadataOnly => '.',
            ChangeType::Delete => unreachable!(),
        };

        // Build the 9-character attribute string.
        let c = if self.change_type == ChangeType::Create {
            '+'
        } else if self.checksum_changed {
            'c'
        } else {
            '.'
        };
        let s = if self.change_type == ChangeType::Create {
            '+'
        } else if self.size_changed {
            's'
        } else {
            '.'
        };
        let t = if self.change_type == ChangeType::Create {
            '+'
        } else if self.time_changed {
            't'
        } else {
            '.'
        };
        let p = if self.change_type == ChangeType::Create {
            '+'
        } else if self.perms_changed {
            'p'
        } else {
            '.'
        };
        let o = if self.change_type == ChangeType::Create {
            '+'
        } else if self.owner_changed {
            'o'
        } else {
            '.'
        };
        let g = if self.change_type == ChangeType::Create {
            '+'
        } else if self.group_changed {
            'g'
        } else {
            '.'
        };
        // Remaining 3 placeholder columns.
        let rest = if self.change_type == ChangeType::Create {
            "+++"
        } else {
            "..."
        };

        write!(
            f,
            "{}{}{}{}{}{}{}{}{} {}",
            xfer_char, dir_char, c, s, t, p, o, g, rest, self.path
        )
    }
}

// ── Backup helper ────────────────────────────────────────────────────────────

use std::path::PathBuf;

/// Create a backup of `target` before it is overwritten.
///
/// * If `backup_dir` is `Some`, the file is moved into that directory
///   preserving its relative path structure.
/// * Otherwise, the file is renamed in-place with `suffix` appended.
pub fn backup_file(
    target: &Path,
    base_dir: &Path,
    backup_dir: Option<&Path>,
    suffix: &str,
) -> std::io::Result<PathBuf> {
    if !target.exists() {
        // Nothing to back up.
        return Ok(target.to_path_buf());
    }

    let dest = if let Some(bdir) = backup_dir {
        // Preserve relative structure inside backup_dir.
        let rel = target.strip_prefix(base_dir).unwrap_or(target.as_ref());
        let mut dest = bdir.join(rel);
        // Append suffix.
        let name = format!(
            "{}{}",
            dest.file_name().unwrap_or_default().to_string_lossy(),
            suffix
        );
        dest.set_file_name(name);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        dest
    } else {
        // Rename in-place with suffix.
        let mut dest = target.as_os_str().to_os_string();
        dest.push(suffix);
        PathBuf::from(dest)
    };

    fs::rename(target, &dest)?;
    Ok(dest)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    // ── glob matching unit tests ────────────────────────────────────────

    #[test]
    fn glob_star_matches_filename() {
        assert!(glob_match("*.log", "debug.log"));
        assert!(!glob_match("*.log", "debug.txt"));
    }

    #[test]
    fn glob_star_does_not_cross_slash() {
        assert!(!glob_match("*.rs", "src/main.rs"));
    }

    #[test]
    fn glob_double_star() {
        assert!(glob_match("**/*.rs", "src/main.rs"));
        assert!(glob_match("**/*.rs", "src/net/server.rs"));
        assert!(glob_match("src/**", "src/net/server.rs"));
    }

    #[test]
    fn glob_question_mark() {
        assert!(glob_match("file?.txt", "file1.txt"));
        assert!(!glob_match("file?.txt", "file12.txt"));
    }

    #[test]
    fn glob_char_class() {
        assert!(glob_match("[abc].txt", "a.txt"));
        assert!(!glob_match("[abc].txt", "d.txt"));
    }

    #[test]
    fn glob_negated_char_class() {
        assert!(glob_match("[!abc].txt", "d.txt"));
        assert!(!glob_match("[!abc].txt", "a.txt"));
    }

    #[test]
    fn glob_range() {
        assert!(glob_match("[a-z].txt", "m.txt"));
        assert!(!glob_match("[a-z].txt", "M.txt"));
    }

    // ── filter engine tests ─────────────────────────────────────────────

    #[test]
    fn exclude_glob() {
        let engine =
            FilterEngine::from_rules(vec![FilterRule::parse("*.log", FilterKind::Exclude)]);
        assert!(engine.is_excluded(Path::new("debug.log"), false));
        assert!(!engine.is_excluded(Path::new("main.rs"), false));
    }

    #[test]
    fn include_overrides_exclude() {
        let engine = FilterEngine::from_rules(vec![
            FilterRule::parse("important.log", FilterKind::Include),
            FilterRule::parse("*.log", FilterKind::Exclude),
        ]);
        assert!(!engine.is_excluded(Path::new("important.log"), false));
        assert!(engine.is_excluded(Path::new("other.log"), false));
    }

    #[test]
    fn dir_only_pattern() {
        let engine =
            FilterEngine::from_rules(vec![FilterRule::parse(".git/", FilterKind::Exclude)]);
        assert!(engine.is_excluded(Path::new(".git"), true));
        assert!(!engine.is_excluded(Path::new(".git"), false)); // not a directory
    }

    #[test]
    fn anchored_pattern() {
        let engine =
            FilterEngine::from_rules(vec![FilterRule::parse("src/**/*.rs", FilterKind::Exclude)]);
        assert!(engine.is_excluded(Path::new("src/net/server.rs"), false));
        assert!(!engine.is_excluded(Path::new("tests/e2e.rs"), false));
    }

    #[test]
    fn no_rules_neutral() {
        let engine = FilterEngine::new();
        assert!(!engine.is_excluded(Path::new("anything.txt"), false));
    }

    // ── rate limiter basic test ─────────────────────────────────────────

    #[test]
    fn rate_limiter_unlimited() {
        let rl = RateLimiter::new(0);
        assert!(rl.is_unlimited());
        rl.acquire(1_000_000); // should return immediately
    }

    #[test]
    fn rate_limiter_limited() {
        let rl = RateLimiter::new(1024); // 1 MB/s
        assert!(!rl.is_unlimited());
        // Acquire a small amount — should succeed instantly.
        rl.acquire(512);
    }

    // ── itemize formatter test ──────────────────────────────────────────

    #[test]
    fn itemize_transfer() {
        let ic = ItemizedChange {
            change_type: ChangeType::Transfer,
            is_dir: false,
            size_changed: true,
            time_changed: true,
            path: "file.txt".into(),
            ..Default::default()
        };
        let s = ic.to_string();
        assert!(s.starts_with(">f"));
        assert!(s.contains("file.txt"));
    }

    #[test]
    fn itemize_create() {
        let ic = ItemizedChange {
            change_type: ChangeType::Create,
            is_dir: true,
            path: "new_dir/".into(),
            ..Default::default()
        };
        let s = ic.to_string();
        assert!(s.starts_with("cd"));
        assert!(s.contains('+'));
    }

    #[test]
    fn itemize_delete() {
        let ic = ItemizedChange {
            change_type: ChangeType::Delete,
            path: "old.log".into(),
            ..Default::default()
        };
        let s = ic.to_string();
        assert!(s.starts_with("*deleting"));
    }

    // ── backup tests ────────────────────────────────────────────────────

    #[test]
    fn backup_nonexistent_is_noop() {
        let p = Path::new("/tmp/resync_test_nonexistent_file_xyz");
        let result = backup_file(p, Path::new("/tmp"), None, "~");
        assert!(result.is_ok());
    }
}
