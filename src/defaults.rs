/// Embedded default assets compiled into the binary via `include_str!()`.
///
/// These are the canonical versions of runtime files that blacksmith extracts
/// on first run into the `.blacksmith/` data directory.

pub const PROMPT_MD: &str = include_str!("../defaults/PROMPT.md");
pub const FINISH_SCRIPT: &str = include_str!("../defaults/bd-finish.sh");
pub const DEFAULT_CONFIG: &str = include_str!("../defaults/blacksmith.toml");
pub const SKILL_PRD_TO_BEADS: &str = include_str!("../defaults/skills/prd-to-beads.md");
pub const SKILL_BREAK_DOWN: &str = include_str!("../defaults/skills/break-down-issue.md");
pub const SKILL_SELF_IMPROVE: &str = include_str!("../defaults/skills/self-improvement.md");
