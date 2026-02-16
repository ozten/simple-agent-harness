// Embedded default assets baked into the binary at compile time.
//
// These files live under `defaults/` in the repo and are included via
// `include_str!()` so blacksmith can bootstrap a `.blacksmith/` directory
// on any repo without requiring the user to copy files manually.

// -- Top-level config files --------------------------------------------------

pub const PROMPT_MD: &str = include_str!("../defaults/PROMPT.md");
pub const BLACKSMITH_TOML: &str = include_str!("../defaults/blacksmith.toml");

// -- Skills ------------------------------------------------------------------

pub const SKILL_BREAK_DOWN_ISSUE: &str =
    include_str!("../defaults/skills/break-down-issue/SKILL.md");
pub const SKILL_PRD_TO_BEADS: &str = include_str!("../defaults/skills/prd-to-beads/SKILL.md");
pub const SKILL_SELF_IMPROVEMENT: &str =
    include_str!("../defaults/skills/self-improvement/SKILL.md");

/// Assets that live inside .blacksmith/ — extracted on first-run init.
pub const INTERNAL_ASSETS: &[(&str, &str)] = &[("config.toml", BLACKSMITH_TOML)];

/// Assets that `--export` copies to the project root for git committing.
pub const EXPORTABLE_ASSETS: &[(&str, &str)] = &[
    ("PROMPT.md", PROMPT_MD),
    (
        ".claude/skills/break-down-issue/SKILL.md",
        SKILL_BREAK_DOWN_ISSUE,
    ),
    (".claude/skills/prd-to-beads/SKILL.md", SKILL_PRD_TO_BEADS),
    (
        ".claude/skills/self-improvement/SKILL.md",
        SKILL_SELF_IMPROVEMENT,
    ),
];
