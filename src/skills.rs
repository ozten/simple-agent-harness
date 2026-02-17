use std::path::Path;

/// Bundled Claude Code skills shipped with blacksmith.
/// Each entry is (relative_path, file_contents).
const BUNDLED_SKILLS: &[(&str, &str)] = &[
    (
        ".claude/skills/break-down-issue/SKILL.md",
        include_str!("../.claude/skills/break-down-issue/SKILL.md"),
    ),
    (
        ".claude/skills/prd-to-beads/SKILL.md",
        include_str!("../.claude/skills/prd-to-beads/SKILL.md"),
    ),
    (
        ".claude/skills/self-improvement/SKILL.md",
        include_str!("../.claude/skills/self-improvement/SKILL.md"),
    ),
];

/// Copy bundled Claude Code skills into the project directory.
///
/// Idempotent: skips files that already exist (never overwrites user customizations).
/// Returns the number of skills that were newly written.
pub fn install_bundled_skills(project_root: &Path) -> std::io::Result<usize> {
    let mut written = 0;
    for (rel_path, contents) in BUNDLED_SKILLS {
        let dest = project_root.join(rel_path);
        if dest.exists() {
            continue;
        }
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&dest, contents)?;
        written += 1;
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_install_creates_skills() {
        let tmp = tempfile::tempdir().unwrap();
        let count = install_bundled_skills(tmp.path()).unwrap();
        assert_eq!(count, 3);

        // All three skill files should exist
        assert!(tmp
            .path()
            .join(".claude/skills/break-down-issue/SKILL.md")
            .exists());
        assert!(tmp
            .path()
            .join(".claude/skills/prd-to-beads/SKILL.md")
            .exists());
        assert!(tmp
            .path()
            .join(".claude/skills/self-improvement/SKILL.md")
            .exists());

        // Contents should match the embedded data
        let content =
            std::fs::read_to_string(tmp.path().join(".claude/skills/break-down-issue/SKILL.md"))
                .unwrap();
        assert!(content.contains("Break Down Issue"));
    }

    #[test]
    fn test_install_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();

        let count1 = install_bundled_skills(tmp.path()).unwrap();
        assert_eq!(count1, 3);

        // Write custom content to one skill file
        let custom_path = tmp.path().join(".claude/skills/break-down-issue/SKILL.md");
        std::fs::write(&custom_path, "# My custom skill").unwrap();

        // Second install should skip all existing files
        let count2 = install_bundled_skills(tmp.path()).unwrap();
        assert_eq!(count2, 0);

        // Custom content should be preserved
        let content = std::fs::read_to_string(&custom_path).unwrap();
        assert_eq!(content, "# My custom skill");
    }
}
