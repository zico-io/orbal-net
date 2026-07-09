//! `orbal-net skill` prints the embedded skill, and `--install --dir <d>` writes it to
//! `<d>/orbal-net/SKILL.md`.

use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_orbal-net")
}

#[test]
fn prints_embedded_skill() {
    let out = Command::new(bin()).arg("skill").output().unwrap();
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(
        text.starts_with("---\nname: orbal-net"),
        "missing frontmatter"
    );
    assert!(text.contains("# orbal-net — mission coordination bus"));
}

#[test]
fn installs_to_dir() {
    let dir = std::env::temp_dir().join(format!("orbal-net-skill-test-{}", std::process::id()));
    let out = Command::new(bin())
        .args(["skill", "--install", "--dir"])
        .arg(&dir)
        .output()
        .unwrap();
    assert!(out.status.success());

    let dest = dir.join("orbal-net").join("SKILL.md");
    let printed = String::from_utf8(out.stdout).unwrap();
    assert_eq!(printed.trim(), dest.to_str().unwrap());
    let written = std::fs::read_to_string(&dest).unwrap();
    assert!(written.starts_with("---\nname: orbal-net"));

    std::fs::remove_dir_all(&dir).ok();
}
