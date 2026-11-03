//! Repository identity discovered from local Git metadata, without invoking Git.

use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The repository and checkout that own a job's working directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobContext {
    /// Canonical primary repository checkout, shared by its linked worktrees.
    pub repository: String,
    /// Canonical checkout containing the working directory.
    pub worktree: String,
    /// Working directory relative to the checkout root; empty at its root.
    pub subdirectory: String,
}

impl JobContext {
    pub fn label(&self) -> String {
        let mut label = basename(&self.repository).to_owned();
        if self.worktree != self.repository {
            label.push_str(" · worktree ");
            label.push_str(basename(&self.worktree));
        }
        if !self.subdirectory.is_empty() {
            label.push_str(" · ");
            label.push_str(&self.subdirectory);
        }
        label
    }
}

fn basename(path: &str) -> &str {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(path)
}

/// Discover the nearest enclosing Git checkout. Missing or unrecognizable
/// metadata yields no context, rather than guessing from directory names.
pub fn discover(cwd: &Path) -> Option<JobContext> {
    let cwd = cwd.canonicalize().ok()?;
    if !cwd.is_dir() {
        return None;
    }
    for worktree in cwd.ancestors() {
        let marker = worktree.join(".git");
        match fs::symlink_metadata(&marker) {
            Ok(_) => return discover_at(&cwd, worktree, &marker),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return None,
        }
    }
    None
}

fn discover_at(cwd: &Path, worktree: &Path, marker: &Path) -> Option<JobContext> {
    let git_dir = if marker.is_dir() {
        marker.canonicalize().ok()?
    } else {
        let contents = read_marker(marker)?;
        let target = contents.strip_prefix("gitdir: ")?;
        resolve(worktree, target)?
    };
    if !git_dir.join("HEAD").is_file() {
        return None;
    }
    let common_marker = git_dir.join("commondir");
    let repository = match fs::symlink_metadata(&common_marker) {
        Ok(_) => {
            let common = resolve(&git_dir, &read_marker(&common_marker)?)?;
            if !common.join("objects").is_dir() {
                return None;
            }
            // A standard linked worktree's common directory is the primary
            // checkout's .git. Other layouts cannot safely be identified by
            // taking an arbitrary metadata directory's parent.
            if common.file_name()? != ".git" {
                return None;
            }
            common.parent()?.to_path_buf()
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // This includes submodules and --separate-git-dir checkouts: their
            // gitdir location does not determine their repository name.
            if !git_dir.join("objects").is_dir() {
                return None;
            }
            worktree.to_path_buf()
        }
        Err(_) => return None,
    };
    Some(JobContext {
        repository: repository.to_str()?.to_owned(),
        worktree: worktree.to_str()?.to_owned(),
        subdirectory: cwd.strip_prefix(worktree).ok()?.to_str()?.to_owned(),
    })
}

fn resolve(base: &Path, target: &str) -> Option<PathBuf> {
    if target.is_empty() || target.contains(['\n', '\r', '\0']) {
        return None;
    }
    base.join(target).canonicalize().ok()
}

fn read_marker(path: &Path) -> Option<String> {
    const LIMIT: u64 = 4096;
    // Reject special files before opening them; a FIFO must not stall status.
    if !fs::metadata(path).ok()?.is_file() {
        return None;
    }
    let mut bytes = Vec::new();
    File::open(path)
        .ok()?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > LIMIT {
        return None;
    }
    let value = String::from_utf8(bytes).ok()?;
    Some(value.trim_end_matches(['\n', '\r']).to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn git(cwd: &Path, args: &[&str]) {
        let output = Command::new("git")
            .current_dir(cwd)
            .args(args)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_COMMON_DIR")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn repository(root: &Path) -> PathBuf {
        let repo = root.join("project");
        fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "--quiet"]);
        git(
            &repo,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "commit",
                "--quiet",
                "--allow-empty",
                "-m",
                "initial",
            ],
        );
        repo.canonicalize().unwrap()
    }

    #[test]
    fn ordinary_checkout_and_nested_directory_have_same_repository() {
        let temp = tempfile::tempdir().unwrap();
        let repo = repository(temp.path());
        fs::create_dir_all(repo.join("experiments/run")).unwrap();
        let root = discover(&repo).unwrap();
        assert_eq!(root.repository, repo.to_str().unwrap());
        assert_eq!(root.worktree, root.repository);
        assert_eq!(root.subdirectory, "");
        assert_eq!(root.label(), "project");
        let nested = discover(&repo.join("experiments/run")).unwrap();
        assert_eq!(nested.repository, root.repository);
        assert_eq!(nested.subdirectory, "experiments/run");
        assert_eq!(nested.label(), "project · experiments/run");
    }

    #[test]
    fn external_linked_worktree_keeps_primary_repository_identity() {
        let temp = tempfile::tempdir().unwrap();
        let repo = repository(temp.path());
        let linked = temp.path().join("deep");
        git(
            &repo,
            &[
                "worktree",
                "add",
                "--quiet",
                "--detach",
                linked.to_str().unwrap(),
            ],
        );
        fs::create_dir(linked.join("tests")).unwrap();
        let context = discover(&linked.join("tests")).unwrap();
        assert_eq!(context.repository, repo.to_str().unwrap());
        assert_eq!(
            context.worktree,
            linked.canonicalize().unwrap().to_str().unwrap()
        );
        assert_eq!(context.label(), "project · worktree deep · tests");
    }

    #[test]
    fn submodule_is_its_own_repository() {
        let temp = tempfile::tempdir().unwrap();
        let repo = repository(temp.path());
        let parent = temp.path().join("parent");
        fs::create_dir(&parent).unwrap();
        git(&parent, &["init", "--quiet"]);
        git(
            &parent,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                "--quiet",
                repo.to_str().unwrap(),
                "child",
            ],
        );
        let child = parent.join("child").canonicalize().unwrap();
        let context = discover(&child).unwrap();
        assert_eq!(context.repository, child.to_str().unwrap());
        assert_eq!(context.worktree, context.repository);
        assert_eq!(context.label(), "child");
    }

    #[test]
    fn separate_git_directory_does_not_become_repository_identity() {
        let temp = tempfile::tempdir().unwrap();
        let checkout = temp.path().join("checkout");
        let metadata = temp.path().join("metadata");
        fs::create_dir(&checkout).unwrap();
        git(
            &checkout,
            &[
                "init",
                "--quiet",
                "--separate-git-dir",
                metadata.to_str().unwrap(),
            ],
        );
        let context = discover(&checkout).unwrap();
        assert_eq!(
            context.repository,
            checkout.canonicalize().unwrap().to_str().unwrap()
        );
        assert_eq!(context.label(), "checkout");
    }

    #[test]
    fn invalid_or_missing_metadata_is_not_a_repository() {
        let temp = tempfile::tempdir().unwrap();
        assert!(discover(temp.path()).is_none());
        assert!(discover(&temp.path().join("missing")).is_none());
        fs::write(temp.path().join(".git"), "not a gitdir marker\n").unwrap();
        assert!(discover(temp.path()).is_none());
        fs::write(temp.path().join(".git"), vec![b'x'; 4097]).unwrap();
        assert!(discover(temp.path()).is_none());
    }

    #[test]
    fn malformed_nested_git_marker_does_not_attribute_to_outer_repository() {
        let temp = tempfile::tempdir().unwrap();
        let repo = repository(temp.path());
        let nested = repo.join("nested");
        fs::create_dir(&nested).unwrap();
        fs::write(nested.join(".git"), "gitdir: missing\n").unwrap();
        assert!(discover(&nested).is_none());
    }
}
