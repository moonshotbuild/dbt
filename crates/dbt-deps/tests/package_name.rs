//! End-to-end `dbt deps` tests for package names that are not a plain dir name.
//!
//! A package is installed to `dbt_packages/<name>`, and `<name>` comes from the
//! package's own `dbt_project.yml`. These drive the real
//! `get_or_install_packages` entry point against a `local:` package and a `git:` package
//! served from a local repo, without touching the network.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::{FsCommand, IoArgs};
use dbt_jinja_utils::phases::load::init::initialize_load_profile_jinja_environment;
use fs_deps::get_or_install_packages;
use fs_deps::private_package::LocalPrivatePackageResolver;
use tempfile::TempDir;
use url::Url;

/// A throwaway project tree with `local:` packages, ready for `dbt deps`.
struct TestProject {
    _tmp: TempDir,
    root: PathBuf,
}

impl TestProject {
    fn new() -> Self {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("dbt_project.yml"),
            "name: root_project\nprofile: default\n",
        )
        .unwrap();
        Self { _tmp: tmp, root }
    }

    /// A sibling directory, outside the project, that must stay untouched.
    fn outside(&self) -> PathBuf {
        self.root.parent().unwrap().join("outside")
    }

    /// Create a sibling package directory `dir` and declare it in `packages.yml`.
    fn with_local_package(&self, dir: &str, project_yml: &str) {
        let package_root = self.root.parent().unwrap().join(dir);
        fs::create_dir_all(&package_root).unwrap();
        fs::write(package_root.join("dbt_project.yml"), project_yml).unwrap();

        let packages_yml = self.root.join("packages.yml");
        let existing =
            fs::read_to_string(&packages_yml).unwrap_or_else(|_| "packages:\n".to_string());
        fs::write(
            &packages_yml,
            format!("{existing}  - local: \"../{dir}\"\n"),
        )
        .unwrap();
    }

    /// Create a sibling git repo `dir` and declare it in `packages.yml`.
    fn with_git_package(&self, dir: &str, project_yml: &str) {
        let repo = self.root.parent().unwrap().join(dir);
        fs::create_dir_all(&repo).unwrap();
        fs::write(repo.join("dbt_project.yml"), project_yml).unwrap();
        for args in [
            &["init", "-q", "-b", "main"][..],
            &["add", "."],
            &[
                "-c",
                "user.name=test",
                "-c",
                "user.email=test@example.com",
                "commit",
                "-q",
                "-m",
                "init",
            ],
        ] {
            let status = Command::new("git")
                .args(args)
                .current_dir(&repo)
                .status()
                .unwrap();
            assert!(status.success());
        }

        // A file URL has no backslashes, which a Windows path would put into the quoted YAML.
        let repo_url = Url::from_file_path(&repo).unwrap();
        let packages_yml = self.root.join("packages.yml");
        let existing =
            fs::read_to_string(&packages_yml).unwrap_or_else(|_| "packages:\n".to_string());
        fs::write(
            &packages_yml,
            format!("{existing}  - git: \"{repo_url}\"\n    revision: main\n"),
        )
        .unwrap();
    }

    async fn try_deps(&self) -> Result<(), Box<dbt_common::FsError>> {
        self.try_deps_into(&self.root.join("dbt_packages")).await
    }

    async fn try_deps_into(&self, install_path: &Path) -> Result<(), Box<dbt_common::FsError>> {
        self.try_get_or_install(install_path, FsCommand::Deps, true)
            .await
    }

    /// The no-lock auto-install every other command runs (`dbt parse`, `dbt compile`, ...): `install_deps` is false, and missing packages are installed through the same delete-then-install sink as `dbt deps`.
    async fn try_auto_install_into(
        &self,
        install_path: &Path,
    ) -> Result<(), Box<dbt_common::FsError>> {
        self.try_get_or_install(install_path, FsCommand::Compile, false)
            .await
    }

    async fn try_get_or_install(
        &self,
        install_path: &Path,
        command: FsCommand,
        install_deps: bool,
    ) -> Result<(), Box<dbt_common::FsError>> {
        let io = IoArgs {
            in_dir: self.root.clone(),
            out_dir: self.root.join("target"),
            ..Default::default()
        };
        let env = initialize_load_profile_jinja_environment();

        get_or_install_packages(
            &io,
            command,
            &env,
            install_path,
            install_deps,
            None,  // add_package
            false, // upgrade
            false, // lock
            Default::default(),
            false, // version_check
            false, // skip_private_deps
            None,  // replay_mode
            &CancellationToken::never_cancels(),
            false, // use_v2_compatible_package_downloads
            false, // require_hub_verified_downloads
            Arc::new(LocalPrivatePackageResolver),
            None, // cloud_config
            None, // ai_provider
        )
        .await
        .map(|_| ())
    }
}

/// Installs `good_pkg`, then adds a package named `bad_name` and runs `deps` again.
/// `deps` must fail, and the project and `good_pkg` must still be there.
async fn assert_name_is_rejected(bad_name: &str) {
    let project = TestProject::new();
    project.with_local_package("good_pkg", "name: good_pkg\nprofile: default\n");
    project.try_deps().await.expect("dbt deps should succeed");
    let good_pkg = project.root.join("dbt_packages/good_pkg/dbt_project.yml");
    assert!(good_pkg.is_file());

    project.with_git_package(
        "bad_pkg",
        &format!("name: \"{bad_name}\"\nprofile: default\n"),
    );
    let err = project
        .try_deps()
        .await
        .expect_err("dbt deps should reject the package name");

    assert!(
        err.to_string().contains("is not a valid directory name"),
        "unexpected error: {err}"
    );
    assert!(project.root.join("dbt_project.yml").is_file());
    assert!(good_pkg.is_file());
}

#[tokio::test]
async fn a_parent_dir_package_name_fails_deps_and_deletes_nothing() {
    assert_name_is_rejected("..").await;
}

#[tokio::test]
async fn an_empty_package_name_fails_deps_and_deletes_nothing() {
    assert_name_is_rejected("").await;
}

#[tokio::test]
async fn a_current_dir_package_name_fails_deps_and_deletes_nothing() {
    assert_name_is_rejected(".").await;
}

#[tokio::test]
async fn a_nested_package_name_fails_deps_and_deletes_nothing() {
    assert_name_is_rejected("nested/pkg").await;
}

/// A `local:` package is staged as a symlink at `<scratch>/<name>` before
/// `install_packages` checks the name, so an absolute name must be refused
/// when it is staged, or the symlink lands outside the project.
#[tokio::test]
async fn an_absolute_local_package_name_writes_nothing_outside() {
    let project = TestProject::new();
    fs::create_dir_all(project.outside()).unwrap();
    let escape = project.outside().join("pwned");
    project.with_local_package(
        "pkg",
        &format!("name: \"{}\"\nprofile: default\n", escape.display()),
    );

    let err = project.try_deps().await.expect_err("deps must fail");

    assert!(
        err.to_string().contains("Invalid package name"),
        "unexpected error: {err}"
    );
    assert!(
        escape.symlink_metadata().is_err(),
        "package landed outside the project"
    );
}

#[tokio::test]
async fn a_parent_traversing_local_package_name_writes_nothing_outside() {
    let project = TestProject::new();
    project.with_local_package("pkg", "name: \"../../outside/pwned\"\nprofile: default\n");

    let err = project.try_deps().await.expect_err("deps must fail");

    assert!(err.to_string().contains("Invalid package name"), "{err}");
    assert!(!project.outside().exists());
}

/// `dbt deps` removes every entry in the install directory that the lock does
/// not list, so an install path outside the project must be refused before
/// anything is created or deleted there.
#[tokio::test]
async fn an_install_path_outside_the_project_is_refused_before_deletion() {
    let project = TestProject::new();
    project.with_local_package("pkg", "name: honest_pkg\nprofile: default\n");
    let outside = project.outside();
    fs::create_dir_all(outside.join("important")).unwrap();
    let sentinel = outside.join("important").join("keep.txt");
    fs::write(&sentinel, "keep me").unwrap();

    for escape in [outside.clone(), project.root.join("../outside")] {
        let err = project
            .try_deps_into(&escape)
            .await
            .expect_err("deps must refuse an install path outside the project");
        assert!(err.to_string().contains("not inside the project"), "{err}");
        assert_eq!(fs::read_to_string(&sentinel).unwrap(), "keep me");
    }
}

/// A sentinel directory with one file, for asserting a delete never reached it.
fn sentinel_at(dir: &Path) -> PathBuf {
    fs::create_dir_all(dir).unwrap();
    let sentinel = dir.join("keep.txt");
    fs::write(&sentinel, "keep me").unwrap();
    sentinel
}

/// The kernel applies `..` after following a symlink: with `link -> outside/a`, `link/../victim` is `outside/victim`, although it collapses lexically to `project/victim`.
#[cfg(unix)]
#[tokio::test]
async fn an_install_path_through_a_symlink_and_parent_is_refused_before_deletion() {
    let project = TestProject::new();
    project.with_local_package("pkg", "name: honest_pkg\nprofile: default\n");
    let outside = project.outside();
    fs::create_dir_all(outside.join("a")).unwrap();
    std::os::unix::fs::symlink(outside.join("a"), project.root.join("link")).unwrap();
    let sentinel = sentinel_at(&outside.join("victim"));

    let err = project
        .try_deps_into(&project.root.join("link/../victim"))
        .await
        .expect_err("deps must refuse link/../victim");
    assert!(err.to_string().contains("not inside the project"), "{err}");
    assert_eq!(fs::read_to_string(&sentinel).unwrap(), "keep me");
}

/// `dbt deps` never removes the install directory itself, it creates it, stages
/// into it and deletes the entries the lock does not list. A `dbt_packages`
/// that is a symlink out of the project would have that happen in its target.
#[cfg(unix)]
#[tokio::test]
async fn an_install_path_that_is_a_symlink_out_is_refused_before_deletion() {
    let project = TestProject::new();
    project.with_local_package("pkg", "name: honest_pkg\nprofile: default\n");
    let outside = project.outside();
    let sentinel = sentinel_at(&outside);
    std::os::unix::fs::symlink(&outside, project.root.join("dbt_packages")).unwrap();

    let err = project
        .try_deps()
        .await
        .expect_err("deps must refuse a dbt_packages symlink out of the project");
    assert!(err.to_string().contains("not inside the project"), "{err}");
    assert_eq!(fs::read_to_string(&sentinel).unwrap(), "keep me");
    assert!(!outside.join("honest_pkg").exists());
}

/// Off Windows `\` is a filename byte, so `x\y/../../victim` climbs out of a directory named `x\y` to a sibling of the project, while dbt's path normalisation reads it as `x/y/../../victim`, inside it.
#[cfg(unix)]
#[tokio::test]
async fn an_install_path_through_a_backslash_name_is_refused_before_deletion() {
    let project = TestProject::new();
    project.with_local_package("pkg", "name: honest_pkg\nprofile: default\n");
    fs::create_dir_all(project.root.join("x\\y")).unwrap();
    let sentinel = sentinel_at(&project.root.parent().unwrap().join("victim"));

    let err = project
        .try_deps_into(&project.root.join("x\\y/../../victim"))
        .await
        .expect_err("deps must refuse x\\y/../../victim");
    assert!(err.to_string().contains("not inside the project"), "{err}");
    assert_eq!(fs::read_to_string(&sentinel).unwrap(), "keep me");
}

/// The advisory's zero-click branch: with no `package-lock.yml`, a command other than `dbt deps` auto-installs missing packages through the same delete.
/// It installs normally into an honest path, and refuses the escapes.
#[cfg(unix)]
#[tokio::test]
async fn the_no_lock_auto_install_refuses_an_escaping_install_path() {
    let project = TestProject::new();
    project.with_local_package("pkg", "name: honest_pkg\nprofile: default\n");
    assert!(!project.root.join("package-lock.yml").exists());

    project
        .try_auto_install_into(&project.root.join("dbt_packages"))
        .await
        .expect("auto-install into dbt_packages should succeed");
    assert!(project.root.join("dbt_packages/honest_pkg").exists());

    // Back to no lock, so the escapes also reach the auto-install branch.
    fs::remove_file(project.root.join("package-lock.yml")).unwrap();
    let outside = project.outside();
    fs::create_dir_all(outside.join("a")).unwrap();
    std::os::unix::fs::symlink(outside.join("a"), project.root.join("link")).unwrap();
    fs::create_dir_all(project.root.join("x\\y")).unwrap();
    let through_link = sentinel_at(&outside.join("victim"));
    let through_backslash = sentinel_at(&project.root.parent().unwrap().join("victim"));

    for escape in ["link/../victim", "x\\y/../../victim"] {
        let err = project
            .try_auto_install_into(&project.root.join(escape))
            .await
            .expect_err("auto-install must refuse an escaping install path");
        assert!(
            err.to_string().contains("not inside the project"),
            "{escape}: {err}"
        );
    }
    assert_eq!(fs::read_to_string(&through_link).unwrap(), "keep me");
    assert_eq!(fs::read_to_string(&through_backslash).unwrap(), "keep me");
}
