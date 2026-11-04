//! `dbt deps` must never install a package outside `dbt_packages`.
//!
//! A package's `dbt_project.yml` `name:` becomes its install directory, and
//! `Path::join` discards the base for an absolute name (or walks out of it for
//! `..`). These drive the real `get_or_install_packages` entry point against a
//! `local:` package and assert the escape is refused and nothing lands outside
//! the project.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dbt_common::cancellation::CancellationToken;
use dbt_common::io_args::{FsCommand, IoArgs};
use dbt_jinja_utils::phases::load::init::initialize_load_profile_jinja_environment;
use fs_deps::get_or_install_packages;
use fs_deps::private_package::LocalPrivatePackageResolver;
use tempfile::TempDir;

const ROOT_PROJECT: &str = "name: root\nversion: '1.0.0'\nconfig-version: 2\n";

struct TestProject {
    _tmp: TempDir,
    root: PathBuf,
}

impl TestProject {
    fn new() -> Self {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("dbt_project.yml"), ROOT_PROJECT).unwrap();
        Self { _tmp: tmp, root }
    }

    /// A sibling directory, outside the project, that must stay untouched.
    fn outside(&self) -> PathBuf {
        self.root.parent().unwrap().join("outside")
    }

    fn with_local_package(&self, dir: &str, declared_name: &str) {
        let package_root = self.root.parent().unwrap().join(dir);
        fs::create_dir_all(package_root.join("models")).unwrap();
        fs::write(package_root.join("models").join("m.sql"), "select 1").unwrap();
        fs::write(
            package_root.join("dbt_project.yml"),
            format!("name: \"{declared_name}\"\nversion: '1.0.0'\nconfig-version: 2\n"),
        )
        .unwrap();
        fs::write(
            self.root.join("packages.yml"),
            format!("packages:\n  - local: \"../{dir}\"\n"),
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
            None,
            false,
            false,
            Default::default(),
            false,
            false,
            None,
            &CancellationToken::never_cancels(),
            false,
            false,
            Arc::new(LocalPrivatePackageResolver),
            None,
            None,
        )
        .await
        .map(|_| ())
    }
}

#[tokio::test]
async fn an_honest_package_name_installs_under_dbt_packages() {
    let project = TestProject::new();
    project.with_local_package("pkg", "honest_pkg");

    project.try_deps().await.expect("deps should succeed");

    assert!(
        project
            .root
            .join("dbt_packages")
            .join("honest_pkg")
            .exists()
    );
}

#[tokio::test]
async fn an_absolute_package_name_is_refused() {
    let project = TestProject::new();
    let escape = project.outside().join("pwned");
    project.with_local_package("pkg", escape.to_str().unwrap());

    let err = project.try_deps().await.expect_err("deps must fail");

    assert!(
        err.to_string().contains("Invalid package name"),
        "unexpected error: {err}"
    );
    assert!(!escape.exists(), "package landed outside the project");
}

#[tokio::test]
async fn a_parent_traversing_package_name_is_refused() {
    let project = TestProject::new();
    project.with_local_package("pkg", "../../outside/pwned");

    let err = project.try_deps().await.expect_err("deps must fail");

    assert!(err.to_string().contains("Invalid package name"), "{err}");
    assert!(!project.outside().exists());
}

/// `dbt deps` removes the install directory before reinstalling, so
/// an install path outside the project must be refused before that deletion.
#[tokio::test]
async fn an_install_path_outside_the_project_is_refused_before_deletion() {
    let project = TestProject::new();
    project.with_local_package("pkg", "honest_pkg");
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
    project.with_local_package("pkg", "honest_pkg");
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

/// Off Windows `\` is a filename byte, so `x\y/../../victim` climbs out of a directory named `x\y` to a sibling of the project, while dbt's path normalisation reads it as `x/y/../../victim`, inside it.
#[cfg(unix)]
#[tokio::test]
async fn an_install_path_through_a_backslash_name_is_refused_before_deletion() {
    let project = TestProject::new();
    project.with_local_package("pkg", "honest_pkg");
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
    project.with_local_package("pkg", "honest_pkg");
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
