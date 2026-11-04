use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};

use dbt_common::tracing::dbt_emit::emit_info_log_message;
use dbt_common::tracing::span_info::find_and_update_span_attrs;
use dbt_common::{ErrorCode, FsResult, constants::DBT_PACKAGES_LOCK_FILE, fs_err, stdfs};
use dbt_schemas::schemas::packages::{DbtPackageLock, DbtPackagesLock};
use dbt_telemetry::DepsAllPackagesInstalled;
use dbt_yaml::Verbatim;

use crate::context::DepsOperationContext;
use crate::package_installer::StagedPackage;
use crate::package_listing::{PackageListing, UnpinnedPackage};
use crate::utils::{max_resolve_concurrency, scrub_package_name_secret_env_vars};

/// Refuse an install directory that is not inside the project, and return the path to use for it.
///
/// `install_packages` stages each package inside the directory and then removes every entry in it that the lock does not list, so this is the last check between a `packages-install-path` of `../OUTSIDE` (or an absolute path) and the deletion of what is there.
/// The loader validates the configured value first, and this guards the sink for every caller.
///
/// The check reasons about the path the filesystem will act on, not a normalised spelling of it:
///
/// - The part below the project may not contain `..`. The kernel applies `..` after following symlinks, so `link/../victim` with `link -> /outside/a` names `/outside/victim` even though it collapses lexically to `victim`.
/// - It may not contain a `\`. Off Windows that is an ordinary filename byte, so `x\y/../../victim` climbs out of a directory named `x\y`, while dbt's path normalisation reads the `\` as a separator.
/// - The part that exists on disk is canonicalised and compared with the canonical project root, so a symlink inside the project (`project/link -> /outside`, install path `link/pkgs`) cannot route the writes and deletes outside it either. That includes a leaf that is itself a symlink: `create_dir_all` follows it, and the install then works inside its target.
///
/// The returned path is the checked canonical target rebased onto `in_dir`, which resolves to the same directory.
/// Callers must use it, not their input, for the delete and everything after it.
pub fn ensure_inside_project(install_path: &Path, in_dir: &Path) -> FsResult<PathBuf> {
    let refuse = |reason: &str| {
        fs_err!(
            ErrorCode::InvalidConfig,
            "Refusing to use packages install path '{}': {}",
            install_path.display(),
            reason
        )
    };

    // `std::path::absolute` keeps `..` (on POSIX) and does not rewrite `\`, unlike `DbtPath`, so the components below are the ones the kernel sees.
    let root = std::path::absolute(in_dir)?;
    let target = std::path::absolute(install_path)?;
    let Ok(relative) = target.strip_prefix(&root) else {
        return Err(refuse(&format!(
            "it is not inside the project directory {}",
            in_dir.display()
        )));
    };
    check_relative_install_path(relative).map_err(|reason| {
        refuse(&format!(
            "it is not inside the project directory {}: {reason}",
            in_dir.display()
        ))
    })?;

    let canonical_root = std::fs::canonicalize(in_dir).map_err(|e| {
        refuse(&format!(
            "cannot resolve the project directory {}: {}",
            in_dir.display(),
            e
        ))
    })?;
    let canonical = canonical_existing_prefix(&target).map_err(|e| {
        refuse(&format!(
            "cannot resolve it to confirm it stays inside the project: {e}"
        ))
    })?;
    match canonical.strip_prefix(&canonical_root) {
        Ok(inside) if !inside.as_os_str().is_empty() => Ok(in_dir.join(inside)),
        _ => Err(refuse(&format!(
            "it resolves to {}, which is not inside the project directory {}",
            canonical.display(),
            canonical_root.display()
        ))),
    }
}

/// Refuse a project-relative install path the containment check cannot reason about: an absolute path, a `..` component, or a `\` in a component.
///
/// The loader runs this on the configured value so the refusal names it, and [`ensure_inside_project`] runs it again at the sink.
pub fn check_relative_install_path(relative: &Path) -> Result<(), String> {
    for component in relative.components() {
        match component {
            Component::Normal(name) => {
                if name.as_encoded_bytes().contains(&b'\\') {
                    return Err(
                        "it contains a backslash, which is a filename character here, not a separator"
                            .to_string(),
                    );
                }
            }
            Component::CurDir => {}
            Component::ParentDir => {
                return Err("it must not contain a '..' component".to_string());
            }
            Component::Prefix(_) | Component::RootDir => {
                return Err(
                    "it must be a path inside the project, not an absolute path".to_string()
                );
            }
        }
    }
    Ok(())
}

/// Canonicalise the part of an absolute path that exists, following every symlink in it including the leaf, and re-append the rest lexically.
///
/// Existence is tested without following symlinks, so a dangling symlink above the leaf fails to canonicalise rather than passing as a directory that does not exist yet.
fn canonical_existing_prefix(path: &Path) -> std::io::Result<PathBuf> {
    let mut existing = path;
    let mut remainder: Vec<&std::ffi::OsStr> = Vec::new();
    loop {
        if std::fs::symlink_metadata(existing).is_ok() {
            let mut canonical = std::fs::canonicalize(existing)?;
            for component in remainder.iter().rev() {
                canonical.push(component);
            }
            return Ok(canonical);
        }
        match (existing.parent(), existing.file_name()) {
            (Some(parent), Some(name)) => {
                remainder.push(name);
                existing = parent;
            }
            _ => return Err(std::io::Error::other("no existing ancestor")),
        }
    }
}

fn package_lock_needs_scrub(package: &DbtPackageLock) -> bool {
    match package {
        DbtPackageLock::Git(git_package_lock) => {
            scrub_package_name_secret_env_vars(git_package_lock.git.as_str()).is_some()
        }
        DbtPackageLock::Tarball(tarball_package_lock) => {
            scrub_package_name_secret_env_vars(tarball_package_lock.tarball.as_str()).is_some()
        }
        _ => false,
    }
}

fn scrub_package_lock_for_file(dbt_packages_lock: &mut DbtPackagesLock) {
    for package in dbt_packages_lock.packages.iter_mut() {
        match package {
            DbtPackageLock::Git(git_package_lock) => {
                if let Some(scrubbed) =
                    scrub_package_name_secret_env_vars(git_package_lock.git.as_str())
                {
                    git_package_lock.git = Verbatim::from(scrubbed.into_owned());
                }
            }
            DbtPackageLock::Tarball(tarball_package_lock) => {
                if let Some(scrubbed) =
                    scrub_package_name_secret_env_vars(tarball_package_lock.tarball.as_str())
                {
                    tarball_package_lock.tarball = Verbatim::from(scrubbed.into_owned());
                }
            }
            _ => {}
        }
    }
}

/// Installs each locked package into its own scratch dir under `packages_install_path`,
/// then moves each package out of its scratch dir to `<packages_install_path>/<name>`.
/// A failed download leaves the previous packages and `package-lock.yml` unchanged.
pub async fn install_packages(
    ctx: &DepsOperationContext<'_>,
    dbt_packages_lock: &DbtPackagesLock,
    packages_install_path: &Path,
) -> FsResult<()> {
    let package_lock_str = if dbt_packages_lock
        .packages
        .iter()
        .any(package_lock_needs_scrub)
    {
        let mut scrubbed = DbtPackagesLock {
            packages: dbt_packages_lock.packages.clone(),
            sha1_hash: dbt_packages_lock.sha1_hash.clone(),
        };
        scrub_package_lock_for_file(&mut scrubbed);
        dbt_yaml::to_string(&scrubbed).unwrap()
    } else {
        dbt_yaml::to_string(dbt_packages_lock).unwrap()
    };
    // Create and install into the path the check validated, not the one passed in, so the two cannot name different directories.
    let packages_install_path = &ensure_inside_project(packages_install_path, &ctx.io.in_dir)?;
    stdfs::create_dir_all(packages_install_path)?;

    let mut package_listing = PackageListing::new(ctx.io.clone(), ctx.vars.clone(), &ctx.notices)
        .with_skip_private_deps(ctx.skip_private_deps)
        .with_private_package_resolver(ctx.private_package_resolver.clone())
        .with_cloud_config(ctx.cloud_config.clone());
    package_listing
        .hydrate_dbt_packages_lock(dbt_packages_lock, ctx.jinja_env)
        .await?;

    find_and_update_span_attrs(|ev: &mut DepsAllPackagesInstalled| {
        ev.package_count = package_listing.packages.len() as u64
    });

    ctx.check_cancellation()?;

    let to_install: Vec<&UnpinnedPackage> = package_listing
        .packages
        .values()
        .filter(|pkg| {
            if ctx.skip_private_deps
                && let UnpinnedPackage::Private(p) = pkg
            {
                emit_info_log_message(format!(
                    "Skipping private package {} due to --skip-private-deps flag",
                    p.name.as_ref().unwrap_or(&p.private)
                ));
                false
            } else {
                true
            }
        })
        .collect();

    let staged = install_packages_concurrent(ctx, packages_install_path, &to_install).await?;

    // Check every name before any package moves, so a bad one changes nothing.
    for package in &staged {
        check_package_dir_name(&package.name)?;
    }

    let mut installed = HashSet::new();
    for package in staged {
        let target = packages_install_path.join(&package.name);
        remove_entry(&target)?;
        stdfs::rename(&package.path, &target)?;
        installed.insert(package.name);
    }

    // The new packages are in place, so the lock now describes them.
    let packages_lock_path = ctx.io.in_dir.join(DBT_PACKAGES_LOCK_FILE);
    std::fs::write(&packages_lock_path, &package_lock_str).map_err(|e| {
        fs_err!(
            ErrorCode::IoError,
            "Failed to write package-lock.yml file: {}",
            e,
        )
    })?;

    // Remove packages the lock no longer lists, and scratch dirs left by a killed install.
    for entry in stdfs::read_dir(packages_install_path)? {
        let entry = entry.map_err(|e| {
            fs_err!(
                ErrorCode::IoError,
                "Failed to read packages install dir: {}",
                e,
            )
        })?;
        if !installed.contains(entry.file_name().to_string_lossy().as_ref()) {
            remove_entry(&entry.path())?;
        }
    }
    Ok(())
}

/// A package is installed to `<packages_install_path>/<name>`, and `name` comes from the
/// package's own `dbt_project.yml`. It must be a single dir name: an empty name or `..`
/// would make `install_packages` delete the install path or the dir above it.
fn check_package_dir_name(name: &str) -> FsResult<()> {
    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(dir)), None) if dir == name => Ok(()),
        _ => Err(fs_err!(
            ErrorCode::InvalidConfig,
            "Package name '{}' is not a valid directory name",
            name,
        )),
    }
}

/// Removes a file, dir or symlink. `remove_dir_all` removes a symlink without following it.
fn remove_entry(path: &Path) -> FsResult<()> {
    match path.symlink_metadata() {
        Ok(metadata) if metadata.is_file() => stdfs::remove_file(path),
        Ok(_) => stdfs::remove_dir_all(path),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(fs_err!(
            ErrorCode::IoError,
            "Failed to read {}: {}",
            path.display(),
            e,
        )),
    }
}

async fn install_packages_concurrent(
    ctx: &DepsOperationContext<'_>,
    dest: &Path,
    packages: &[&UnpinnedPackage],
) -> FsResult<Vec<StagedPackage>> {
    let max_concurrency = max_resolve_concurrency();
    let mut staged = Vec::with_capacity(packages.len());
    for chunk in packages.chunks(max_concurrency) {
        ctx.check_cancellation()?;
        staged.extend(
            futures::future::try_join_all(chunk.iter().map(|pkg| pkg.install(ctx, dest))).await?,
        );
    }
    Ok(staged)
}

#[cfg(test)]
mod install_path_tests {
    use super::*;

    #[test]
    fn ensure_inside_project_rejects_an_escape() {
        let tmp = tempfile::tempdir().unwrap();
        let in_dir = tmp.path().join("proj");
        std::fs::create_dir_all(in_dir.join("vendor")).unwrap();
        let p = |rel: &str| in_dir.join(rel);
        assert!(ensure_inside_project(&p("dbt_packages"), &in_dir).is_ok());
        assert!(ensure_inside_project(&p("vendor/pkgs"), &in_dir).is_ok());
        assert!(ensure_inside_project(&p("new/deep/pkgs"), &in_dir).is_ok());
        assert!(ensure_inside_project(&p("../OUTSIDE"), &in_dir).is_err());
        assert!(ensure_inside_project(&p("a/../../OUTSIDE"), &in_dir).is_err());
        assert!(ensure_inside_project(Path::new("/tmp/OUTSIDE"), &in_dir).is_err());
        assert!(ensure_inside_project(&in_dir, &in_dir).is_err());
        assert!(ensure_inside_project(&tmp.path().join("proj2"), &in_dir).is_err());
    }

    /// A symlink inside the project must not route the install directory,
    /// whose unlisted entries `dbt deps` deletes, outside it.
    #[cfg(unix)]
    #[test]
    fn ensure_inside_project_resolves_symlinked_parents() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().unwrap();
        let in_dir = tmp.path().join("proj");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&in_dir).unwrap();
        std::fs::create_dir_all(outside.join("pkgs")).unwrap();
        symlink(&outside, in_dir.join("link")).unwrap();
        symlink(tmp.path(), in_dir.join("up")).unwrap();

        // Through a symlinked parent: lexically inside, really outside.
        assert!(ensure_inside_project(&in_dir.join("link/pkgs"), &in_dir).is_err());
        assert!(ensure_inside_project(&in_dir.join("link/new"), &in_dir).is_err());
        // The project root reached through a symlink alias is still the root.
        assert!(ensure_inside_project(&in_dir.join("up/proj"), &in_dir).is_err());
        // A symlink leaf is followed by `create_dir_all`, so one that points
        // outside is refused too.
        assert!(ensure_inside_project(&in_dir.join("link"), &in_dir).is_err());
        // A real directory inside is fine, and so is a symlink leaf to it.
        std::fs::create_dir_all(in_dir.join("real")).unwrap();
        assert!(ensure_inside_project(&in_dir.join("real/pkgs"), &in_dir).is_ok());
        symlink(in_dir.join("real"), in_dir.join("inner_link")).unwrap();
        assert!(ensure_inside_project(&in_dir.join("inner_link"), &in_dir).is_ok());
    }

    /// The kernel applies `..` after following a symlink, so `link/../victim` with `link -> outside/a` names `outside/victim`, although it collapses lexically to `victim` inside the project.
    #[cfg(unix)]
    #[test]
    fn ensure_inside_project_refuses_parent_after_a_symlink() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().unwrap();
        let in_dir = tmp.path().join("proj");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&in_dir).unwrap();
        std::fs::create_dir_all(outside.join("a")).unwrap();
        std::fs::create_dir_all(outside.join("victim")).unwrap();
        symlink(outside.join("a"), in_dir.join("link")).unwrap();

        let err = ensure_inside_project(&in_dir.join("link/../victim"), &in_dir).unwrap_err();
        assert!(err.to_string().contains("'..'"), "{err}");
    }

    /// Off Windows `\` is a filename byte, so `x\y/../../victim` climbs out of a directory named `x\y` and lands beside the project, while `DbtPath` reads it as `x/y/../../victim`, inside it.
    #[cfg(unix)]
    #[test]
    fn ensure_inside_project_refuses_a_backslash() {
        let tmp = tempfile::tempdir().unwrap();
        let in_dir = tmp.path().join("proj");
        std::fs::create_dir_all(in_dir.join("x\\y")).unwrap();
        std::fs::create_dir_all(tmp.path().join("victim")).unwrap();

        assert!(ensure_inside_project(&in_dir.join("x\\y/../../victim"), &in_dir).is_err());
        let err = ensure_inside_project(&in_dir.join("x\\y"), &in_dir).unwrap_err();
        assert!(err.to_string().contains("backslash"), "{err}");
    }

    /// The returned path is the validated one, spelt from `in_dir`, so a caller that deletes it deletes what was checked.
    /// A symlink leaf keeps its name.
    #[cfg(unix)]
    #[test]
    fn ensure_inside_project_returns_the_checked_path() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().unwrap();
        let in_dir = tmp.path().join("proj");
        std::fs::create_dir_all(in_dir.join("vendor")).unwrap();
        symlink(tmp.path(), in_dir.join("leaf")).unwrap();
        symlink(in_dir.join("vendor"), in_dir.join("inner_leaf")).unwrap();

        for rel in [
            "dbt_packages",
            "vendor/pkgs",
            "new/deep/pkgs",
            "./vendor/pkgs",
        ] {
            let checked = ensure_inside_project(&in_dir.join(rel), &in_dir).unwrap();
            assert_eq!(checked, in_dir.join(rel.trim_start_matches("./")), "{rel}");
        }
        // A symlink leaf is followed: to its target when that is inside, and
        // refused when it is not.
        let checked = ensure_inside_project(&in_dir.join("inner_leaf"), &in_dir).unwrap();
        assert_eq!(checked, in_dir.join("vendor"));
        assert!(ensure_inside_project(&in_dir.join("leaf"), &in_dir).is_err());
    }

    /// A dangling symlink above the leaf cannot be resolved, so it is refused rather than passing as a directory that does not exist yet.
    #[cfg(unix)]
    #[test]
    fn ensure_inside_project_refuses_a_dangling_symlink_parent() {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().unwrap();
        let in_dir = tmp.path().join("proj");
        std::fs::create_dir_all(&in_dir).unwrap();
        symlink(tmp.path().join("missing"), in_dir.join("dangling")).unwrap();

        assert!(ensure_inside_project(&in_dir.join("dangling/pkgs"), &in_dir).is_err());
    }
}
