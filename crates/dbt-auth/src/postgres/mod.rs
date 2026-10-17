use crate::{AdapterConfig, Auth, AuthError, AuthWarningPrinter, auth_configure_pipeline};
use database::Builder as DatabaseBuilder;

use dbt_adbc::duration::parse_duration;
use dbt_adbc::{Backend, database};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use std::borrow::Cow;

/// Everything but the RFC 3986 unreserved characters. libpq percent-decodes
/// every component of a connection URI but does not treat `+` as a space, so
/// form encoding is wrong here.
const CONN_ARG_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

fn encode_conn_arg(value: &str) -> Cow<'_, str> {
    utf8_percent_encode(value, CONN_ARG_SET).into()
}

/// The `sslmode` values libpq accepts.
const SSL_MODES: [&str; 6] = [
    "disable",
    "allow",
    "prefer",
    "require",
    "verify-ca",
    "verify-full",
];

/// Profile fields passed through to libpq unchanged, as URI query parameters
/// of the same name. `connect_timeout`, `role` and `search_path` are converted
/// first.
///
/// `retries` is not applied here: it is retry policy rather than a connection
/// parameter.
const LIBPQ_PARAMS: [&str; 5] = [
    "sslmode",
    "sslcert",
    "sslkey",
    "sslrootcert",
    "keepalives_idle",
];

/// libpq's `connect_timeout` is whole seconds, but the engine also accepts a
/// duration string here (`dbt debug` sets `1s` for its connection test), so
/// convert, rounding a fraction of a second up.
fn connect_timeout_secs(value: &str) -> Result<u64, AuthError> {
    if let Ok(secs) = value.parse::<u64>() {
        return Ok(secs);
    }
    let duration = parse_duration(value).map_err(|_| {
        AuthError::config(format!(
            "connect_timeout must be whole seconds or a duration such as `10s`, got `{value}`"
        ))
    })?;
    Ok(duration.as_secs() + u64::from(duration.subsec_nanos() > 0))
}

#[derive(Debug)]
enum PostgresAuthIR<'a> {
    Database {
        user: &'a str,
        password: &'a str,
        host: &'a str,
        port: Cow<'a, str>,
        database: &'a str,
        /// libpq connection parameters, in URI query order.
        params: Vec<(&'static str, Cow<'a, str>)>,
    },
}

impl<'a> PostgresAuthIR<'a> {
    pub fn apply(
        self,
        mut builder: DatabaseBuilder,
        _warning_printer: &dyn AuthWarningPrinter,
    ) -> Result<DatabaseBuilder, AuthError> {
        match self {
            Self::Database {
                user,
                password,
                host,
                port,
                database,
                params,
            } => {
                let user = encode_conn_arg(user);
                let password = encode_conn_arg(password);
                let database = encode_conn_arg(database);
                let mut uri = format!("postgresql://{user}:{password}@{host}:{port}/{database}");
                for (i, (key, value)) in params.iter().enumerate() {
                    let sep = if i == 0 { '?' } else { '&' };
                    uri.push(sep);
                    uri.push_str(key);
                    uri.push('=');
                    uri.push_str(&encode_conn_arg(value));
                }
                builder.with_parse_uri(uri)?;
            }
        }

        Ok(builder)
    }
}

/// The role a profile's `role` names. dbt-postgres runs `set role {role}`
/// unquoted, so a bare name folds to lower case and a double-quoted one is
/// taken exactly.
fn role_name(role: &str) -> String {
    match role.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => role.to_lowercase(),
    }
}

/// libpq splits `options` on whitespace and takes a backslash as an escape,
/// so both are escaped inside a value. dbt-postgres escapes spaces the same
/// way for `search_path`.
fn escape_option(value: &str) -> String {
    value.replace('\\', "\\\\").replace(' ', "\\ ")
}

fn parse_auth<'a>(
    config: &'a AdapterConfig,
    _warning_printer: &dyn AuthWarningPrinter,
) -> Result<PostgresAuthIR<'a>, AuthError> {
    let mut params = Vec::new();
    for key in LIBPQ_PARAMS {
        // `keepalives_idle` is usually a YAML integer.
        if let Some(value) = config.get_string(key) {
            params.push((key, value));
        }
    }
    if let Some(value) = config.get_string("connect_timeout") {
        let secs = connect_timeout_secs(&value)?;
        params.push(("connect_timeout", secs.to_string().into()));
    }
    if let Some((_, mode)) = params.iter().find(|(key, _)| *key == "sslmode")
        && !SSL_MODES.contains(&mode.as_ref())
    {
        return Err(AuthError::config(format!(
            "sslmode must be one of {}, got `{mode}`",
            SSL_MODES.join(", ")
        )));
    }
    // Server settings for the session, as libpq `options`. Setting `role` at
    // startup has the effect of dbt-postgres's `set role` after connecting.
    let mut options = Vec::new();
    if let Some(role) = config.get_str("role") {
        options.push(format!("-c role={}", escape_option(&role_name(role))));
    }
    if let Some(search_path) = config.get_str("search_path") {
        options.push(format!("-c search_path={}", escape_option(search_path)));
    }
    if !options.is_empty() {
        params.push(("options", options.join(" ").into()));
    }

    Ok(PostgresAuthIR::Database {
        user: config.require_str("user")?,
        password: config.require_str("password")?,
        host: config.require_str("host")?,
        // In profiles.yml `port` is commonly an integer; accept both numeric and string values.
        port: config.require_string("port")?,
        database: config.require_str("database")?,
        params,
    })
}

// Every connection argument is part of the URI built in `PostgresAuthIR::apply`,
// because the ADBC Postgres driver hands that URI to libpq as is.
fn apply_connection_args(
    _config: &AdapterConfig,
    builder: DatabaseBuilder,
    _warning_printer: &dyn AuthWarningPrinter,
) -> Result<DatabaseBuilder, AuthError> {
    Ok(builder)
}

pub struct PostgresAuth {
    pub warning_printer: Box<dyn AuthWarningPrinter>,
}

impl PostgresAuth {
    pub fn new(warning_printer: Box<dyn AuthWarningPrinter>) -> Self {
        Self { warning_printer }
    }
}

impl Auth for PostgresAuth {
    fn backend(&self) -> Backend {
        Backend::Postgres
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        auth_configure_pipeline!(self, &config, parse_auth, apply_connection_args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_options::uri_value;
    use dbt_test_primitives::assert_contains;
    use dbt_yaml::Mapping;

    #[test]
    fn test_uri_contains_expected_parts() {
        let config = Mapping::from_iter([
            ("user".into(), "alice".into()),
            ("password".into(), "secret".into()),
            ("host".into(), "pg.local".into()),
            ("port".into(), "5432".into()),
            ("database".into(), "db".into()),
        ]);

        let builder = PostgresAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(config))
            .expect("configure");

        let uri = uri_value(&builder);
        assert_contains!(&uri, "postgresql://alice:secret@pg.local:5432/db");
        assert_contains!(&uri, "user=alice");
        assert_contains!(&uri, "password=secret");
    }

    #[test]
    fn test_port_accepts_integer_value() {
        let config: Mapping = dbt_yaml::from_str(
            r#"
user: alice
password: secret
host: pg.local
port: 5432
database: db
"#,
        )
        .expect("parse yaml");

        let builder = PostgresAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(config))
            .expect("configure");

        let uri = uri_value(&builder);
        assert_contains!(&uri, "postgresql://alice:secret@pg.local:5432/db");
    }

    fn configure_yaml(yaml: &str) -> Result<database::Builder, AuthError> {
        let config: Mapping = dbt_yaml::from_str(yaml).expect("parse yaml");
        PostgresAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(config))
    }

    const BASE: &str = r#"
user: alice
password: secret
host: pg.local
port: 5432
database: db
"#;

    #[test]
    fn test_no_connection_args_adds_no_libpq_params() {
        let uri = uri_value(&configure_yaml(BASE).expect("configure"));
        assert_eq!(
            uri,
            "postgresql://alice:secret@pg.local:5432/db?user=alice&password=secret"
        );
    }

    #[test]
    fn test_connection_args_become_libpq_params() {
        let yaml = format!(
            "{BASE}sslmode: verify-full\nsslcert: /certs/client.crt\nsslkey: /certs/client.key\nsslrootcert: /certs/root.crt\nkeepalives_idle: 30\nconnect_timeout: 10\n"
        );
        let uri = uri_value(&configure_yaml(&yaml).expect("configure"));
        assert_contains!(
            &uri,
            "/db?sslmode=verify-full&sslcert=%2Fcerts%2Fclient.crt&sslkey=%2Fcerts%2Fclient.key&sslrootcert=%2Fcerts%2Froot.crt&keepalives_idle=30&connect_timeout=10&user=alice"
        );
    }

    #[test]
    fn test_search_path_becomes_options() {
        let yaml = format!("{BASE}search_path: \"analytics, my schema\"\n");
        let uri = uri_value(&configure_yaml(&yaml).expect("configure"));
        // `-c search_path=analytics,\ my\ schema`, percent-encoded.
        assert_contains!(
            &uri,
            "?options=-c%20search_path%3Danalytics%2C%5C%20my%5C%20schema&user=alice"
        );
    }

    #[test]
    fn test_role_becomes_options() {
        let yaml = format!("{BASE}role: Transformer\nsearch_path: analytics\n");
        let uri = uri_value(&configure_yaml(&yaml).expect("configure"));
        // `-c role=transformer -c search_path=analytics`: the bare name folds.
        assert_contains!(
            &uri,
            "?options=-c%20role%3Dtransformer%20-c%20search_path%3Danalytics&user=alice"
        );

        let yaml = format!("{BASE}role: '\"My Role\"'\n");
        let uri = uri_value(&configure_yaml(&yaml).expect("configure"));
        // A quoted name keeps its case, and its space is escaped.
        assert_contains!(&uri, "?options=-c%20role%3DMy%5C%20Role&user=alice");
    }

    #[test]
    fn test_reserved_characters_in_credentials_round_trip() {
        let yaml = r#"
user: "al ice@corp"
password: "p@ss/w:rd?#% +"
host: pg.local
port: 5432
database: "my db"
"#;
        let uri = uri_value(&configure_yaml(yaml).expect("configure"));
        assert_contains!(
            &uri,
            "postgresql://al%20ice%40corp:p%40ss%2Fw%3Ard%3F%23%25%20%2B@pg.local:5432/my%20db?"
        );
        assert_contains!(&uri, "&password=p%40ss%2Fw%3Ard%3F%23%25%20%2B");
    }

    #[test]
    fn test_connect_timeout_duration_becomes_whole_seconds() {
        for (value, secs) in [
            ("\"1s\"", "1"),
            ("\"2m\"", "120"),
            ("\"500ms\"", "1"),
            ("15", "15"),
        ] {
            let yaml = format!("{BASE}connect_timeout: {value}\n");
            let uri = uri_value(&configure_yaml(&yaml).expect("configure"));
            assert_contains!(&uri, &format!("?connect_timeout={secs}&user=alice"));
        }
        let yaml = format!("{BASE}connect_timeout: soon\n");
        let error = configure_yaml(&yaml).expect_err("invalid connect_timeout");
        assert_contains!(error.msg(), "connect_timeout must be whole seconds");
    }

    #[test]
    fn test_invalid_sslmode_is_rejected() {
        let yaml = format!("{BASE}sslmode: required\n");
        let error = configure_yaml(&yaml).expect_err("invalid sslmode");
        assert_contains!(error.msg(), "sslmode must be one of");
        assert_contains!(error.msg(), "`required`");
    }
}
