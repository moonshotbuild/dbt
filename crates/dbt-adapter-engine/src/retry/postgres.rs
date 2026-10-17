use dbt_auth::AdapterConfig;

/// Retries a Postgres connect that failed for a reason that can pass: the
/// server is unreachable, starting up or shutting down, or out of connection
/// slots.
///
/// The ADBC Postgres driver reports every connect failure as `[libpq] Failed
/// to connect: <libpq message>`, so this matches libpq's message text.
/// dbt-postgres retries any `psycopg2.OperationalError`, which includes a
/// wrong password or a missing role or database. Those are not retried here,
/// because another attempt cannot succeed. `retry_all: true` restores the
/// dbt-postgres behaviour.
/// https://github.com/dbt-labs/dbt-adapters/blob/802d40f0f0eb663c17e5be08d1e3fb2e55b9ee34/dbt-postgres/src/dbt/adapters/postgres/connections.py#L220-L238
pub(super) fn is_retryable_postgres_login_error(
    config: &AdapterConfig,
    err: &adbc_core::error::Error,
) -> bool {
    if config.get_bool("retry_all").unwrap_or(false) {
        return true;
    }

    const POSTGRES_TRANSIENT_PATTERNS: &[&str] = &[
        // The socket: nothing listening yet, a dropped route, a reset.
        "connection refused",
        "no route to host",
        "network is unreachable",
        "connection reset by peer",
        "server closed the connection unexpectedly",
        // libpq's own connect_timeout.
        "timeout expired",
        // DNS that may answer on a second try (EAI_AGAIN).
        "temporary failure in name resolution",
        // SQLSTATE 57P03 cannot_connect_now: starting up, shutting down,
        // recovering, or not yet accepting connections.
        "the database system is",
        // SQLSTATE 53300 too_many_connections.
        "too many clients already",
        "remaining connection slots are reserved",
    ];
    let msg = err.message.to_lowercase();
    POSTGRES_TRANSIENT_PATTERNS.iter().any(|p| msg.contains(p))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::tests::adbc_err;
    use crate::retry::tests::cfg_default;
    use crate::retry::tests::cfg_retry_all;
    use adbc_core::error::Status;

    #[test]
    fn postgres_retries_transient_libpq_connect_failures() {
        for msg in [
            "[libpq] Failed to connect: connection to server at \"127.0.0.1\", port 5432 failed: Connection refused\n\tIs the server running on that host and accepting TCP/IP connections?",
            "[libpq] Failed to connect: connection to server at \"db\", port 5432 failed: timeout expired",
            "[libpq] Failed to connect: could not translate host name \"db\" to address: Temporary failure in name resolution",
            "[libpq] Failed to connect: connection to server at \"db\", port 5432 failed: FATAL:  the database system is starting up",
            "[libpq] Failed to connect: connection to server at \"db\", port 5432 failed: FATAL:  sorry, too many clients already",
            "[libpq] Failed to connect: connection to server at \"db\", port 5432 failed: server closed the connection unexpectedly",
        ] {
            let e = adbc_err(Status::IO, msg);
            assert!(
                is_retryable_postgres_login_error(&cfg_default(), &e),
                "expected retry for msg {msg:?}"
            );
        }
    }

    #[test]
    fn postgres_does_not_retry_permanent_connect_failures() {
        for msg in [
            "[libpq] Failed to connect: connection to server at \"db\", port 5432 failed: FATAL:  password authentication failed for user \"alice\"",
            "[libpq] Failed to connect: connection to server at \"db\", port 5432 failed: FATAL:  role \"x\" does not exist",
            "[libpq] Failed to connect: connection to server at \"db\", port 5432 failed: FATAL:  database \"x\" does not exist",
            "[libpq] Failed to connect: connection to server at \"db\", port 5432 failed: server does not support SSL, but SSL was required",
            "[libpq] Failed to connect: could not translate host name \"nope\" to address: Name or service not known",
        ] {
            let e = adbc_err(Status::IO, msg);
            assert!(
                !is_retryable_postgres_login_error(&cfg_default(), &e),
                "expected no retry for msg {msg:?}"
            );
        }
        let e = adbc_err(Status::IO, "FATAL:  password authentication failed");
        assert!(is_retryable_postgres_login_error(&cfg_retry_all(), &e));
    }
}
