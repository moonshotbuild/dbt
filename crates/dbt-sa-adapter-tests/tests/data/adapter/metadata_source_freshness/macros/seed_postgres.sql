{# The test's first command: creates the tables the rest of the test reads. #}
{% macro seed_postgres() %}
  {% do run_query("create schema if not exists " ~ target.schema) %}
  {% do run_query("create table " ~ target.schema ~ ".a (val int)") %}
  {% do run_query("create table " ~ target.schema ~ ".source (id varchar(255), name varchar(255), amount numeric(10,2))") %}
{% endmacro %}
