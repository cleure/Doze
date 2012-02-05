
"""
SELECT
    n.nspname as schema,
    c2.relname as table,
    c.relname as name,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'v' THEN 'view'
        WHEN 'i' THEN 'index'
        WHEN 'S' THEN 'sequence'
        WHEN 's' THEN 'special'
    END as type,
    i.indisunique AS is_unique,
    i.indisprimary AS is_primary,
    u.usename AS owner
    FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_index i ON (i.indexrelid = c.oid)
        JOIN pg_catalog.pg_class c2 ON (i.indrelid = c2.oid)
        LEFT JOIN pg_catalog.pg_user u ON (u.usesysid = c.relowner)
        LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
    WHERE c.relkind IN ('i','')
        AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
        AND pg_catalog.pg_table_is_visible(c.oid);

"""


"""

    pg_index
        indexrelid
        indrelid
    pg_attribute
        attrelid
    
    table
        column --\____
        index  --/

        SELECT
        tb.tablename AS table,
        tb.schemaname AS schema,
        a.attname AS name,
        t.typname AS internaltype,
        CASE
            WHEN SUBSTRING(TRIM(t.typname) FROM 1 FOR 3) = 'int' THEN 'integer'
            WHEN SUBSTRING(TRIM(t.typname) FROM 1 FOR 4) = 'float' THEN 'float'
            WHEN t.typname = 'varchar' THEN 'char'
            WHEN t.typname = 'text' THEN 'char'
            WHEN t.typname = 'bytea' THEN 'binary'
            ELSE t.typname
        END AS externaltype,
        CASE a.attlen
            WHEN -1 THEN
                CASE t.typname
                    WHEN 'numeric' THEN (a.atttypmod / 65536)
                    WHEN 'decimal' THEN (a.atttypmod / 65536)
                    WHEN 'money'   THEN (a.atttypmod / 65536)
                    ELSE CASE a.atttypmod
                        WHEN -1 THEN NULL
                        ELSE a.atttypmod - 4
                        END
                    END
                ELSE a.attlen
                END AS length,
                CASE t.typname
                    WHEN 'numeric' THEN (a.atttypmod % 65536) - 4
                    WHEN 'decimal' THEN (a.atttypmod % 65536) - 4
                    WHEN 'money'   THEN (a.atttypmod % 65536) - 4
                    ELSE 0
                END AS scale,
        a.attnotnull AS not_null,
        a.atttypmod AS type_mod,
        a.atthasdef AS has_default,
        (SELECT substring(pg_get_expr(d.adbin, d.adrelid) for 128)
            FROM pg_attrdef d
            WHERE d.adrelid = a.attrelid
                AND d.adnum = a.attnum
                AND a.atthasdef
        ) as default
        FROM pg_catalog.pg_attribute a
            RIGHT JOIN pg_catalog.pg_class c ON (c.oid = a.attrelid)
            RIGHT JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
            JOIN pg_catalog.pg_tables tb ON (tb.schemaname = 'public' AND tb.tablename = 'gallery')
        WHERE c.relname = 'gallery'
            AND NOT a.attisdropped
            AND a.attnum > 0
        ORDER BY a.attnum;

76992 |      77491


TODO: Autoload columns AND indexes, at the same time,
    when columns are accessed, as they depend on each other.

SELECT
    ix.indexrelid,
    ix.indrelid,
    ns.nspname AS schema,
    t.relname AS table,
    i.relname AS index,
    ix.indisprimary AS is_primary,
    ix.indisunique AS is_unique,
    a.attname AS column,
    r.rolname AS owner
    FROM
        pg_catalog.pg_class t,
        pg_catalog.pg_class i,
        pg_catalog.pg_index ix,
        pg_catalog.pg_attribute a,
        pg_catalog.pg_namespace ns,
        pg_catalog.pg_roles r
    WHERE
        t.oid = ix.indrelid
        AND r.oid = i.relowner
        AND i.oid = ix.indexrelid
        AND a.attrelid = t.oid
        AND a.attnum = ANY(ix.indkey)
        AND t.relkind = 'r'
        AND t.relname = 'clubhouse_members'
        AND ns.oid = t.relnamespace
    ORDER BY t.relname, i.relname;

# Sequences
SELECT
    s.relname AS name,
    ns.nspname AS schema,
    a.rolname AS owner
    FROM
        pg_catalog.pg_class s,
        pg_catalog.pg_namespace ns,
        pg_catalog.pg_roles a
    WHERE
        ns.oid = s.relnamespace
        AND a.oid = s.relowner
        AND s.relkind = 'S';

SELECT pg_catalog.pg_get_viewdef

# Views
SELECT
    s.relname AS name,
    ns.nspname AS schema,
    a.rolname AS owner,
    pg_catalog.pg_get_viewdef(s.oid, true) AS definition
    FROM
        pg_catalog.pg_class s,
        pg_catalog.pg_namespace ns,
        pg_catalog.pg_roles a
    WHERE
        ns.oid = s.relnamespace
        AND a.oid = s.relowner
        AND s.relkind = 'v';

# Schemas
SELECT
    n.nspname AS name,
    r.rolname AS owner
    FROM
        pg_catalog.pg_namespace n,
        pg_catalog.pg_roles r
    WHERE
        r.oid = n.nspowner;


"""





