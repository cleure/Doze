
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

# Databases
SELECT d.datname, r.rolname AS owner
    FROM
        pg_catalog.pg_database d,
        pg_catalog.pg_roles r
    WHERE r.oid = d.datdba;

# Constraints
# http://www.postgresql.org/docs/7.4/interactive/catalog-pg-constraint.html
SELECT
    c.conname AS name,
    t.relname AS table,
    c.consrc AS definition
    FROM
        pg_catalog.pg_constraint c,
        pg_catalog.pg_class t
    WHERE
        t.oid = c.conrelid
        AND t.relname = 'clubhouse_members';

# Check Constraints
SELECT
    n.nspname AS schema,
    c.conname AS name,
    t.relname AS table,
    c.consrc AS definition
    FROM
        pg_catalog.pg_constraint c,
        pg_catalog.pg_class t,
        pg_catalog.pg_namespace n
    WHERE
        t.oid = c.conrelid
        AND t.relnamespace = n.oid
        AND t.relname = 'clubhouse_members'
        AND c.contype = 'f';

# Foriegn Key Constraints
SELECT
    c.confupdtype,
    c.confdeltype,
    n1.nspname AS src_schema,
    n2.nspname AS dst_schema,
    c.conname AS name,
    t1.relname AS src_table,
    t2.relname as dst_table,
    c.condeferrable AS deferrable,
    c.confmatchtype AS match_type,
    c.conkey,
    c.confkey,
    c.conrelid AS src_table_oid,
    c.confrelid AS dst_table_oid
    FROM
        pg_catalog.pg_constraint c,
        pg_catalog.pg_class t1,
        pg_catalog.pg_class t2,
        pg_catalog.pg_namespace n1,
        pg_catalog.pg_namespace n2
    WHERE
        t1.oid = c.conrelid
        AND t1.relnamespace = n1.oid
        AND c.confrelid = t2.oid
        AND t2.relnamespace = n2.oid
        AND c.contype = 'f'
        AND t1.relname = 'clubhouse_prize_selection';



        SELECT
        a.attnum,
        a.attname AS name
        FROM pg_catalog.pg_attribute a
            RIGHT JOIN pg_catalog.pg_class c ON (c.oid = a.attrelid)
        WHERE c.oid = 77003
            AND NOT a.attisdropped
            AND a.attnum > 0
            AND a.attnum = 1
        ORDER BY a.attnum;

dst_table_oid = 77003

         34163 | bursts          | 00010001 | RI_FKey_check_upd
         34163 | bursts          | 00001001 | RI_FKey_noaction_del
         34163 | bursts          | 00010001 | RI_FKey_noaction_upd

CREATE TABLE test (
    id serial primary key,
    modified TIMESTAMP
);

CREATE OR REPLACE FUNCTION dummy_trigger_func () RETURNS TRIGGER
AS $$
DECLARE
    r RECORD;
BEGIN
    SELECT INTO r NOW() AS now;
        
    IF FOUND THEN
        NEW.modified = now;
        RETURN NEW;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--- 0 | test                      | 00010111 | dummy_trigger_func
CREATE TRIGGER dummy_trigger_func_tr BEFORE INSERT OR UPDATE ON test
FOR EACH ROW EXECUTE PROCEDURE dummy_trigger_func();

--- 0 | test                      | 00010101 | dummy_trigger_func
CREATE TRIGGER dummy_trigger_func_tr AFTER INSERT OR UPDATE ON test
FOR EACH ROW EXECUTE PROCEDURE dummy_trigger_func();

--- 0 | test                      | 00010100 | dummy_trigger_func
CREATE TRIGGER dummy_trigger_func_tr AFTER INSERT OR UPDATE ON test
EXECUTE PROCEDURE dummy_trigger_func();

INSERT_MASK = 00000100
DELETE_MASK = 00001000
UPDATE_MASK = 00010000

BEFORE_INS_UPD_MASK = 00000010
AFTER_INS_UPD_MASK = 00000000

FOR_EACH_ROW_MASK = 00000001
NOT_FOR_EACH_ROW_MASK = 00000000

if tgconstrrelid is 0, trigger is NOT a referential integrity trigger, otherwise, it is.

PG_TRIGGER_FOR_EACH_MASK    = 0b00000001 = 1
PG_TRIGGER_BEFORE_MASK      = 0b00000010 = 2
PG_TRIGGER_INS_MASK         = 0b00000100 = 4
PG_TRIGGER_DEL_MASK         = 0b00001000 = 8
PG_TRIGGER_UPD_MASK         = 0b00010000 = 16

# Non RI Triggers
SELECT
    n.nspname AS schema,
    t.relname AS table,
    tg.tgname AS name,
    (tg.tgtype::int)::bit(8) AS type,
    (tg.tgtype & 1)::bool AS for_each_row,
    (tg.tgtype & 2)::bool AS execute_before,
    (tg.tgtype & 4)::bool AS on_insert,
    (tg.tgtype & 8)::bool AS on_delete,
    (tg.tgtype & 16)::bool AS on_update,
    --p.prosrc AS source,
    p.proname AS function
    FROM
        pg_catalog.pg_trigger tg,
        pg_catalog.pg_class t,
        pg_catalog.pg_namespace n,
        pg_catalog.pg_proc p
    WHERE
        tg.tgrelid = t.oid
        AND tg.tgfoid = p.oid
        AND t.relnamespace = n.oid
        AND tg.tgisconstraint = 'f'
        AND n.nspname = 'public'
        AND t.relname = 'test';

# Sequences?

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

SELECT n.nspname AS schemaname, c.relname AS tablename, pg_get_userbyid(c.relowner) AS tableowner, t.spcname AS tablespace, c.relhasindex AS hasindexes, c.relhasrules AS hasrules, c.reltriggers > 0 AS hastriggers
   FROM pg_class c
   LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
   LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
  WHERE c.relkind = 'S'::"char";

"""





