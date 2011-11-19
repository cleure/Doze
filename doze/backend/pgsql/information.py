
"""
*
* Proposed extension, for information_schema, pg_catalog, etc.
*
"""

# Get Tables:
#   SELECT * FROM information_schema.tables
#   SELECT * FROM pg_catalog.pg_tables

# Get Indexes
# See: http://archives.postgresql.org/pgsql-php/2005-09/msg00011.php

def get_tables(db = None, schema = None):
    if db is None:
        return None
    
    if schema is None:
        schema = ['public']
