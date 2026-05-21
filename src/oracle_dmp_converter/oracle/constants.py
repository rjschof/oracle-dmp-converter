"""Oracle system constants shared across subpackages.

Keeping these here prevents DDL-parser modules from importing the full
``oracle.metadata`` module just to access a constant.
"""

from __future__ import annotations

# Schemas that Oracle ships with and maintains; any schema/table pair whose
# schema is in this set is excluded from dump discovery results.
ORACLE_MAINTAINED_SCHEMAS: frozenset[str] = frozenset(
    {
        "ANONYMOUS",
        "APPQOSSYS",
        "AUDSYS",
        "CTXSYS",
        "DBSFWUSER",
        "DBSNMP",
        "DIP",
        "DVF",
        "DVSYS",
        "GGSYS",
        "GSMADMIN_INTERNAL",
        "GSMCATUSER",
        "GSMUSER",
        "LBACSYS",
        "MDSYS",
        "OJVMSYS",
        "OLAPSYS",
        "ORDDATA",
        "ORDPLUGINS",
        "ORDSYS",
        "OUTLN",
        "REMOTE_SCHEDULER_AGENT",
        "SYS",
        "SYS$UMF",
        "SYSBACKUP",
        "SYSDG",
        "SYSKM",
        "SYSRAC",
        "SYSTEM",
        "WMSYS",
        "XDB",
        "XS$NULL",
    }
)
