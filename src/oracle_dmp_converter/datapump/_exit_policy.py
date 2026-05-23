"""Per-tool exit-code policy + classified Oracle error codes.

Modern Data Pump (``impdp``/``expdp``) treats any non-zero exit as fatal.
Legacy ``imp``/``exp`` uses a graded scheme: exit ``0`` is success, ``1`` is
fatal (``EX_FAIL``), and ``2`` is ``EX_OKWARN`` — the operation completed
but with non-fatal warnings.  Treating every non-zero exit as fatal causes
``imp`` runs that finish successfully with warnings (e.g. cross-schema FK
not exported) to be reported as failures.

This module groups that knowledge so the runner can:

* decide whether a returncode is fatal, warning, or success;
* scan the combined stdout/stderr for ORA codes that **promote** a
  warning-grade exit to fatal (e.g. ``ORA-39126`` "worker unexpected fatal
  error" or ``ORA-31693`` "table data load aborted").
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum


class OraCodeBehavior(Enum):
    """How callers should react to a specific ORA-NNNNN error code."""

    WARN = "warn"
    RETRY = "retry"
    FATAL = "fatal"
    SKIP = "skip"


# Centralised classification of ORA codes the converter has opinions about.
# Use ``KNOWN_ORA_CODES.get(code, OraCodeBehavior.FATAL)`` — anything missing
# is treated as fatal so we never silently swallow unfamiliar errors.
KNOWN_ORA_CODES: dict[int, OraCodeBehavior] = {
    # --- successful-but-noisy DDL (idempotent re-runs) ------------------
    942: OraCodeBehavior.SKIP,  # table or view does not exist
    1430: OraCodeBehavior.SKIP,  # column being added already exists
    1543: OraCodeBehavior.SKIP,  # tablespace already exists
    1918: OraCodeBehavior.SKIP,  # user does not exist
    1920: OraCodeBehavior.SKIP,  # user name already exists
    4043: OraCodeBehavior.SKIP,  # object does not exist (DROP TYPE)
    4080: OraCodeBehavior.SKIP,  # trigger does not exist
    28102: OraCodeBehavior.SKIP,  # VPD policy does not exist
    # --- recoverable: caller fixes up + retries the same DDL ------------
    959: OraCodeBehavior.RETRY,  # tablespace 'X' does not exist
    1950: OraCodeBehavior.RETRY,  # no privileges on tablespace
    # --- transient connection / resource problems -----------------------
    54: OraCodeBehavior.RETRY,  # row locked (NOWAIT)
    1000: OraCodeBehavior.RETRY,  # maximum open cursors exceeded
    12170: OraCodeBehavior.RETRY,  # TNS:Connect timeout occurred
    12514: OraCodeBehavior.RETRY,  # listener does not know of service
    12537: OraCodeBehavior.RETRY,  # TNS:connection closed
    12541: OraCodeBehavior.RETRY,  # TNS:no listener
    # --- import warnings that should not stop the run -------------------
    39082: OraCodeBehavior.WARN,  # object created with compilation warning
    39151: OraCodeBehavior.WARN,  # table exists (skip the object)
    # --- import errors that DO stop the run -----------------------------
    1017: OraCodeBehavior.FATAL,  # invalid username/password
    1031: OraCodeBehavior.FATAL,  # insufficient privileges
    28000: OraCodeBehavior.FATAL,  # the account is locked
    31693: OraCodeBehavior.FATAL,  # table data load aborted (worker died)
    39083: OraCodeBehavior.FATAL,  # object failed to create
    39126: OraCodeBehavior.FATAL,  # worker unexpected fatal error
}


# Convenience subsets the existing call sites use as ``ignored_codes={...}``.
SKIPPABLE_ORA_CODES = frozenset(c for c, b in KNOWN_ORA_CODES.items() if b is OraCodeBehavior.SKIP)
RETRYABLE_ORA_CODES = frozenset(c for c, b in KNOWN_ORA_CODES.items() if b is OraCodeBehavior.RETRY)


# ``ORA-NNNNN`` extractor used when scanning combined stdout/stderr.
_ORA_CODE_RE = re.compile(r"ORA-(\d{4,5})", re.IGNORECASE)


def scan_for_ora_codes(output: str) -> set[int]:
    """Return the set of ORA codes referenced anywhere in *output*."""
    return {int(m) for m in _ORA_CODE_RE.findall(output)}


def scan_for_fatal_codes(output: str) -> set[int]:
    """Return the subset of ORA codes in *output* classified as FATAL.

    Used to *promote* a warning-grade subprocess exit (e.g. legacy ``imp``
    returncode 2) into a hard failure when the output reveals a fatal
    code like ``ORA-39126`` even though the process itself exited
    cleanly-with-warnings.
    """
    return {
        c for c in scan_for_ora_codes(output) if KNOWN_ORA_CODES.get(c) is OraCodeBehavior.FATAL
    }


class ExitClassification(Enum):
    """Outcome of :meth:`ToolExitPolicy.classify`."""

    SUCCESS = "success"
    WARNING = "warning"
    FATAL = "fatal"


@dataclass(frozen=True)
class ToolExitPolicy:
    """How to interpret a tool's returncode + output.

    Attributes:
        tool: Short tool name (``"impdp"``, ``"imp"``); used in log messages.
        warning_returncodes: Returncodes that mean "finished with warnings".
            Default empty: any non-zero exit is fatal.
        fatal_returncodes: Returncodes that are explicitly fatal even if
            they appear in ``warning_returncodes`` of an ancestor policy.
            Default empty — used by subclasses if needed.
        promote_fatal_ora_codes: When the returncode would normally be a
            warning, scan the output for these ORA codes and promote to
            fatal if any are found.
    """

    tool: str
    warning_returncodes: frozenset[int] = field(default_factory=frozenset)
    fatal_returncodes: frozenset[int] = field(default_factory=frozenset)
    promote_fatal_ora_codes: frozenset[int] = field(
        default_factory=lambda: frozenset(
            c for c, b in KNOWN_ORA_CODES.items() if b is OraCodeBehavior.FATAL
        )
    )

    def classify(self, returncode: int, output: str) -> ExitClassification:
        """Decide whether ``returncode`` + ``output`` is success, warning, or fatal."""
        if returncode == 0:
            return ExitClassification.SUCCESS
        if returncode in self.fatal_returncodes:
            return ExitClassification.FATAL
        if returncode in self.warning_returncodes:
            seen_fatal = scan_for_ora_codes(output) & self.promote_fatal_ora_codes
            if seen_fatal:
                return ExitClassification.FATAL
            return ExitClassification.WARNING
        return ExitClassification.FATAL


# Pre-built policies for the tools the converter invokes.
STRICT_POLICY = ToolExitPolicy(tool="strict")
EXPDP_POLICY = ToolExitPolicy(tool="expdp")
# Data Pump impdp exit-code policy:
#   0  = EX_SUCC      — clean success
#   1  = EX_FAIL      — parameter / DDL / fatal error
#   2  = EX_SUCC_INFO — completed, with informational messages
#   5  = EX_SUCC_ERR  — completed, with non-fatal errors (e.g. ORA-39120
#                      "table can't be truncated" when an FK from another
#                      table blocks TABLE_EXISTS_ACTION=TRUNCATE).
# Treat both 2 and 5 as warning-grade so legitimate impdp completions with
# per-table issues don't abort the entire convert.  Output is still scanned
# for codes flagged FATAL in KNOWN_ORA_CODES (e.g. ORA-39126 worker death)
# to promote the warning to fatal when warranted.
IMPDP_POLICY = ToolExitPolicy(
    tool="impdp",
    warning_returncodes=frozenset({2, 5}),
)

# Legacy imp/exp use returncode 2 for ``EX_OKWARN`` (completed with warnings).
LEGACY_IMP_POLICY = ToolExitPolicy(
    tool="imp",
    warning_returncodes=frozenset({2}),
)
LEGACY_EXP_POLICY = ToolExitPolicy(
    tool="exp",
    warning_returncodes=frozenset({2}),
)
