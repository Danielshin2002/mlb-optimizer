"""MLB Toolbox — team-level utility helpers."""

from utils.constants import CBT_TIERS


def cbt_info(budget_m: float) -> tuple[str, str, str, float | None, str]:
    """Return (label, bg, fg, next_threshold, apron_note) for a given budget."""
    for i, (thresh, label, bg, fg, note) in enumerate(CBT_TIERS):
        if budget_m < thresh:
            nxt = thresh if i > 0 else None
            return label, bg, fg, nxt, note
    t = CBT_TIERS[-1]
    return t[1], t[2], t[3], None, t[4]


def ordinal(n: int) -> str:
    """Return ordinal string: 1st, 2nd, 3rd, 4th, etc."""
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd'][min(n % 10, 4) if n % 10 < 4 else 0]}"
