"""MLB Toolbox — player name normalisation & headshot helpers."""

import unicodedata

import pandas as pd


def fix_player_name(s: str) -> str:
    """Normalise a player name: undo double-encoded UTF-8, then strip diacritics.

    Handles mojibake like "JosÃ©" → "Jose" and clean accents like "Pérez" → "Perez".
    Plain ASCII names pass through unchanged.
    """
    if not isinstance(s, str):
        return s
    # Step 1: undo double-encoded UTF-8 (latin-1 round-trip)
    try:
        s = s.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    # Step 2: strip combining diacritical marks → ASCII equivalents
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def fix_player_col(df: pd.DataFrame) -> pd.DataFrame:
    """If a 'Player' column exists, normalise every name in place."""
    if "Player" in df.columns:
        df["Player"] = df["Player"].map(fix_player_name)
    return df


def headshot_url(mlbam_id: str, width: int = 56) -> str:
    """Return the MLB static headshot URL for a given MLBAM ID."""
    return (
        "https://img.mlbstatic.com/mlb-photos/image/upload/"
        f"d_people:generic:headshot:67:current.png/w_{width},q_auto:best"
        f"/v1/people/{mlbam_id}/headshot/67/current"
    )


def hover_img_tag(player_name: str, mlbam_map: dict[str, str]) -> str:
    """Return an <img> tag for the player's headshot, or empty string."""
    mid = mlbam_map.get(player_name, "")
    if not mid:
        return ""
    url = headshot_url(mid, width=56)
    return (
        f"<img src='{url}' width='56' height='56' "
        f"style='border-radius:50%;vertical-align:middle;margin-right:6px;'>"
    )
