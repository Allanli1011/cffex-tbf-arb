"""Yield-curve trades: DV01-neutral steepeners and 50/50-DV01 butterflies.

A 2-leg "steepener / flattener" trade goes long one tenor and short another
to express a view on the spread of their yields. Sized for $-DV01 neutrality
so a parallel curve shift cancels out::

    N_a * DV01_a = N_b * DV01_b
    => N_b = (DV01_a / DV01_b) * N_a

A 3-leg butterfly is sized so each wing contributes equal $-DV01 to half the
belly's DV01 (50/50 weighting). This isolates curvature from level and slope:

    N_short_wing * DV01_short + N_long_wing * DV01_long = N_belly * DV01_belly
    N_short_wing * DV01_short = N_long_wing * DV01_long = N_belly * DV01_belly / 2

The "fly spread" tracked is the curvature in yield space (bps)::

    fly_bp = 2 * y_belly - y_short_wing - y_long_wing

Negative fly = belly yield rich relative to wings (cheap belly futures);
positive fly = belly cheap.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SteepenerWeights:
    n_short: float          # contracts of short-tenor leg (anchor = 1)
    n_long: float           # contracts of long-tenor leg, DV01-neutral to short
    dv01_short: float
    dv01_long: float


@dataclass(frozen=True)
class ButterflyWeights:
    n_short_wing: float
    n_belly: float
    n_long_wing: float
    dv01_short_wing: float
    dv01_belly: float
    dv01_long_wing: float


def dv01_neutral_weights(
    dv01_short: float,
    dv01_long: float,
    *,
    n_short: float = 1.0,
) -> SteepenerWeights:
    """Return contracts of each leg for a $DV01-neutral 2-leg steepener
    (or flattener — sign convention is up to the caller).

    With ``n_short`` contracts on the short-tenor leg, the long-tenor leg
    needs ``n_short * dv01_short / dv01_long`` contracts so that a parallel
    yield shift produces zero P&L.
    """
    if dv01_short <= 0 or dv01_long <= 0:
        raise ValueError(
            f"DV01s must be positive; got short={dv01_short}, long={dv01_long}"
        )
    n_long = n_short * dv01_short / dv01_long
    return SteepenerWeights(
        n_short=n_short,
        n_long=n_long,
        dv01_short=dv01_short,
        dv01_long=dv01_long,
    )


def butterfly_weights(
    dv01_short_wing: float,
    dv01_belly: float,
    dv01_long_wing: float,
    *,
    n_belly: float = 1.0,
) -> ButterflyWeights:
    """Return contracts for a 50/50 DV01-weighted butterfly with the belly
    fixed at ``n_belly`` contracts.

    Each wing contributes ``n_belly * dv01_belly / 2`` of $-DV01, so that
    the wings together neutralise the belly's DV01.
    """
    if dv01_short_wing <= 0 or dv01_belly <= 0 or dv01_long_wing <= 0:
        raise ValueError(
            f"DV01s must be positive; got short={dv01_short_wing}, "
            f"belly={dv01_belly}, long={dv01_long_wing}"
        )
    target_per_wing = n_belly * dv01_belly / 2.0
    return ButterflyWeights(
        n_short_wing=target_per_wing / dv01_short_wing,
        n_belly=n_belly,
        n_long_wing=target_per_wing / dv01_long_wing,
        dv01_short_wing=dv01_short_wing,
        dv01_belly=dv01_belly,
        dv01_long_wing=dv01_long_wing,
    )


def fly_yield_bp(
    y_short_wing_pct: float,
    y_belly_pct: float,
    y_long_wing_pct: float,
) -> float:
    """Curvature in yield space, expressed in basis points.

    ``fly = 2 * y_belly - y_short - y_long``

    Inputs are yields in **percent** (e.g. 1.85 means 1.85%).
    Output is bps. Negative => belly rich vs wings.
    """
    return (2.0 * y_belly_pct - y_short_wing_pct - y_long_wing_pct) * 100.0


def steepener_bp(y_short_pct: float, y_long_pct: float) -> float:
    """Long-minus-short yield spread in basis points (curve slope).

    Inputs are yields in **percent**. Increasing => curve steepening.
    """
    return (y_long_pct - y_short_pct) * 100.0
