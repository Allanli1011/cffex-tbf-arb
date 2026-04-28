"""CTD switch probability via Monte Carlo on a parallel yield shift.

Model
-----

Hold the futures price at its **current** value implied by the current
CTD: ``F = bond_clean[ctd] / CF[ctd]``. Under a parallel yield shift
``Δy`` (in decimal, e.g. 0.0050 = 50 bp), each deliverable's clean
price moves by its modified-duration approximation::

    new_clean[i] = old_clean[i] * (1 - mod_dur[i] * Δy)

The futures fair value follows the (still) current CTD::

    new_F = new_clean[ctd] / CF[ctd]

The new gross basis of bond ``i`` is then::

    new_gross_basis[i] = new_clean[i] - new_F * CF[i]

The new CTD is ``argmin(new_gross_basis)``: the bond cheapest to
deliver after the shift. This linear model:

* honours CF mechanics (the 3% notional rate baked into the CFFEX CF
  formula is what lets bonds with very different durations all map to
  a common futures price)
* preserves the property that ``d(gross_basis[ctd])/dΔy = 0`` at the
  current operating point — so we only flip CTD when *another* bond's
  gross basis crosses zero
* costs O(K) per simulation (K = deliverables, typically < 15) so MC
  with 1000 paths is essentially free at the 144-day-history scale

Limitations
-----------

* Parallel shift only — does not capture CTD switches driven by curve
  twist. A second mode (slope shock) is exposed in
  :func:`scenario_table` for transparency.
* Modified duration is a first-order approximation; convexity is
  ignored. For shifts < 100 bp this is well within the noise floor of
  the par-curve interpolation we already use.
* Holds futures fixed to the current CTD. In reality the futures
  price would partially anticipate the CTD switch; the resulting bias
  is conservative (we *underestimate* switch probability slightly).
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Deliverable:
    bond_code: str
    clean: float          # current clean price per 100 face
    mod_dur: float        # modified duration in years
    cf: float


@dataclass(frozen=True)
class CTDSwitchResult:
    current_ctd: str
    switch_probability: float
    horizon_vol_bp: float
    days_to_delivery: int
    n_sims: int
    bond_distribution: dict[str, float]      # bond_code -> probability
    top_alternative: tuple[str, float] | None  # (bond_code, prob), excluding current


def _new_ctd_index(
    deliverables: list[Deliverable],
    current_ctd_idx: int,
    shift: float,
) -> int:
    """Return index of the cheapest-to-deliver bond after a parallel
    yield shift ``shift`` (in decimal)."""
    new_cleans = [d.clean * (1.0 - d.mod_dur * shift) for d in deliverables]
    ctd = deliverables[current_ctd_idx]
    new_f = new_cleans[current_ctd_idx] / ctd.cf
    best_idx = 0
    best_basis = float("inf")
    for i, d in enumerate(deliverables):
        basis = new_cleans[i] - new_f * d.cf
        if basis < best_basis:
            best_basis = basis
            best_idx = i
    return best_idx


def estimate_ctd_switch_probability(
    deliverables: list[Deliverable],
    current_ctd_bond_code: str,
    *,
    days_to_delivery: int,
    daily_vol_bp: float = 5.0,
    n_sims: int = 1000,
    rng_seed: int = 42,
) -> CTDSwitchResult:
    """Monte-Carlo estimate of the probability that the CTD changes
    between ``valuation_date`` (now) and ``delivery_date``.

    ``daily_vol_bp`` is the annual-yield 1-day standard deviation in
    basis points; the horizon vol is scaled by sqrt(days). 5 bp/day
    is roughly the realised vol of CGB 10Y yield in calm regimes and
    8–10 bp/day in turbulent ones.
    """
    if days_to_delivery <= 0:
        raise ValueError("days_to_delivery must be positive")
    if not deliverables:
        raise ValueError("deliverables is empty")

    try:
        current_idx = next(
            i for i, d in enumerate(deliverables)
            if d.bond_code == current_ctd_bond_code
        )
    except StopIteration as exc:
        raise ValueError(
            f"Current CTD {current_ctd_bond_code!r} not in deliverables"
        ) from exc

    horizon_vol = (daily_vol_bp / 10_000.0) * math.sqrt(days_to_delivery)
    rng = random.Random(rng_seed)

    flips = 0
    counts: dict[str, int] = {}
    for _ in range(n_sims):
        shift = rng.gauss(0.0, horizon_vol)
        new_idx = _new_ctd_index(deliverables, current_idx, shift)
        code = deliverables[new_idx].bond_code
        counts[code] = counts.get(code, 0) + 1
        if new_idx != current_idx:
            flips += 1

    distribution = {k: v / n_sims for k, v in counts.items()}
    alts = sorted(
        ((k, p) for k, p in distribution.items()
         if k != current_ctd_bond_code),
        key=lambda kv: kv[1],
        reverse=True,
    )
    top_alt = alts[0] if alts else None

    return CTDSwitchResult(
        current_ctd=current_ctd_bond_code,
        switch_probability=flips / n_sims,
        horizon_vol_bp=horizon_vol * 10_000.0,
        days_to_delivery=days_to_delivery,
        n_sims=n_sims,
        bond_distribution=distribution,
        top_alternative=top_alt,
    )


def scenario_table(
    deliverables: list[Deliverable],
    current_ctd_bond_code: str,
    *,
    shifts_bp: tuple[int, ...] = (-100, -50, -25, 0, 25, 50, 100),
) -> list[dict]:
    """Deterministic scenario table: which bond becomes CTD under
    each parallel-shift size. Useful for the dashboard "what-if"
    panel. Returns one dict per shift with ``shift_bp`` and
    ``ctd_bond_code``.
    """
    try:
        current_idx = next(
            i for i, d in enumerate(deliverables)
            if d.bond_code == current_ctd_bond_code
        )
    except StopIteration as exc:
        raise ValueError(
            f"Current CTD {current_ctd_bond_code!r} not in deliverables"
        ) from exc

    out: list[dict] = []
    for bp in shifts_bp:
        idx = _new_ctd_index(deliverables, current_idx, bp / 10_000.0)
        out.append({
            "shift_bp": bp,
            "ctd_bond_code": deliverables[idx].bond_code,
            "switched": deliverables[idx].bond_code != current_ctd_bond_code,
        })
    return out
