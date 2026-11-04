"""
ai_anomaly.py — Statistical anomaly detection & predictive alerting
for the MLB ETL Monitor (ETLMonitor.py).

WHAT THIS ADDS ON TOP OF YOUR EXISTING check_out_of_range():
--------------------------------------------------------------
Your current pipeline_ranges table is a *static* rule: min/max bounds,
seeded once from a 30-day average ± 20% (or ±50%/200% for elapsed_seconds),
then left alone until someone re-seeds it. That's a fine first line of
defense, but it has two blind spots this module fills:

  1. SEASONALITY-BLIND STATIC RANGES
     MLB load volume genuinely varies by day of week (doubleheaders on
     weekends, off-days, All-Star break, postseason vs. offseason). A
     single ±20% band around a 30-day average either has to be loose
     enough to never fire on a heavy Saturday, or tight enough that it
     false-positives every Monday. This module computes a robust
     baseline *per day-of-week* directly from dbo.pipeline history, so
     "normal for a Saturday" and "normal for a Tuesday" are different
     baselines, no manual re-seeding required.

  2. NO EARLY WARNING
     check_out_of_range() only fires *after* a value crosses the line.
     check_predictive_trend() looks at the last N runs for a model,
     fits a simple linear trend, and warns if the metric is drifting
     toward a breach — e.g. elapsed_seconds creeping up run-over-run —
     even while the current run is still technically "in range." This
     is the "predictive alert" piece: catch it before it's a red row.

DESIGN CHOICES (why, not just what):
  - Uses the MEDIAN and MAD (median absolute deviation) instead of
    mean/stddev. ETL run history is small-sample and outlier-prone
    (a single stuck run, a one-off API hiccup) — MAD-based "modified
    z-scores" are the standard robust-statistics choice here and won't
    get dragged around by one bad historical run the way mean/stddev
    would. This is the same technique used in production anomaly
    detection systems (e.g. Twitter's classic S-H-ESD approach) when
    sample sizes are too small for full seasonal decomposition.
  - No new dependencies beyond numpy (already in most data-stack envs).
    No external ML service, no model training/retraining pipeline to
    maintain — everything is recomputed fresh from dbo.pipeline on
    every call, which is cheap at ETL-monitor scale (dozens of models,
    hundreds/thousands of historical runs).

INTEGRATION — 3 small edits to ETLMonitor.py:
-----------------------------------------------
1. Near your other imports, add:
       import ai_anomaly

2. Inside pipeline_status_summary(), right after the existing
   check_out_of_range() call, add:

       run['ai_warnings'] = ai_anomaly.check_statistical_anomaly(run, query)
       run['predictive_warnings'] = ai_anomaly.check_predictive_trend(
           run['model_name'], query
       )

   and fold them into the status_color logic, e.g.:

       if (run['status'] or '').lower() in ('fail','failed','failure','error') or run['issues']:
           run['status_color'] = 'danger'
       elif run['zero_warnings'] or run['recon_warnings'] or run['ai_warnings']:
           run['status_color'] = 'warning'
       elif run['predictive_warnings']:
           run['status_color'] = 'info'   # early warning, not yet a problem
       else:
           run['status_color'] = 'success'

3. Inside build_alert_payload(runs, health), include the new fields
   so your webhook payload carries them through to whatever consumes
   /api/alert (Slack, PagerDuty, etc.) — just add 'ai_warnings' and
   'predictive_warnings' to whatever per-run dict you're already
   building there.

That's it — no schema changes, no new tables, no cron jobs. Everything
reads from dbo.pipeline, which you're already populating.
"""

from datetime import datetime
import statistics
import numpy as np


# ── Tunables ─────────────────────────────────────────────────────────────
Z_THRESHOLD = 3.5          # modified z-score cutoff (3.5 is the standard
                            # robust-statistics default — roughly
                            # equivalent to "3 sigma" but MAD-based)
MIN_HISTORY_POINTS = 8     # don't try to flag anomalies with too little
                            # history — better to say nothing than to
                            # guess off 2 data points
BASELINE_LOOKBACK_DAYS = 90
TREND_LOOKBACK_RUNS = 12   # how many recent runs to fit a trend line on
TREND_MIN_POINTS = 6


# ── Robust statistics helpers ───────────────────────────────────────────

def _modified_z_scores(values):
    """
    Modified z-score using median + MAD instead of mean + stddev.
    Formula: 0.6745 * (x - median) / MAD
    (0.6745 makes MAD comparable in scale to a standard deviation
    under a normal distribution — this is the standard Iglewicz &
    Hoaglin constant used in robust outlier detection.)

    Falls back gracefully (returns 0s) if MAD is 0 (e.g. a model whose
    metric genuinely never varies) to avoid divide-by-zero false alarms.
    """
    if not values:
        return []
    median = statistics.median(values)
    abs_devs = [abs(v - median) for v in values]
    mad = statistics.median(abs_devs)
    if mad == 0:
        return [0.0] * len(values)
    return [0.6745 * (v - median) / mad for v in values]


def _fetch_history(model_name, query_fn, days=BASELINE_LOOKBACK_DAYS):
    """
    Pull successful-run history for a model from dbo.pipeline.
    Returns list of dicts with the metrics we baseline on, plus the
    day-of-week (0=Monday..6=Sunday) of each run's start_time.
    """
    sql = """
        SELECT start_record_count, end_record_count, records_added,
               elapsed_seconds, start_time
        FROM dbo.pipeline
        WHERE model_name = ?
          AND status = 'success'
          AND created_at >= DATEADD(day, -?, SYSUTCDATETIME())
        ORDER BY start_time ASC
    """
    rows = query_fn(sql, [model_name, days])
    for r in rows:
        st = r.get('start_time')
        r['_dow'] = st.weekday() if isinstance(st, datetime) else None
    return rows


# ── 1. Statistical (seasonality-aware) anomaly detection ───────────────

def check_statistical_anomaly(run, query_fn):
    """
    Compare `run`'s metrics against a robust, day-of-week-aware baseline
    built from dbo.pipeline history for that model.

    Returns a list of human-readable warning strings. Empty list = no
    statistical anomaly detected (or not enough history to judge yet —
    we deliberately stay silent rather than false-alarm on thin data).

    Only meaningful for successful runs; call this the same place you
    call check_out_of_range() / check_zero_rows().
    """
    warnings = []
    model_name = run.get('model_name')
    if not model_name:
        return warnings

    history = _fetch_history(model_name, query_fn)
    if len(history) < MIN_HISTORY_POINTS:
        return warnings  # not enough data to establish a baseline yet

    run_dow = None
    st = run.get('start_time')
    if isinstance(st, str):
        try:
            st = datetime.fromisoformat(st)
        except ValueError:
            st = None
    if isinstance(st, datetime):
        run_dow = st.weekday()

    metrics = [
        ('records_added',      run.get('records_added')),
        ('elapsed_seconds',    run.get('elapsed_seconds')),
        ('end_record_count',   run.get('end_record_count')),
    ]

    for metric_name, current_val in metrics:
        if current_val is None:
            continue

        # Prefer same-day-of-week history if there's enough of it;
        # otherwise fall back to the full history for this model.
        same_dow_vals = [
            h[metric_name] for h in history
            if h.get('_dow') == run_dow and h.get(metric_name) is not None
        ]
        if run_dow is not None and len(same_dow_vals) >= MIN_HISTORY_POINTS:
            baseline_vals = same_dow_vals
            scope = f"same-weekday ({['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][run_dow]})"
        else:
            baseline_vals = [h[metric_name] for h in history if h.get(metric_name) is not None]
            scope = "all-history"

        if len(baseline_vals) < MIN_HISTORY_POINTS:
            continue

        scores = _modified_z_scores(baseline_vals + [current_val])
        current_score = scores[-1]

        if abs(current_score) >= Z_THRESHOLD:
            median = statistics.median(baseline_vals)
            direction = "above" if current_val > median else "below"
            warnings.append(
                f"{metric_name} = {current_val:,.0f} is statistically anomalous "
                f"({direction} {scope} baseline median {median:,.0f}, "
                f"z-score {current_score:.1f})"
            )

    return warnings


# ── 2. Predictive trend alerting ────────────────────────────────────────

def check_predictive_trend(model_name, query_fn,
                            lookback=TREND_LOOKBACK_RUNS,
                            min_points=TREND_MIN_POINTS):
    """
    Fit a simple linear trend to the last `lookback` successful runs'
    elapsed_seconds for this model. If the trend is climbing steeply
    enough that the NEXT run is projected to land outside this model's
    configured range (dbo.pipeline_ranges) or, absent a configured
    range, outside a robust baseline band — warn now, before it
    actually happens.

    This is deliberately narrow-scope (elapsed_seconds only, for now):
    that's the metric where "it's been getting slower for the last
    week" is the classic early-warning signal for a pipeline about to
    start timing out or falling behind — far more useful to catch
    early than a one-off row-count blip.

    Returns a list of warning strings (usually 0 or 1).
    """
    sql = """
        SELECT TOP (?) elapsed_seconds, start_time
        FROM dbo.pipeline
        WHERE model_name = ? AND status = 'success' AND elapsed_seconds IS NOT NULL
        ORDER BY start_time DESC
    """
    rows = query_fn(sql, [lookback, model_name])
    if len(rows) < min_points:
        return []

    rows = list(reversed(rows))  # oldest -> newest for the fit
    y = np.array([r['elapsed_seconds'] for r in rows], dtype=float)
    x = np.arange(len(y), dtype=float)

    # Linear fit: slope tells us seconds-per-run trend direction/rate
    slope, intercept = np.polyfit(x, y, 1)
    projected_next = slope * len(y) + intercept

    # Only care about upward trends (getting slower), and only if the
    # trend is a meaningful fraction of the current typical value —
    # avoids flagging noise around a tiny baseline (e.g. 2s -> 2.3s).
    current_median = float(np.median(y))
    if slope <= 0 or current_median <= 0:
        return []

    pct_climb_per_run = (slope / current_median) * 100
    if pct_climb_per_run < 3:
        return []  # trending up, but too slowly to be worth an alert yet

    # Compare projected next value against this model's configured range,
    # if one exists — that gives us a concrete "N runs until breach" story.
    range_rows = query_fn(
        "SELECT elapsed_seconds_high FROM dbo.pipeline_ranges WHERE model_name = ?",
        [model_name]
    )
    high_bound = range_rows[0]['elapsed_seconds_high'] if range_rows and range_rows[0].get('elapsed_seconds_high') else None

    if high_bound:
        if projected_next <= high_bound:
            # Not projected to breach next run — but tell us how many
            # runs out the breach is, if it continues at this rate.
            remaining = high_bound - y[-1]
            runs_to_breach = remaining / slope if slope > 0 else None
            if runs_to_breach is not None and runs_to_breach <= 5:
                return [
                    f"elapsed_seconds trending up {pct_climb_per_run:.1f}% per run "
                    f"over last {len(y)} runs — projected to exceed configured max "
                    f"({high_bound:,.0f}s) in ~{runs_to_breach:.1f} more runs"
                ]
            return []
        else:
            return [
                f"elapsed_seconds trending up {pct_climb_per_run:.1f}% per run — "
                f"next run projected at {projected_next:,.0f}s, "
                f"already over configured max ({high_bound:,.0f}s)"
            ]

    # No configured range for this model — just flag a sustained,
    # meaningful upward trend on its own terms.
    return [
        f"elapsed_seconds trending up {pct_climb_per_run:.1f}% per run over "
        f"last {len(y)} runs (last: {y[-1]:,.0f}s, projected next: {projected_next:,.0f}s)"
    ]


# ── Convenience wrapper ──────────────────────────────────────────────────

def check_ai_anomalies(run, query_fn):
    """
    Single entry point: runs both checks and returns a combined dict.
    Call this once from pipeline_status_summary() instead of calling
    the two functions separately, if you prefer one line over two.
    """
    return {
        'ai_warnings': check_statistical_anomaly(run, query_fn),
        'predictive_warnings': check_predictive_trend(run.get('model_name'), query_fn),
    }
