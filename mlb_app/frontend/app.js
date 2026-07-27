const { useState, useEffect, useCallback } = React;

// Change this if your FastAPI server runs somewhere else.
const API_BASE = "http://localhost:8000/api";

async function api(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch (_) {}
    throw new Error(detail);
  }
  return res.json();
}

// ---------------------------------------------------------------------
// Small building blocks
// ---------------------------------------------------------------------

function Loading({ label }) {
  return <div className="empty-state"><span className="loading-dot" />{label || "Loading…"}</div>;
}

function ErrorBox({ message }) {
  return <div className="error-state">Couldn't load that. {message}</div>;
}

function EmptyState({ label }) {
  return <div className="empty-state">{label || "Nothing here yet."}</div>;
}

// Fetches `path` whenever it changes; returns {data, loading, error}.
function useApi(path, enabled = true) {
  const [state, setState] = useState({ data: null, loading: true, error: null });

  useEffect(() => {
    if (!enabled || !path) return;
    let cancelled = false;
    setState({ data: null, loading: true, error: null });
    api(path)
      .then((data) => { if (!cancelled) setState({ data, loading: false, error: null }); })
      .catch((err) => { if (!cancelled) setState({ data: null, loading: false, error: err.message }); });
    return () => { cancelled = true; };
  }, [path, enabled]);

  return state;
}

function Modal({ title, onClose, children }) {
  useEffect(() => {
    const onKey = (e) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal-panel panel" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3 className="panel-title" style={{ margin: 0 }}>{title}</h3>
          <button className="back-link" onClick={onClose}>Close</button>
        </div>
        <div className="modal-body">{children}</div>
      </div>
    </div>
  );
}

function PlayerYtdModal({ playerId, playerName, gamePk, role, onClose }) {
  const ytd = useApi(`/players/${playerId}/ytd?game_pk=${gamePk}&role=${role}`);
  return (
    <Modal title={`${playerName || "Player " + playerId} — YTD`} onClose={onClose}>
      {ytd.loading ? <Loading /> : ytd.error ? <ErrorBox message={ytd.error} /> :
        <KVGrid obj={ytd.data} />}
    </Modal>
  );
}

function PlayerAwardsModal({ playerId, playerName, onClose }) {
  const awards = useApi(`/players/${playerId}/awards`);
  const columns = [
    { key: "award_name", label: "Award" },
    { key: "season", label: "Season" },
    { key: "award_date", label: "Date" },
    { key: "team", label: "Team" },
  ];
  return (
    <Modal title={`${playerName || "Player " + playerId} — Awards`} onClose={onClose}>
      {awards.loading ? <Loading /> : awards.error ? <ErrorBox message={awards.error} /> :
        !awards.data?.length ? <EmptyState label="No awards on record" /> :
        <DataTable rows={awards.data} columns={columns} />}
    </Modal>
  );
}

function PlayerDraftModal({ playerId, playerName, onClose }) {
  // PLAYER_DRAFT returns a single object (the player's most recent draft
  // record), not a list, so this renders with KVGrid rather than DataTable.
  const draft = useApi(`/players/${playerId}/draft`);
  const hasRecord = draft.data && Object.keys(draft.data).length > 0;
  return (
    <Modal title={`${playerName || "Player " + playerId} — Draft`} onClose={onClose}>
      {draft.loading ? <Loading /> : draft.error ? <ErrorBox message={draft.error} /> :
        !hasRecord ? <EmptyState label="No draft record on file" /> :
        <KVGrid obj={draft.data} />}
    </Modal>
  );
}

function KVGrid({ obj, narrow }) {
  if (!obj) return <EmptyState />;
  const entries = Object.entries(obj).filter(([k]) => !k.startsWith("_"));
  if (!entries.length) return <EmptyState />;
  return (
    <div className={"kv-grid" + (narrow ? " kv-grid-narrow" : "")}>
      {entries.map(([k, v]) => (
        <div className="kv" key={k}>
          <div className="kv-label">{k.replace(/_/g, " ")}</div>
          <div className={"kv-value" + (typeof v === "number" ? " num" : "")}>
            {v === null || v === undefined || v === "" ? "—" : String(v)}
          </div>
        </div>
      ))}
    </div>
  );
}

function DataTable({ rows, columns, onRowClick, compact }) {
  if (!rows) return <Loading />;
  if (!rows.length) return <EmptyState />;
  const cols = columns || Object.keys(rows[0]);
  return (
    <div className="table-scroll">
      <table className={"data-table" + (compact ? " data-table--compact" : "")}>
        <thead>
          <tr>{cols.map((c) => <th key={c.key || c}>{c.label || String(c).replace(/_/g, " ")}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} onClick={() => onRowClick && onRowClick(row)}>
              {cols.map((c) => {
                const key = c.key || c;
                const render = c.render;
                return <td key={key}>{render ? render(row) : String(row[key] ?? "—")}</td>;
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// Columns dropped outright from the Game Details panel — internal ids
// that aren't useful to display (pitcher names are shown instead).
const GAME_DETAILS_HIDE = new Set(["winning_pitcher_id", "losing_pitcher_id", "save_pitcher_id"]);

// "1960-08-18T17:00:00+00:00" -> "8/18/1960 5:00 PM". Parsed from the raw
// string (not `new Date(...)`) so we render the wall-clock time the DB
// gave us, not that time shifted into the browser's local timezone.
function formatFirstPitch(v) {
  if (!v) return v;
  const m = String(v).match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
  if (!m) return v;
  const [, year, month, day, hour, minute] = m;
  let h = parseInt(hour, 10);
  const ampm = h >= 12 ? "PM" : "AM";
  h = h % 12 || 12;
  return `${parseInt(month, 10)}/${parseInt(day, 10)}/${year} ${h}:${minute} ${ampm}`;
}

// Reshapes the raw `/games/:pk/details` row for display: drops noisy
// id/load columns, collapses every home_*/away_* field down to a single
// team-name row, and formats attendance + first pitch.
function formatGameDetails(obj) {
  if (!obj) return obj;
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (k.startsWith("_")) continue;
    if (GAME_DETAILS_HIDE.has(k)) continue;
    if (/load_id$/i.test(k)) continue;

    if (/^home_/i.test(k)) {
      if (/^home_team_name$/i.test(k)) out["Home Team"] = v;
      continue;
    }
    if (/^away_/i.test(k)) {
      if (/^away_team_name$/i.test(k)) out["Away Team"] = v;
      continue;
    }
    if (k === "attendance") {
      out[k] = v == null || v === "" ? v : Number(v).toLocaleString();
      continue;
    }
    if (k === "first_pitch") {
      out[k] = formatFirstPitch(v);
      continue;
    }
    out[k] = v;
  }
  return out;
}

function BackLink({ onClick, label }) {
  return <button className="back-link" onClick={onClick}>← {label || "Back"}</button>;
}

// ---------------------------------------------------------------------
// Games
// ---------------------------------------------------------------------

function GamesView({ onSelectGame, onSelectTeam }) {
  const [mode, setMode] = useState("season"); // 'season' | 'team'
  const [season, setSeason] = useState("");
  const [teamId, setTeamId] = useState("");
  const [opponentId, setOpponentId] = useState("");

  const seasons = useApi("/seasons");
  const teams = useApi("/teams");

  const gamesPath =
    mode === "season" && season ? `/games?season=${season}` :
    mode === "team" && teamId ? `/games?team_id=${teamId}${opponentId ? `&opponent_id=${opponentId}` : ""}` :
    null;
  const games = useApi(gamesPath, !!gamesPath);

  const gameColumns = [
    { key: "game_date", label: "Date" },
    {
      key: "matchup", label: "Matchup",
      render: (r) => (
        <div className="matchup">
          <span className="abbr" onClick={(e) => { e.stopPropagation(); onSelectTeam(r.away_team_id); }}>
            {r.away_team_abbr}
          </span>
          <span className="at">@</span>
          <span className="abbr gold" onClick={(e) => { e.stopPropagation(); onSelectTeam(r.home_team_id); }}>
            {r.home_team_abbr}
          </span>
        </div>
      ),
    },
  ];

  return (
    <div>
      <h2 className="section-title">Games</h2>

      <div className="controls-row">
        <div className="field">
          <label>Browse by</label>
          <select value={mode} onChange={(e) => { setMode(e.target.value); setSeason(""); setTeamId(""); setOpponentId(""); }}>
            <option value="season">Season</option>
            <option value="team">Team</option>
          </select>
        </div>

        {mode === "season" && (
          <div className="field">
            <label>Season</label>
            <select value={season} onChange={(e) => setSeason(e.target.value)}>
              <option value="">Select a season…</option>
              {seasons.data?.map((s) => <option key={s.season} value={s.season}>{s.season}</option>)}
            </select>
          </div>
        )}

        {mode === "team" && (
          <>
            <div className="field">
              <label>Team</label>
              <select value={teamId} onChange={(e) => { setTeamId(e.target.value); setOpponentId(""); }}>
                <option value="">Select a team…</option>
                {teams.data?.map((t) => <option key={t.team_id} value={t.team_id}>{t.team_name}</option>)}
              </select>
            </div>
            {teamId && (
              <div className="field">
                <label>&nbsp;</label>
                <button className="back-link" style={{ marginBottom: 0 }} onClick={() => onSelectTeam(teamId)}>
                  View team details →
                </button>
              </div>
            )}
            {teamId && (
              <div className="field">
                <label>Opponent (optional)</label>
                <select value={opponentId} onChange={(e) => setOpponentId(e.target.value)}>
                  <option value="">All opponents</option>
                  {teams.data
                    ?.filter((t) => String(t.team_id) !== String(teamId))
                    .sort((a, b) => a.team_abbreviation.localeCompare(b.team_abbreviation))
                    .map((t) => <option key={t.team_id} value={t.team_id}>{t.team_abbreviation} — {t.team_name}</option>)}
                </select>
              </div>
            )}
          </>
        )}
      </div>

      {seasons.error && <ErrorBox message={seasons.error} />}
      {teams.error && <ErrorBox message={teams.error} />}

      {gamesPath ? (
        games.loading ? <Loading label="Loading games…" /> :
        games.error ? <ErrorBox message={games.error} /> :
        <DataTable rows={games.data} columns={gameColumns} onRowClick={(g) => onSelectGame(g.game_pk)} />
      ) : (
        <EmptyState label="Choose a season or team above to see games." />
      )}
    </div>
  );
}

// Column sets for the two stat tables in the Players panel. `sum: true`
// columns get added up in the totals row; rate stats (AVG, ERA) don't —
// summing a batting average or ERA across players is meaningless, so
// those show "—" in the totals row instead.
const BATTER_FIELDER_COLUMNS = [
  { key: "player_name", label: "Player" },
  { key: "position_name", label: "Pos" },
  { key: "batting_at_bats", label: "AB", sum: true },
  { key: "batting_runs", label: "R", sum: true },
  { key: "batting_hits", label: "H", sum: true },
  { key: "batting_doubles", label: "2B", sum: true },
  { key: "batting_triples", label: "3B", sum: true },
  { key: "batting_home_runs", label: "HR", sum: true },
  { key: "batting_rbi", label: "RBI", sum: true },
  { key: "batting_walks", label: "BB", sum: true },
  { key: "batting_strikeouts", label: "SO", sum: true },
  { key: "batting_stolen_bases", label: "SB", sum: true },
  { key: "batting_avg", label: "AVG" },
  { key: "fielding_putouts", label: "PO", sum: true },
  { key: "fielding_assists", label: "A", sum: true },
  { key: "fielding_errors", label: "E", sum: true },
];

const PITCHER_COLUMNS = [
  { key: "player_name", label: "Player" },
  { key: "pitching_innings_pitched", label: "IP", innings: true },
  { key: "pitching_hits_allowed", label: "H", sum: true },
  { key: "pitching_runs_allowed", label: "R", sum: true },
  { key: "pitching_earned_runs", label: "ER", sum: true },
  { key: "pitching_walks", label: "BB", sum: true },
  { key: "pitching_strikeouts", label: "SO", sum: true },
  { key: "pitching_home_runs_allowed", label: "HR", sum: true },
  { key: "pitching_era", label: "ERA" },
];

// Baseball innings-pitched notation is thirds, not decimal — "6.1" means
// 6⅓ innings (19 outs), not 6.1 innings. Summing the raw numbers with
// normal addition would silently produce wrong totals (e.g. two 0.2 lines
// should total 1.1, not 0.4), so convert to outs, sum, then convert back.
function sumInningsPitched(rows, key) {
  let outs = 0;
  for (const r of rows) {
    const v = parseFloat(r[key]);
    if (!isNaN(v)) {
      const whole = Math.trunc(v);
      const thirds = Math.round((v - whole) * 10); // the digit after the dot: 0, 1, or 2
      outs += whole * 3 + (thirds === 1 || thirds === 2 ? thirds : 0);
    }
  }
  return `${Math.trunc(outs / 3)}.${outs % 3}`;
}

function StatsTable({ title, rows, columns, onRowClick }) {
  if (!rows.length) return null;
  return (
    <div className="stats-table-block">
      <p className="panel-title">{title}</p>
      <div className="table-scroll">
        <table className="data-table data-table--compact">
          <thead>
            <tr>{columns.map((c) => <th key={c.key}>{c.label}</th>)}</tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} onClick={() => onRowClick && onRowClick(r)}>
                {columns.map((c) => (
                  <td key={c.key}>
                    {c.render
                      ? c.render(r)
                      : (r[c.key] === null || r[c.key] === undefined || r[c.key] === "" ? "—" : String(r[c.key]))}
                  </td>
                ))}
              </tr>
            ))}
            <tr className="totals-row">
              {columns.map((c, i) => {
                if (i === 0) return <td key={c.key}>Total</td>;
                if (c.innings) return <td key={c.key}>{sumInningsPitched(rows, c.key)}</td>;
                if (c.sum) return <td key={c.key}>{rows.reduce((s, r) => s + (Number(r[c.key]) || 0), 0)}</td>;
                return <td key={c.key}>—</td>;
              })}
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

// Appends an Actions column (YTD / Awards / Draft buttons) onto a base
// column set. `role` is "batter" or "pitcher" — passed straight through
// to the YTD endpoint so the backend knows which dw view to query,
// reusing the same classification GAME_PLAYERS_STATS already computed
// (player_role) rather than re-deriving it from scratch.
function withActions(baseColumns, role, onAction) {
  return [
    ...baseColumns,
    {
      key: "_actions",
      label: "Actions",
      render: (r) => (
        <div className="action-btns">
          <button className="mini-btn" onClick={(e) => { e.stopPropagation(); onAction("ytd", r, role); }}>YTD</button>
          <button className="mini-btn" onClick={(e) => { e.stopPropagation(); onAction("awards", r, role); }}>Awards</button>
          <button className="mini-btn" onClick={(e) => { e.stopPropagation(); onAction("draft", r, role); }}>Draft</button>
        </div>
      ),
    },
  ];
}

function GamePlayersStats({ gamePk }) {
  const stats = useApi(`/games/${gamePk}/players-stats`);
  const [modal, setModal] = useState(null); // {type: 'ytd'|'awards'|'draft', id, name, role}

  if (stats.loading) return <Loading />;
  if (stats.error) return <ErrorBox message={stats.error} />;
  const rows = stats.data || [];
  if (!rows.length) return <EmptyState label="No player stats for this game." />;

  const isBatter = (r) => r.player_role === "Batter/Fielder" || r.player_role === "Both";
  const isPitcher = (r) => r.player_role === "Pitcher" || r.player_role === "Both";

  const homeBatters = rows.filter((r) => r.is_home && isBatter(r));
  const homePitchers = rows.filter((r) => r.is_home && isPitcher(r));
  const awayBatters = rows.filter((r) => !r.is_home && isBatter(r));
  const awayPitchers = rows.filter((r) => !r.is_home && isPitcher(r));

  const homeTeam = rows.find((r) => r.is_home)?.team_name || "Home";
  const awayTeam = rows.find((r) => !r.is_home)?.team_name || "Away";

  const openModal = (type, r, role) => setModal({ type, id: r.player_id, name: r.player_name, role });

  return (
    <div>
      <StatsTable title={`${homeTeam} — Batting/Fielding`} rows={homeBatters}
        columns={withActions(BATTER_FIELDER_COLUMNS, "batter", openModal)} />
      <StatsTable title={`${homeTeam} — Pitching`} rows={homePitchers}
        columns={withActions(PITCHER_COLUMNS, "pitcher", openModal)} />
      <StatsTable title={`${awayTeam} — Batting/Fielding`} rows={awayBatters}
        columns={withActions(BATTER_FIELDER_COLUMNS, "batter", openModal)} />
      <StatsTable title={`${awayTeam} — Pitching`} rows={awayPitchers}
        columns={withActions(PITCHER_COLUMNS, "pitcher", openModal)} />

      {modal?.type === "ytd" && (
        <PlayerYtdModal playerId={modal.id} playerName={modal.name} gamePk={gamePk} role={modal.role} onClose={() => setModal(null)} />
      )}
      {modal?.type === "awards" && (
        <PlayerAwardsModal playerId={modal.id} playerName={modal.name} onClose={() => setModal(null)} />
      )}
      {modal?.type === "draft" && (
        <PlayerDraftModal playerId={modal.id} playerName={modal.name} onClose={() => setModal(null)} />
      )}
    </div>
  );
}

function GameDetailView({ gamePk, onBack, onSelectPlayer }) {
  const details = useApi(`/games/${gamePk}/details`);
  const umpires = useApi(`/games/${gamePk}/umpires`);
  const performance = useApi(`/games/${gamePk}/umpire-performance`);

  // Umpire performance's full schema isn't known ahead of time (SELECT *),
  // so build its column list dynamically: drop ids that aren't useful to
  // show (game_pk is implied by the page, official_id is replaced by the
  // joined umpire name, load_id is dlt/pipeline metadata), and lead with
  // the umpire's name instead of their id.
  const performanceColumns = performance.data?.length
    ? (() => {
        const HIDE = new Set(["game_pk", "load_id", "official_id"]);
        const keys = Object.keys(performance.data[0]).filter(
          (k) => !HIDE.has(k) && !/(^|_)key$/i.test(k) && k !== "full_name"
        );
        const cols = [];
        if ("full_name" in performance.data[0]) cols.push({ key: "full_name", label: "Official" });
        return cols.concat(keys);
      })()
    : undefined;

  return (
    <div>
      <BackLink onClick={onBack} label="Back to games" />
      <h2 className="section-title">Game {gamePk}</h2>

      <div className="game-detail-grid">
        <div className="panel">
          <p className="panel-title">Game details</p>
          {details.loading ? <Loading /> : details.error ? <ErrorBox message={details.error} /> :
            <KVGrid obj={formatGameDetails(details.data)} narrow />}
        </div>

        <div className="panel">
          <p className="panel-title">Players</p>
          <GamePlayersStats gamePk={gamePk} />
        </div>

        <div>
          <div className="panel">
            <p className="panel-title">Umpires</p>
            {umpires.loading ? <Loading /> : umpires.error ? <ErrorBox message={umpires.error} /> :
              <DataTable rows={umpires.data} columns={["official_type", "full_name"]} />}
          </div>

          <div className="panel">
            <p className="panel-title">Umpire performance</p>
            {performance.loading ? <Loading /> : performance.error ? <ErrorBox message={performance.error} /> :
              <DataTable rows={performance.data} columns={performanceColumns} compact />}
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------
// Players
// ---------------------------------------------------------------------

function PlayersView({ activeGamePk, onSelectPlayer }) {
  const gamePlayers = useApi(activeGamePk ? `/games/${activeGamePk}/players` : null, !!activeGamePk);
  const gameVenue = useApi(activeGamePk ? `/games/${activeGamePk}/venue` : null, !!activeGamePk);
  const allPlayers = useApi(activeGamePk ? null : "/players", !activeGamePk);

  const columns = [
    { key: "full_name", label: "Name" },
    { key: "primary_position", label: "Position" },
  ];

  return (
    <div>
      <h2 className="section-title">Players</h2>

      {activeGamePk && (
        <div className="panel">
          <p className="panel-title">Venue for game {activeGamePk}</p>
          {gameVenue.loading ? <Loading /> : gameVenue.error ? <ErrorBox message={gameVenue.error} /> : <KVGrid obj={gameVenue.data} />}
        </div>
      )}

      <div className="panel">
        <p className="panel-title">{activeGamePk ? `Players in game ${activeGamePk}` : "All players"}</p>
        {(() => {
          const src = activeGamePk ? gamePlayers : allPlayers;
          if (src.loading) return <Loading />;
          if (src.error) return <ErrorBox message={src.error} />;
          return <DataTable rows={src.data} columns={columns} onRowClick={(p) => onSelectPlayer(p.player_id)} />;
        })()}
      </div>
    </div>
  );
}

function PlayerDetailView({ playerId, onBack, onSelectTeam }) {
  const player = useApi(`/players/${playerId}`);
  const isPitcher = (player.data?.primary_position || player.data?.primaryPosition || "").toString().toUpperCase().includes("P");
  const atBats = useApi(`/players/${playerId}/at-bats?role=${isPitcher ? "pitcher" : "batter"}`, !player.loading);
  const draft = useApi(`/players/${playerId}/draft`);
  const awards = useApi(`/players/${playerId}/awards`);

  return (
    <div>
      <BackLink onClick={onBack} label="Back to players" />
      <h2 className="section-title">Player {playerId}</h2>

      <div className="panel">
        <p className="panel-title">Bio</p>
        {player.loading ? <Loading /> : player.error ? <ErrorBox message={player.error} /> : <KVGrid obj={player.data} />}
      </div>

      <div className="panel">
        <p className="panel-title">{isPitcher ? "At-bats faced (pitching)" : "At-bats (batting)"}</p>
        {atBats.loading ? <Loading /> : atBats.error ? <ErrorBox message={atBats.error} /> : <DataTable rows={atBats.data} />}
      </div>

      <div className="panel">
        <p className="panel-title">Draft</p>
        {draft.loading ? <Loading /> : draft.error ? <ErrorBox message={draft.error} /> :
          !draft.data || !Object.keys(draft.data).length ? <EmptyState label="No draft record on file" /> :
          <KVGrid obj={draft.data} />}
      </div>

      <div className="panel">
        <p className="panel-title">Awards</p>
        {awards.loading ? <Loading /> : awards.error ? <ErrorBox message={awards.error} /> : <DataTable rows={awards.data} />}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------
// Teams
// ---------------------------------------------------------------------

function TeamDetailView({ teamId, onBack }) {
  const team = useApi(`/teams/${teamId}`);
  const venue = useApi(`/teams/${teamId}/venue`);
  const draft = useApi(`/teams/${teamId}/draft`);

  return (
    <div>
      <BackLink onClick={onBack} label="Back" />
      <h2 className="section-title">Team {teamId}</h2>

      <div className="panel">
        <p className="panel-title">Team info</p>
        {team.loading ? <Loading /> : team.error ? <ErrorBox message={team.error} /> : <KVGrid obj={team.data} />}
      </div>

      <div className="panel">
        <p className="panel-title">Venue</p>
        {venue.loading ? <Loading /> : venue.error ? <ErrorBox message={venue.error} /> : <KVGrid obj={venue.data} />}
      </div>

      <div className="panel">
        <p className="panel-title">Draft picks</p>
        {draft.loading ? <Loading /> : draft.error ? <ErrorBox message={draft.error} /> : <DataTable rows={draft.data} />}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------
// Simple list menu pages
// ---------------------------------------------------------------------

function SimpleListView({ title, path }) {
  const { data, loading, error } = useApi(path);
  return (
    <div>
      <h2 className="section-title">{title}</h2>
      <div className="panel">
        {loading ? <Loading /> : error ? <ErrorBox message={error} /> : <DataTable rows={data} />}
      </div>
    </div>
  );
}

function NotSpecifiedView({ title, path }) {
  const { data } = useApi(path);
  return (
    <div>
      <h2 className="section-title">{title}</h2>
      <div className="panel">
        <p className="panel-title">Not specified yet</p>
        <p className="empty-state">{data?.message || "This section wasn't described in the spec — say what it should show and it'll get built out."}</p>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------
// App shell
// ---------------------------------------------------------------------

const TABS = ["Games", "Players", "Awards", "Draft", "Umpires", "Matchups", "Pitcher"];

function App() {
  const [tab, setTab] = useState("Games");
  const [view, setView] = useState(null); // {type: 'gameDetail'|'playerDetail'|'teamDetail', id}
  const [activeGamePk, setActiveGamePk] = useState(null); // last selected game, used by Players tab

  const goTab = (t) => { setTab(t); setView(null); };

  let content;
  if (view?.type === "gameDetail") {
    content = (
      <GameDetailView
        gamePk={view.id}
        onBack={() => setView(null)}
        onSelectPlayer={(id) => setView({ type: "playerDetail", id })}
      />
    );
  } else if (view?.type === "playerDetail") {
    content = <PlayerDetailView playerId={view.id} onBack={() => setView(null)} />;
  } else if (view?.type === "teamDetail") {
    content = <TeamDetailView teamId={view.id} onBack={() => setView(null)} />;
  } else {
    switch (tab) {
      case "Games":
        content = (
          <GamesView
            onSelectGame={(pk) => { setActiveGamePk(pk); setView({ type: "gameDetail", id: pk }); }}
            onSelectTeam={(id) => setView({ type: "teamDetail", id })}
          />
        );
        break;
      case "Players":
        content = <PlayersView activeGamePk={activeGamePk} onSelectPlayer={(id) => setView({ type: "playerDetail", id })} />;
        break;
      case "Awards":
        content = <SimpleListView title="Awards" path="/awards" />;
        break;
      case "Draft":
        content = <SimpleListView title="Draft" path="/draft" />;
        break;
      case "Umpires":
        content = <SimpleListView title="Umpires" path="/umpires" />;
        break;
      case "Matchups":
        content = <NotSpecifiedView title="Matchups" path="/matchups" />;
        break;
      case "Pitcher":
        content = <NotSpecifiedView title="Pitcher" path="/pitchers" />;
        break;
      default:
        content = null;
    }
  }

  return (
    <div>
      <header className="app-header">
        <div className="brand-row">
          <div className="brand">MLB <span>Explorer</span></div>
          <div className="brand-sub">silver schema · live</div>
        </div>
        <nav className="tab-row">
          {TABS.map((t) => (
            <button key={t} className={"tab" + (tab === t && !view ? " active" : "")} onClick={() => goTab(t)}>
              {t}
            </button>
          ))}
        </nav>
      </header>
      <main className="page">{content}</main>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);