# HARDSTATS OGame (Alpha v0.1)

Dashboard analytics **lecture seule** pour OGame, basé uniquement sur les endpoints publics `/api/` (aucun login, aucun cookie, aucune action de jeu, aucun navigateur).

## Prereqs
- Python 3.11+

## Quickstart (PowerShell)

```powershell
New-Item -ItemType Directory -Force "C:\HARDSTATS OGame\ogame-stats"
Set-Location "C:\HARDSTATS OGame\ogame-stats"
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m ogame_stats init --config config.yaml
python -m ogame_stats list-universes --community fr
python -m ogame_stats collect --config config.yaml
python -m ogame_stats render --config config.yaml
python -m ogame_stats publish --config config.yaml
```

## Configuration

1. Trouver un univers FR: `python -m ogame_stats list-universes --community fr`
2. Mettre à jour `config.yaml`:
   - `universe.server_id: "sXXX-fr"`
   - `player_name: "VotrePseudo"`
   - Optionnel: `discord.webhook_url` (sinon `dry_run: true` imprime en console)

## CLI

- `python -m ogame_stats list-universes --community fr`
- `python -m ogame_stats init --config config.yaml`
- `python -m ogame_stats collect --config config.yaml`
- `python -m ogame_stats render --config config.yaml --date YYYY-MM-DD`
- `python -m ogame_stats publish --config config.yaml`
- `python -m ogame_stats post-recap --config config.yaml`
- `python -m ogame_stats run --config config.yaml`

## Publication (GitHub Pages)

Objectif: ne pas attacher le HTML sur Discord, mais poster un lien vers `latest.html` heberge.

1. Dans `config.yaml`:
   - `output.publish_dir: "./docs"`
   - `output.latest_filename: "latest.html"`
   - `output.keep_history: true`
   - `output.public_base_url: "https://<user>.github.io/<repo>/"`
2. Generer et publier:
   - `python -m ogame_stats render --config config.yaml`
   - `python -m ogame_stats publish --config config.yaml`
3. Activer GitHub Pages:
   - Settings -> Pages -> Deploy from a branch
   - Branch: `main` (ou `master`), Folder: `/docs`
4. Commit/push du dossier `docs/` (et **ne pas** committer `data/` ni `out/`).

Quand `output.public_base_url` est defini, `post-recap` poste des liens (Clean/Neon) au lieu d'une piece jointe.

## Notes importantes

- Les chemins relatifs du YAML sont resolves **par rapport au dossier du `config.yaml`**, pas par rapport au `cwd`.
- Les snapshots sont dedupliquees via l’attribut `timestamp` fourni par l’API (root XML / JSON), par `metric_type`.
- Timezone: `Europe/Paris` (via `zoneinfo`).
- Le webhook Discord est une URL sensible: evite de committer `config.yaml` si tu y mets le webhook.

## Data sources

- Lobby servers (JSON): `https://lobby.ogame.gameforge.com/api/servers`
- OGame public API: `https://{serverId}.ogame.gameforge.com/api/...`

## Output

- SQLite: `./data/ogame_stats.sqlite`
- Rapports HTML: `./out/report_YYYY-MM-DD_{serverId}_{player}.html`
