"""
Post-deployment semantic model connection repair script.
Adapted from FabricDevCamp/fabric-devops pattern.
Runs on the Azure DevOps agent (not as a Fabric notebook) to avoid XMLA auth issues.
Uses Fabric REST API and Power BI REST API with direct SP authentication.

Supported semantic model storage modes (detected per model from the TMDL definition):
  - Import                  : repoint source, bind SQL connection, full refresh
  - DirectQuery             : repoint source, bind SQL connection, NO refresh (not supported by the service)
  - Dual / Composite        : treated like Import (has imported tables) -> refresh
  - Direct Lake on SQL      : repoint Sql.Database expression, bind SQL connection, refresh (framing)
  - Direct Lake on OneLake  : repoint OneLake workspace/item GUIDs, refresh (framing)
"""

import os, sys, time, base64, re, argparse, json, requests
from azure.identity import ClientSecretCredential

FABRIC_API = "https://api.fabric.microsoft.com/v1"
POWERBI_API = "https://api.powerbi.com/v1.0/myorg"
REQUEST_TIMEOUT = 120

GUID = r"[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}"

# Sql.Database("server", "database" ...)  -> Import, DirectQuery, Direct Lake on SQL
SQL_DB_RE = re.compile(r'(Sql\.Database\(\s*")([^"]+)("\s*,\s*")([^"]+)(")')
# https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<itemId>  -> Direct Lake on OneLake / ADLS-style import
ONELAKE_RE = re.compile(rf"(onelake\.dfs\.fabric\.microsoft\.com/)({GUID})/({GUID})", re.I)
# Lakehouse.Contents(null){[workspaceId="..."]}[Data]{[lakehouseId="..."]}[Data]  -> Import / DQ via Lakehouse connector
NAV_WS_RE = re.compile(rf'(workspaceId\s*=\s*")({GUID})(")', re.I)
NAV_LH_RE = re.compile(rf'(lakehouseId\s*=\s*")({GUID})(")', re.I)

PARTITION_RE = re.compile(r"^\s*partition\s+.+?=\s*(\w+)", re.M)
MODE_RE = re.compile(r"^\s*mode:\s*(\w+)", re.M | re.I)


class TokenManager:
    """Manages token acquisition for Fabric and Power BI APIs."""

    def __init__(self, credential):
        self.credential = credential

    def get_fabric_headers(self):
        token = self.credential.get_token("https://api.fabric.microsoft.com/.default")
        return {"Authorization": f"Bearer {token.token}", "Content-Type": "application/json"}

    def get_powerbi_headers(self):
        token = self.credential.get_token("https://analysis.windows.net/powerbi/api/.default")
        return {"Authorization": f"Bearer {token.token}", "Content-Type": "application/json"}


# --------------------------------------------------------------------------- #
# Fabric helpers
# --------------------------------------------------------------------------- #

def get_paged(url, headers):
    """GET a Fabric list endpoint, following continuationUri."""
    items = []
    while url:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        body = response.json()
        items.extend(body.get("value", []))
        url = body.get("continuationUri")
    return items


def get_workspace_id(workspace_name, headers):
    """Resolve workspace GUID from display name."""
    for ws in get_paged(f"{FABRIC_API}/workspaces", headers):
        if ws["displayName"] == workspace_name:
            return ws["id"]
    raise ValueError(f"Workspace '{workspace_name}' not found.")


def list_items(workspace_id, item_type, headers):
    """List items of a given type in a workspace."""
    return get_paged(f"{FABRIC_API}/workspaces/{workspace_id}/items?type={item_type}", headers)


def get_lakehouse_sql_endpoint(workspace_id, lakehouse_id, headers):
    """Get the SQL endpoint for a lakehouse, polling until provisioned."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
    for _ in range(30):
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        sql_props = response.json().get("properties", {}).get("sqlEndpointProperties", {})
        if sql_props.get("provisioningStatus") == "Success":
            return {"server": sql_props["connectionString"], "database": sql_props["id"]}
        print(f"  SQL endpoint provisioning: {sql_props.get('provisioningStatus')}, waiting...")
        time.sleep(10)
    raise TimeoutError("SQL endpoint did not provision in time.")


def poll_lro(location, headers, max_attempts=30):
    """Poll a Fabric long-running operation until completion. Returns the result URL."""
    retry_after = 5
    for _ in range(max_attempts):
        time.sleep(retry_after)
        response = requests.get(location, headers=headers, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            continue
        result = response.json()
        retry_after = int(response.headers.get("Retry-After", retry_after))
        if result.get("status") == "Succeeded":
            return location.rstrip("/") + "/result"
        if result.get("status") == "Failed":
            raise RuntimeError(f"LRO failed: {json.dumps(result.get('error', {}))}")
    raise TimeoutError("LRO polling timed out.")


def get_item_definition(workspace_id, item_id, headers):
    """Get semantic model definition in TMDL format (handles LRO).

    TMDL is requested explicitly: without it the service may return TMSL (model.bim),
    in which case expressions/partitions cannot be scanned part-by-part.
    """
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/getDefinition?format=TMDL"
    response = requests.post(url, headers=headers, timeout=REQUEST_TIMEOUT)

    if response.status_code == 202:
        result_url = poll_lro(response.headers["Location"], headers)
        response = requests.get(result_url, headers=headers, timeout=REQUEST_TIMEOUT)

    response.raise_for_status()
    result = response.json()
    if "definition" in result:
        return result
    if "parts" in result:
        return {"definition": result}
    return result


def update_item_definition(workspace_id, item_id, definition, headers):
    """Update item definition via Fabric REST API (handles LRO)."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/items/{item_id}/updateDefinition"
    response = requests.post(url, headers=headers, json={"definition": definition}, timeout=REQUEST_TIMEOUT)
    if response.status_code == 202:
        poll_lro(response.headers.get("Location", ""), headers)
    elif response.status_code != 200:
        print(f"  Update definition failed ({response.status_code}): {response.text}")
        return False
    return True


# --------------------------------------------------------------------------- #
# TMDL analysis + rewrite
# --------------------------------------------------------------------------- #

def _decode(part):
    return base64.b64decode(part["payload"]).decode("utf-8")


def _encode(text):
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


def _tmdl_parts(definition):
    return [p for p in definition.get("parts", []) if p["path"].lower().endswith(".tmdl")]


def detect_model_profile(definition):
    """Return the storage modes present in the model and the Direct Lake flavour.

    modes: subset of {"import", "directquery", "dual", "directlake"}
    dl_flavour: "sql" | "onelake" | None (only meaningful when "directlake" in modes)
    """
    modes = set()
    has_sql_expr = has_onelake_expr = False

    for part in _tmdl_parts(definition):
        text = _decode(part)

        matches = list(PARTITION_RE.finditer(text))
        for i, m in enumerate(matches):
            ptype = m.group(1).lower()
            if ptype == "calculated":
                continue  # calculated tables have no external source
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            mode_match = MODE_RE.search(text[m.end():end])
            if mode_match:
                modes.add(mode_match.group(1).lower())
            else:
                # no explicit mode: entity partitions are Direct Lake, M partitions are Import
                modes.add("directlake" if ptype == "entity" else "import")

        if SQL_DB_RE.search(text):
            has_sql_expr = True
        if ONELAKE_RE.search(text):
            has_onelake_expr = True

    dl_flavour = None
    if "directlake" in modes:
        dl_flavour = "onelake" if (has_onelake_expr and not has_sql_expr) else "sql"
    return {"modes": modes, "dl_flavour": dl_flavour}


def rewrite_text(text, target):
    """Repoint every supported source reference in a TMDL text to the target item."""
    changes = []

    def sql_sub(m):
        server, db = m.group(2), m.group(4)
        if server == target["server"] and db == target["database"]:
            return m.group(0)
        changes.append(f"Sql.Database {server} / {db}")
        return f'{m.group(1)}{target["server"]}{m.group(3)}{target["database"]}{m.group(5)}'

    def onelake_sub(m):
        ws, item = m.group(2), m.group(3)
        if ws.lower() == target["workspace_id"].lower() and item.lower() == target["lakehouse_id"].lower():
            return m.group(0)
        changes.append(f"OneLake {ws}/{item}")
        return f'{m.group(1)}{target["workspace_id"]}/{target["lakehouse_id"]}'

    def nav_sub(key):
        def _sub(m):
            if m.group(2).lower() == target[key].lower():
                return m.group(0)
            changes.append(f"{key} {m.group(2)}")
            return f"{m.group(1)}{target[key]}{m.group(3)}"
        return _sub

    text = SQL_DB_RE.sub(sql_sub, text)
    text = ONELAKE_RE.sub(onelake_sub, text)
    text = NAV_WS_RE.sub(nav_sub("workspace_id"), text)
    text = NAV_LH_RE.sub(nav_sub("lakehouse_id"), text)
    return text, changes


def rewrite_definition(definition, target):
    """Rewrite all TMDL parts (expressions.tmdl AND tables/*.tmdl partitions) in place.

    Returns a list of human-readable change descriptions (empty if already correct).
    """
    all_changes = []
    for part in _tmdl_parts(definition):
        new_text, changes = rewrite_text(_decode(part), target)
        if changes:
            part["payload"] = _encode(new_text)
            all_changes.extend(f"{part['path']}: {c}" for c in changes)
    return all_changes


# --------------------------------------------------------------------------- #
# Connection / binding
# --------------------------------------------------------------------------- #

def find_or_create_connection(server, database, ws_id, lh_name, tenant_id, client_id, client_secret, headers):
    """Find an existing SQL connection or create a new one."""
    display_name = f"Workspace[{ws_id}]-Lakehouse[{lh_name}]-SqlEndpoint"

    # Search existing connections (paged)
    try:
        for conn in get_paged(f"{FABRIC_API}/connections", headers):
            if conn.get("displayName") == display_name:
                print(f"  Reusing existing connection: {conn['id']}")
                return conn["id"]
    except requests.HTTPError as e:
        print(f"  Could not list connections ({e}); will try to create one")

    # Create new connection
    body = {
        "displayName": display_name,
        "connectivityType": "ShareableCloud",
        "privacyLevel": "Organizational",
        "connectionDetails": {
            "type": "SQL", "creationMethod": "Sql",
            "parameters": [
                {"value": server, "dataType": "Text", "name": "server"},
                {"value": database, "dataType": "Text", "name": "database"}
            ]
        },
        "credentialDetails": {
            "credentials": {
                "tenantId": tenant_id, "servicePrincipalClientId": client_id,
                "servicePrincipalSecret": client_secret, "credentialType": "ServicePrincipal"
            },
            "singleSignOnType": "None", "connectionEncryption": "NotEncrypted",
            "skipTestConnection": False
        }
    }
    response = requests.post(f"{FABRIC_API}/connections", headers=headers, json=body, timeout=REQUEST_TIMEOUT)
    if response.status_code in (200, 201):
        conn_id = response.json()["id"]
        print(f"  Created new connection: {conn_id}")
        return conn_id
    print(f"  Failed to create connection ({response.status_code}): {response.text}")
    return None


def takeover_semantic_model(ws_id, sm_id, headers):
    """Take over ownership of a semantic model."""
    response = requests.post(f"{POWERBI_API}/groups/{ws_id}/datasets/{sm_id}/Default.TakeOver",
                             headers=headers, timeout=REQUEST_TIMEOUT)
    if response.status_code == 200:
        print("  Took over ownership")
        return True
    print(f"  Takeover failed ({response.status_code}): {response.text}")
    return False


def get_datasources(ws_id, sm_id, headers):
    """List the model's current datasources (requires ownership)."""
    response = requests.get(f"{POWERBI_API}/groups/{ws_id}/datasets/{sm_id}/datasources",
                            headers=headers, timeout=REQUEST_TIMEOUT)
    if response.status_code != 200:
        print(f"  Could not read datasources ({response.status_code}): {response.text}")
        return []
    return response.json().get("value", [])


def bind_semantic_model(ws_id, sm_id, conn_id, headers):
    """Bind semantic model to a SQL connection."""
    body = {"gatewayObjectId": "00000000-0000-0000-0000-000000000000", "datasourceObjectIds": [conn_id]}
    response = requests.post(f"{POWERBI_API}/groups/{ws_id}/datasets/{sm_id}/Default.BindToGateway",
                             headers=headers, json=body, timeout=REQUEST_TIMEOUT)
    if response.status_code == 200:
        print("  Bound to connection")
        return True
    print(f"  Bind failed ({response.status_code}): {response.text}")
    return False


def refresh_semantic_model(ws_id, sm_id, headers):
    """Trigger a full refresh (Import data load / Direct Lake framing) and wait.

    Returns True on success, False on failure/timeout.
    """
    response = requests.post(
        f"{POWERBI_API}/groups/{ws_id}/datasets/{sm_id}/refreshes",
        headers=headers, json={"notifyOption": "NoNotification", "type": "Full"},
        timeout=REQUEST_TIMEOUT
    )
    if response.status_code != 202:
        print(f"  Refresh trigger failed ({response.status_code}): {response.text}")
        return False
    refresh_id = response.headers.get("x-ms-request-id")
    if not refresh_id:
        print("  Refresh triggered (no polling ID)")
        return True
    poll_url = f"{POWERBI_API}/groups/{ws_id}/datasets/{sm_id}/refreshes/{refresh_id}"
    for _ in range(120):
        time.sleep(6)
        r = requests.get(poll_url, headers=headers, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            details = r.json()
            status = details.get("status", "Unknown")
            if status not in ("Unknown", "NotStarted", "InProgress"):
                print(f"  Refresh status: {status}")
                if status != "Completed":
                    print(f"  Error: {details.get('serviceExceptionJson', 'N/A')}")
                    return False
                return True
    print("  Refresh polling timed out")
    return False


# --------------------------------------------------------------------------- #
# Per-model orchestration
# --------------------------------------------------------------------------- #

def describe_profile(profile):
    modes = sorted(profile["modes"]) or ["unknown"]
    label = ", ".join(modes)
    if profile["dl_flavour"]:
        label += f" (Direct Lake on {'OneLake' if profile['dl_flavour'] == 'onelake' else 'SQL'})"
    return label


def needs_refresh(profile):
    """Pure DirectQuery models cannot be refreshed; everything else can/should be."""
    modes = profile["modes"]
    if not modes:
        return True
    return bool(modes - {"directquery"})


def repair_model(sm, ws_id, target, args, token_mgr):
    """Repair a single semantic model. Returns a result dict; raises on hard failure."""
    result = {"model": sm["displayName"], "modes": "", "definition": "-", "bound": "-", "refresh": "-", "ok": True}

    fabric_headers = token_mgr.get_fabric_headers()
    pbi_headers = token_mgr.get_powerbi_headers()

    # 1. Definition: detect mode(s), repoint sources in expressions AND table partitions
    defn = get_item_definition(ws_id, sm["id"], fabric_headers)
    definition = defn.get("definition", {})
    if not _tmdl_parts(definition):
        print("  No TMDL parts found (model may not be exposed as TMDL), skipping")
        result.update(modes="n/a", ok=False)
        return result

    profile = detect_model_profile(definition)
    result["modes"] = describe_profile(profile)
    print(f"  Detected: {result['modes']}")

    changes = rewrite_definition(definition, target)
    if changes:
        for c in changes:
            print(f"  Replacing -> {c}")
        if not update_item_definition(ws_id, sm["id"], definition, fabric_headers):
            result.update(definition="FAILED", ok=False)
            return result
        print("  Definition updated")
        result["definition"] = "updated"
    else:
        print("  Definition already correct")
        result["definition"] = "unchanged"

    # 2. Ownership + datasource binding (SQL-based sources only)
    if not takeover_semantic_model(ws_id, sm["id"], pbi_headers):
        result.update(bound="takeover failed", ok=False)
        return result

    datasources = get_datasources(ws_id, sm["id"], pbi_headers)
    sql_sources = [d for d in datasources if str(d.get("datasourceType", "")).lower() in ("sql", "sqlserver")]

    if sql_sources:
        conn_id = find_or_create_connection(
            target["server"], target["database"], ws_id, args.target_lakehouse,
            args.aztenantid, args.azclientid, args.azspsecret, fabric_headers
        )
        if not conn_id:
            result.update(bound="no connection", ok=False)
            return result
        if not bind_semantic_model(ws_id, sm["id"], conn_id, pbi_headers):
            result.update(bound="FAILED", ok=False)
            return result
        result["bound"] = "SQL connection"
    else:
        # e.g. Direct Lake on OneLake (SSO / fixed identity) or Lakehouse.Contents sources
        kinds = sorted({str(d.get("datasourceType")) for d in datasources}) or ["none"]
        print(f"  No SQL datasource to bind (datasources: {', '.join(kinds)}), skipping bind")
        result["bound"] = "n/a"

    # 3. Refresh (Import load / Direct Lake framing); skip for pure DirectQuery
    if args.skip_refresh:
        result["refresh"] = "skipped (flag)"
    elif not needs_refresh(profile):
        print("  Pure DirectQuery model: refresh not supported/needed, skipping")
        result["refresh"] = "n/a (DirectQuery)"
    else:
        print("  Refreshing..." if "directlake" not in profile["modes"] else "  Framing (refresh)...")
        ok = refresh_semantic_model(ws_id, sm["id"], pbi_headers)
        result["refresh"] = "ok" if ok else "FAILED"
        result["ok"] = result["ok"] and ok

    return result


def main():
    parser = argparse.ArgumentParser(description="Post-deployment semantic model connection repair.")
    parser.add_argument("--aztenantid", required=True)
    parser.add_argument("--azclientid", required=True)
    parser.add_argument("--azspsecret", required=True)
    parser.add_argument("--target_env", required=True)
    parser.add_argument("--target_lakehouse", default="DemoLakehouse")
    parser.add_argument("--models", default="", help="Optional comma-separated semantic model names to repair (default: all non-default models)")
    parser.add_argument("--skip_refresh", action="store_true", help="Repair connections only; do not refresh/frame")
    args = parser.parse_args()

    # Authenticate
    print("Authenticating...")
    credential = ClientSecretCredential(client_id=args.azclientid, client_secret=args.azspsecret, tenant_id=args.aztenantid)
    token_mgr = TokenManager(credential)
    fabric_headers = token_mgr.get_fabric_headers()

    # Resolve workspace
    ws_name = os.environ[f"{args.target_env}WorkspaceName".upper()]
    print(f"Workspace: {ws_name}")
    ws_id = get_workspace_id(ws_name, fabric_headers)

    # Get target lakehouse + SQL endpoint (covers SQL- and OneLake-based sources)
    print(f"Getting SQL endpoint for '{args.target_lakehouse}'...")
    lakehouses = list_items(ws_id, "Lakehouse", fabric_headers)
    target_lh = next((lh for lh in lakehouses if lh["displayName"] == args.target_lakehouse), None)
    if not target_lh:
        raise ValueError(f"Lakehouse '{args.target_lakehouse}' not found.")
    sql_endpoint = get_lakehouse_sql_endpoint(ws_id, target_lh["id"], fabric_headers)
    target = {
        "server": sql_endpoint["server"],
        "database": sql_endpoint["database"],
        "workspace_id": ws_id,
        "lakehouse_id": target_lh["id"],
    }
    print(f"Target: server={target['server']}, database={target['database']}, "
          f"workspace={ws_id}, lakehouse={target_lh['id']}")

    # Identify non-default semantic models
    all_sms = list_items(ws_id, "SemanticModel", fabric_headers)
    default_names = {i["displayName"] for i in lakehouses + list_items(ws_id, "Warehouse", fabric_headers)}
    wanted = {m.strip() for m in args.models.split(",") if m.strip()}
    results = []

    for sm in all_sms:
        if sm["displayName"] in default_names:
            continue
        if wanted and sm["displayName"] not in wanted:
            continue

        print(f"\nProcessing: {sm['displayName']}")
        try:
            results.append(repair_model(sm, ws_id, target, args, token_mgr))
        except Exception as e:
            print(f"  FAILED: {e}")
            results.append({"model": sm["displayName"], "modes": "?", "definition": "-", "bound": "-",
                            "refresh": "-", "ok": False})

    # Summary
    print("\n" + "=" * 100)
    print(f"{'Model':35} {'Mode':38} {'Definition':11} {'Bind':15} {'Refresh':15}")
    for r in results:
        print(f"{r['model'][:34]:35} {r['modes'][:37]:38} {r['definition']:11} {r['bound']:15} {r['refresh']:15}")
    print("=" * 100)

    if any(not r["ok"] for r in results):
        print("\nSome semantic models failed. Check logs above.")
        sys.exit(1)
    else:
        print("\nAll semantic models repaired successfully.")


if __name__ == "__main__":
    main()
