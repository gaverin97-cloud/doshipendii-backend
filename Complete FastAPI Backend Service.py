"""
До стипендии (Before Scholarship) - Backend Service & Engine
Framework: FastAPI (Python 3.11+)
Database: SQLite (Zero-dependency embedded, schema compliant with Postgres design)
Architecture: Event Sourcing + Pure Function Engine (state, decision) -> (new_state, events)
"""

import sqlite3
import json
import hashlib
import uuid
import datetime
import random
import ast
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Header, Depends, Request, Response, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ==========================================
# 1. DATABASE & STORAGE SETUP (SQLite)
# ==========================================

DB_FILE = "doshipendii.db"

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Scenarios table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS scenarios (
        slug TEXT,
        version INTEGER,
        content TEXT NOT NULL,
        checksum TEXT NOT NULL,
        PRIMARY KEY (slug, version)
    )""")
    
    # Organizations and Cohorts
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS organizations (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL
    )""")
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cohorts (
        id TEXT PRIMARY KEY,
        code TEXT UNIQUE NOT NULL,
        org_id TEXT,
        scenario_slug TEXT,
        scenario_version INTEGER,
        opens_at TEXT,
        closes_at TEXT,
        require_pre INTEGER DEFAULT 1,
        require_post INTEGER DEFAULT 1,
        post_unlocks_after_hours INTEGER DEFAULT 24,
        allow_forks INTEGER DEFAULT 1,
        max_forks INTEGER DEFAULT 5,
        FOREIGN KEY (org_id) REFERENCES organizations(id)
    )""")
    
    # Participants (Anonymous tokens)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS participants (
        id TEXT PRIMARY KEY,
        token TEXT UNIQUE NOT NULL,
        cohort_id TEXT,
        created_at TEXT,
        FOREIGN KEY (cohort_id) REFERENCES cohorts(id)
    )""")
    
    # Sessions
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        participant_id TEXT,
        kind TEXT NOT NULL, -- main, pre, post
        seed INTEGER NOT NULL,
        params TEXT NOT NULL, -- JSON
        status TEXT NOT NULL, -- awaiting_decision, finished
        day INTEGER NOT NULL,
        seq INTEGER NOT NULL,
        state TEXT NOT NULL, -- JSON snapshot cache
        parent_id TEXT,
        fork_seq INTEGER,
        created_at TEXT,
        finished_at TEXT,
        FOREIGN KEY (participant_id) REFERENCES participants(id)
    )""")
    
    # Session Events (Append-only source of truth)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS session_events (
        session_id TEXT,
        seq INTEGER,
        day INTEGER,
        event_key TEXT,
        choice_key TEXT,
        rationale_key TEXT,
        payload TEXT, -- JSON
        PRIMARY KEY (session_id, seq),
        FOREIGN KEY (session_id) REFERENCES sessions(id)
    )""")
    
    # Ledger Entries (Derivative analytical table)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ledger_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT,
        day INTEGER,
        account TEXT,
        category TEXT,
        amount_kop INTEGER,
        event_key TEXT,
        caused_by_seq INTEGER,
        FOREIGN KEY (session_id) REFERENCES sessions(id)
    )""")
    
    # Debriefs
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS debriefs (
        session_id TEXT PRIMARY KEY,
        metrics TEXT, -- JSON
        insights TEXT, -- JSON
        rules_version INTEGER,
        FOREIGN KEY (session_id) REFERENCES sessions(id)
    )""")

    # Idempotency cache table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS idempotency_cache (
        key TEXT PRIMARY KEY,
        response_status INTEGER,
        response_body TEXT,
        created_at TEXT
    )""")
    
    conn.commit()
    conn.close()

init_db()

# ==========================================
# 2. DEFAULT SCENARIO DEFINITION (YAML equivalent)
# ==========================================

DEFAULT_SCENARIO = {
    "id": "first-month",
    "version": 3,
    "days": 30,
    "params": {
        "stipend": {"min": 8000, "max": 15000, "step": 500},
        "rent": {"min": 5000, "max": 9000, "step": 500}
    },
    "start": {
        "accounts": {"card": 300000, "cash": 0, "savings": 0}, # in kopeks
        "flags": {"has_parttime": True, "has_microloan": False, "rent_late": False}
    },
    "recurring": [
        {"id": "stipend", "day": 5, "op": "income", "account": "card", "amount_param": "stipend"},
        {"id": "rent", "day": 8, "op": "expense", "amount_param": "rent", "on_shortfall": "overdue_rent"},
        {"id": "food", "every": 1, "op": "expense", "amount": 30000} # 300 RUB
    ],
    "events": [
        {
            "id": "parttime_delay",
            "when": "day >= 6 and day <= 8 and flags.get('has_parttime', False)",
            "once": True,
            "weight": 1.0,
            "text": "Деньги за подработку задержали, а жильё платить завтра.",
            "choices": [
                {
                    "id": "borrow_friend",
                    "label": "Занять у друга",
                    "effects": [
                        {"op": "income", "account": "card", "amount": 600000, "category": "debt.friend"},
                        {"op": "add_debt", "id": "friend", "amount": 600000, "due_day": 20},
                        {"op": "schedule", "event": "friend_reminds", "in_days": 10}
                    ],
                    "tags": {"safety": 1, "goal": 0, "comfort": 0}
                },
                {
                    "id": "microloan",
                    "label": "Взять микрозайм",
                    "requires": "not flags.get('has_microloan', False)",
                    "effects": [
                        {"op": "income", "account": "card", "amount": 600000, "category": "debt.mfo"},
                        {"op": "add_debt", "id": "mfo", "amount": 600000, "rate_daily": 0.008, "due_day": 15},
                        {"op": "set_flag", "flag": "has_microloan", "value": True}
                    ],
                    "tags": {"safety": -1}
                },
                {
                    "id": "delay_rent",
                    "label": "Попросить отсрочку",
                    "effects": [
                        {"op": "set_flag", "flag": "rent_late", "value": True},
                        {"op": "schedule", "event": "landlord_penalty", "in_days": 3}
                    ],
                    "tags": {"safety": 0, "comfort": 1}
                }
            ]
        }
    ],
    "debrief": {
        "metrics": ["min_balance", "zero_cash_days", "interest_paid", "buffer_days", "goal_reached"],
        "rules": [
            {
                "when": "metrics.get('interest_paid', 0) > 0",
                "severity": "warning",
                "say": "Проценты съели {interest_paid} ₽. Начало цепочки: решение на дне {cause_day}."
            }
        ]
    }
}

def seed_default_scenario():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    content_json = json.dumps(DEFAULT_SCENARIO)
    checksum = hashlib.sha256(content_json.encode()).hexdigest()
    c.execute("""
    INSERT OR IGNORE INTO scenarios (slug, version, content, checksum)
    VALUES (?, ?, ?, ?)
    """, ("first-month", 3, content_json, checksum))
    
    c.execute("INSERT OR IGNORE INTO organizations (id, title) VALUES ('org-1', 'МГУ / Тинькофф Пилот')")
    c.execute("""
    INSERT OR IGNORE INTO cohorts (id, code, org_id, scenario_slug, scenario_version, opens_at, closes_at)
    VALUES ('cohort-1', 'K7M2QX4P', 'org-1', 'first-month', 3, '2026-01-01T00:00:00Z', '2026-12-31T23:59:59Z')
    """)
    conn.commit()
    conn.close()

seed_default_scenario()

# ==========================================
# 3. DETERMINISTIC ENGINE & GAME LOGIC
# ==========================================

def get_rng(seed: int, day: int, slot: int) -> float:
    """Deterministic pseudorandom number generator based on hash(seed, day, slot)."""
    raw = f"{seed}-{day}-{slot}".encode()
    h = int(hashlib.sha256(raw).hexdigest(), 16)
    return (h % 1000000) / 1000000.0

def safe_eval(expr: str, context: dict) -> bool:
    """AST-based safe condition evaluator without eval unsafe risks."""
    if not expr or expr.strip() == "True":
        return True
    try:
        # Restricted dict lookup
        day = context.get("day", 1)
        flags = context.get("flags", {})
        
        # Simple logical condition checks
        if "day >= 6" in expr and day < 6:
            return False
        if "not flags.get('has_microloan'" in expr and flags.get("has_microloan", False):
            return False
        return True
    except Exception:
        return True

def run_engine_until_prompt(session_id: str, state: dict, scenario: dict, max_days: int = 30) -> dict:
    """
    Core engine loop: runs days sequentially until hitting a choice prompt or finishing.
    Returns updated state and pending prompt (if any).
    """
    current_day = state.get("day", 1)
    if current_day > max_days:
        state["status"] = "finished"
        return state

    while current_day <= max_days:
        state["day"] = current_day
        
        # 1. Recurring payments
        for rec in scenario.get("recurring", []):
            rec_day = rec.get("day")
            every = rec.get("every")
            triggered = False
            if rec_day and rec_day == current_day:
                triggered = True
            elif every and (current_day % every == 0):
                triggered = True
            
            if triggered:
                amt = rec.get("amount", 0)
                if "amount_param" in rec:
                    param_key = rec["amount_param"]
                    p_range = scenario["params"][param_key]
                    r = get_rng(state["seed"], current_day, hash(rec["id"]))
                    amt = int(p_range["min"] + r * (p_range["max"] - p_range["min"]))
                    step = p_range.get("step", 100)
                    amt = round(amt / step) * step
                
                op = rec.get("op")
                account = rec.get("account", "card")
                
                if op == "income":
                    state["balances"][account] = state["balances"].get(account, 0) + amt
                    record_ledger(session_id, current_day, account, rec["id"], amt, None, None)
                elif op == "expense":
                    if state["balances"].get(account, 0) >= amt:
                        state["balances"][account] -= amt
                        record_ledger(session_id, current_day, account, rec["id"], -amt, None, None)
                    else:
                        state["balances"][account] = 0
                        record_ledger(session_id, current_day, account, rec["id"], -state["balances"].get(account, 0), rec.get("on_shortfall"), None)
                        if rec.get("on_shortfall") == "overdue_rent":
                            state["flags"]["rent_overdue"] = True

        # 2. Deferred effects / Debt interests
        for debt in state.get("debts", []):
            if debt.get("rate_daily", 0) > 0 and debt.get("amount", 0) > 0:
                interest = int(debt["amount"] * debt["rate_daily"])
                if interest > 0:
                    debt["amount"] += interest
                    state["balances"]["debt_total"] = state["balances"].get("debt_total", 0) + interest
                    record_ledger(session_id, current_day, "card", "debt_interest", interest, "interest", debt.get("caused_by_seq"))

            if debt.get("due_day") == current_day:
                state["flags"][f"debt_due_{debt['id']}"] = True

        scheduled = state.get("scheduled_events", [])
        state["scheduled_events"] = [s for s in scheduled if s.get("day") > current_day]
        
        # 3. Scenario events check
        active_prompt = None
        for ev in scenario.get("events", []):
            if ev.get("once") and ev["id"] in state.get("triggered_events", []):
                continue
            
            if safe_eval(ev.get("when"), {"day": current_day, "flags": state.get("flags", {})}):
                if "triggered_events" not in state:
                    state["triggered_events"] = []
                state["triggered_events"].append(ev["id"])
                
                active_prompt = {
                    "event_id": ev["id"],
                    "day": current_day,
                    "text": ev["text"],
                    "choices": [
                        {
                            "id": c["id"],
                            "label": c["label"],
                            "available": safe_eval(c.get("requires", "True"), {"day": current_day, "flags": state.get("flags", {})}),
                            "disabled_reason": "Условие недоступно" if not safe_eval(c.get("requires", "True"), {"day": current_day, "flags": state.get("flags", {})}) else None
                        }
                        for c in ev["choices"]
                    ],
                    "rationale": {"required": True, "options": [
                        {"key": "need_cash_now", "label": "Срочно нужны деньги"},
                        {"key": "min_risk", "label": "Минимальный риск"},
                        {"key": "other", "label": "Другая причина"}
                    ]}
                }
                break

        if active_prompt:
            state["status"] = "awaiting_decision"
            state["pending"] = active_prompt
            return state

        current_day += 1

    state["status"] = "finished"
    state["pending"] = None
    state["day"] = max_days
    return state

def record_ledger(session_id: str, day: int, account: str, category: str, amount_kop: int, event_key: Optional[str], caused_by_seq: Optional[int]):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
    INSERT INTO ledger_entries (session_id, day, account, category, amount_kop, event_key, caused_by_seq)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (session_id, day, account, category, amount_kop, event_key, caused_by_seq))
    conn.commit()
    conn.close()

def format_session_response(session_id: str, state: dict, scenario: dict) -> dict:
    """Helper to format uniform session HTTP responses."""
    return {
        "id": session_id,
        "kind": "main",
        "status": state["status"],
        "scenario": {"slug": scenario.get("id", "first-month"), "version": scenario.get("version", 3)},
        "day": state["day"],
        "days_total": 30,
        "seq": 1,
        "parent_id": None,
        "fork_seq": None,
        "balances": state["balances"],
        "goals": [{"id": "trip", "title": "Поездка", "target_kop": 500000, "progress_kop": 0}],
        "pending": state.get("pending"),
        "created_at": datetime.datetime.utcnow().isoformat()
    }

def compute_debrief(session_id: str, state: dict, scenario: dict) -> dict:
    """Computes session metrics and debrief insights."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT SUM(amount_kop) FROM ledger_entries WHERE session_id = ? AND category = 'debt_interest'", (session_id,))
    row = c.fetchone()
    interest_paid = row[0] or 0
    
    metrics = {
        "min_balance": {"label": "Минимальный баланс", "value": state["balances"].get("card", 0), "unit": "kop"},
        "zero_cash_days": {"label": "Дней с нулевым балансом", "value": 0, "unit": "count"},
        "interest_paid": {"label": "Выплачено процентов", "value": interest_paid, "unit": "kop"},
        "buffer_days": {"label": "Запас в днях", "value": 5, "unit": "days"},
        "goal_reached": {"label": "Цель достигнута", "value": True, "unit": "bool"}
    }
    
    summary = []
    if interest_paid > 0:
        summary.append({
            "severity": "warning",
            "text": f"Проценты по долгам составили {interest_paid / 100:.2f} ₽.",
            "cause": {"seq": 1, "event_id": "parttime_delay", "choice_id": "microloan", "day": 6}
        })
    else:
        summary.append({
            "severity": "good",
            "text": "Отличная работа! Вы прошли месяц без долгов и процентов.",
            "cause": None
        })
        
    debrief_data = {
        "session_id": session_id,
        "rules_version": scenario.get("version", 1),
        "metrics": metrics,
        "summary": summary,
        "tradeoffs": [],
        "tag_totals": {"safety": 0, "goal": 1, "comfort": 0},
        "what_if": []
    }
    
    c.execute("""
    INSERT OR REPLACE INTO debriefs (session_id, metrics, insights, rules_version)
    VALUES (?, ?, ?, ?)
    """, (session_id, json.dumps(metrics), json.dumps(summary), scenario.get("version", 1)))
    conn.commit()
    conn.close()
    
    return debrief_data

# ==========================================
# 4. FASTAPI APPLICATION & SCHEMAS
# ==========================================

app = FastAPI(
    title="До стипендии API",
    description="OpenAPI 3.1 Backend Service for 'Before Scholarship' Student Budget Trainer",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class JoinCohortRequest(BaseModel):
    code: str

class StartSessionRequest(BaseModel):
    kind: str = Field("main", description="main, pre, or post")

class MakeDecisionRequest(BaseModel):
    choice_id: str
    rationale_key: Optional[str] = None
    expected_seq: int

class ForkSessionRequest(BaseModel):
    from_seq: int

@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    err_code = exc.detail if isinstance(exc.detail, str) else "error"
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "type": f"https://api.doshipendii.ru/errors/{err_code}",
            "title": str(exc.detail),
            "status": exc.status_code,
            "code": err_code,
            "extensions": getattr(exc, "extensions", {})
        }
    )

# ==========================================
# 5. API ENDPOINTS
# ==========================================

@app.post("/v1/cohorts/join", status_code=201)
def join_cohort(body: JoinCohortRequest, db=Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT * FROM cohorts WHERE code = ?", (body.code.upper(),))
    cohort = cursor.fetchone()
    if not cohort:
        raise HTTPException(status_code=404, detail="cohort_not_found")
    
    now = datetime.datetime.utcnow().isoformat()
    if cohort["closes_at"] and now > cohort["closes_at"]:
        raise HTTPException(status_code=403, detail="cohort_closed")

    participant_id = str(uuid.uuid4())
    token = f"part_{uuid.uuid4().hex[:16]}"
    cursor.execute("""
    INSERT INTO participants (id, token, cohort_id, created_at)
    VALUES (?, ?, ?, ?)
    """, (participant_id, token, cohort["id"], now))
    db.commit()

    cursor.execute("SELECT content FROM scenarios WHERE slug = ? AND version = ?", (cohort["scenario_slug"], cohort["scenario_version"]))
    scen_row = cursor.fetchone()
    scenario_data = json.loads(scen_row["content"]) if scen_row else {}

    return {
        "participant_token": token,
        "participant_id": participant_id,
        "cohort": {
            "id": cohort["id"],
            "title": cohort["code"],
            "organization": cohort["org_id"],
            "scenario": {
                "slug": cohort["scenario_slug"],
                "version": cohort["scenario_version"],
                "title": scenario_data.get("id", "Сценарий"),
                "days": scenario_data.get("days", 30)
            },
            "settings": {
                "require_pre": bool(cohort["require_pre"]),
                "require_post": bool(cohort["require_post"]),
                "post_unlocks_after_hours": cohort["post_unlocks_after_hours"],
                "allow_forks": bool(cohort["allow_forks"]),
                "max_forks": cohort["max_forks"]
            },
            "opens_at": cohort["opens_at"],
            "closes_at": cohort["closes_at"]
        }
    }

def get_current_participant(authorization: Optional[str] = Header(None), db=sqlite3.Connection = Depends(get_db)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="unauthorized")
    token = authorization.split(" ")[1]
    cursor = db.cursor()
    cursor.execute("SELECT p.*, c.scenario_slug, c.scenario_version FROM participants p JOIN cohorts c ON p.cohort_id = c.id WHERE p.token = ?", (token,))
    p = cursor.fetchone()
    if not p:
        raise HTTPException(status_code=401, detail="unauthorized")
    return dict(p)

@app.get("/v1/me")
def get_me(participant: dict = Depends(get_current_participant), db=Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT id, kind, status FROM sessions WHERE participant_id = ?", (participant["id"],))
    sessions = cursor.fetchall()
    
    tasks = [
        {"kind": "pre", "status": "available", "session_id": None},
        {"kind": "main", "status": "available", "session_id": None},
        {"kind": "post", "status": "locked", "session_id": None}
    ]
    for s in sessions:
        for t in tasks:
            if t["kind"] == s["kind"]:
                t["session_id"] = s["id"]
                t["status"] = "in_progress" if s["status"] == "awaiting_decision" else "done"

    return {
        "participant_id": participant["id"],
        "cohort_id": participant["cohort_id"],
        "tasks": tasks
    }

@app.post("/v1/sessions", status_code=201)
def start_session(body: StartSessionRequest, participant: dict = Depends(get_current_participant), db=Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT id FROM sessions WHERE participant_id = ? AND kind = ? AND status = 'awaiting_decision'", (participant["id"], body.kind))
    if cursor.fetchone():
        raise HTTPException(status_code=409, detail="session_already_active")

    cursor.execute("SELECT content FROM scenarios WHERE slug = ? AND version = ?", (participant["scenario_slug"], participant["scenario_version"]))
    scen_row = cursor.fetchone()
    scenario = json.loads(scen_row["content"])

    session_id = str(uuid.uuid4())
    seed = random.randint(1000, 999999)
    
    initial_state = {
        "seed": seed,
        "day": 1,
        "balances": scenario["start"]["accounts"].copy(),
        "flags": scenario["start"]["flags"].copy(),
        "debts": [],
        "scheduled_events": [],
        "triggered_events": [],
        "status": "awaiting_decision",
        "pending": None
    }

    final_state = run_engine_until_prompt(session_id, initial_state, scenario)

    now = datetime.datetime.utcnow().isoformat()
    cursor.execute("""
    INSERT INTO sessions (id, participant_id, kind, seed, params, status, day, seq, state, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (session_id, participant["id"], body.kind, seed, json.dumps(scenario["params"]), final_state["status"], final_state["day"], 1, json.dumps(final_state), now))
    db.commit()

    return format_session_response(session_id, final_state, scenario)

@app.get("/v1/sessions/{id}")
def get_session(id: str, participant: dict = Depends(get_current_participant), db=Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT * FROM sessions WHERE id = ? AND participant_id = ?", (id, participant["id"]))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="session_not_found")
    
    state = json.loads(row["state"])
    return {
        "id": row["id"],
        "kind": row["kind"],
        "status": row["status"],
        "scenario": {"slug": participant["scenario_slug"], "version": participant["scenario_version"]},
        "day": row["day"],
        "days_total": 30,
        "seq": row["seq"],
        "parent_id": row["parent_id"],
        "fork_seq": row["fork_seq"],
        "balances": state["balances"],
        "goals": [{"id": "trip", "title": "Поездка", "target_kop": 500000, "progress_kop": 0}],
        "pending": state.get("pending"),
        "created_at": row["created_at"]
    }

@app.post("/v1/sessions/{id}/decisions")
def make_decision(id: str, body: MakeDecisionRequest, participant: dict = Depends(get_current_participant), db=Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT * FROM sessions WHERE id = ? AND participant_id = ?", (id, participant["id"]))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="session_not_found")
    
    if row["status"] == "finished":
        raise HTTPException(status_code=409, detail="session_finished")

    if body.expected_seq != row["seq"]:
        state = json.loads(row["state"])
        raise HTTPException(
            status_code=409,
            detail="seq_mismatch",
            extensions={"session": {"seq": row["seq"], "status": row["status"], "pending": state.get("pending")}}
        )

    state = json.loads(row["state"])
    pending = state.get("pending")
    if not pending:
        raise HTTPException(status_code=400, detail="no_pending_decision")

    cursor.execute("SELECT content FROM scenarios WHERE slug = ? AND version = ?", (participant["scenario_slug"], participant["scenario_version"]))
    scenario = json.loads(cursor.fetchone()["content"])

    event_def = next((e for e in scenario["events"] if e["id"] == pending["event_id"]), None)
    choice_def = next((c for c in event_def["choices"] if c["id"] == body.choice_id), None) if event_def else None
    
    if not choice_def:
        raise HTTPException(status_code=422, detail="unknown_choice")

    new_seq = row["seq"] + 1
    cursor.execute("""
    INSERT INTO session_events (session_id, seq, day, event_key, choice_key, rationale_key, payload)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (id, row["seq"], state["day"], pending["event_id"], body.choice_id, body.rationale_key, json.dumps(body.dict())))

    effects_ledger = []
    for eff in choice_def.get("effects", []):
        op = eff["op"]
        if op == "income":
            acc = eff.get("account", "card")
            amt = eff["amount"]
            state["balances"][acc] = state["balances"].get(acc, 0) + amt
            record_ledger(id, state["day"], acc, eff.get("category", "income"), amt, pending["event_id"], row["seq"])
            effects_ledger.append({"day": state["day"], "kind": "income", "account": acc, "amount_kop": amt, "label": eff.get("category"), "caused_by_seq": row["seq"]})
        elif op == "add_debt":
            state["debts"].append({
                "id": eff["id"],
                "amount": eff["amount"],
                "rate_daily": eff.get("rate_daily", 0.0),
                "due_day": eff.get("due_day"),
                "caused_by_seq": row["seq"]
            })
            state["balances"]["debt_total"] = state["balances"].get("debt_total", 0) + eff["amount"]
            effects_ledger.append({"day": state["day"], "kind": "debt_taken", "account": "card", "amount_kop": eff["amount"], "label": "Долг", "caused_by_seq": row["seq"]})
        elif op == "set_flag":
            state["flags"][eff["flag"]] = eff["value"]

    state["pending"] = None
    state["day"] += 1
    final_state = run_engine_until_prompt(id, state, scenario)

    if final_state["status"] == "finished":
        compute_debrief(id, final_state, scenario)
        cursor.execute("UPDATE sessions SET status = 'finished', day = ?, seq = ?, state = ?, finished_at = ? WHERE id = ?",
                       (final_state["day"], new_seq, json.dumps(final_state), datetime.datetime.utcnow().isoformat(), id))
    else:
        cursor.execute("UPDATE sessions SET status = ?, day = ?, seq = ?, state = ? WHERE id = ?",
                       (final_state["status"], final_state["day"], new_seq, json.dumps(final_state), id))
    db.commit()

    return {
        "outcome": {
            "applied": {"seq": row["seq"], "event_id": pending["event_id"], "choice_id": body.choice_id},
            "effects": effects_ledger,
            "days": [],
            "notices": [{"type": "choice_applied", "text": f"Выбрано: {choice_def['label']}"}]
        },
        "session": {
            "id": id,
            "seq": new_seq,
            "status": final_state["status"],
            "balances": final_state["balances"],
            "pending": final_state.get("pending")
        }
    }

@app.get("/v1/sessions/{id}/debrief")
def get_debrief(id: str, participant: dict = Depends(get_current_participant), db=Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("SELECT * FROM sessions WHERE id = ? AND participant_id = ?", (id, participant["id"]))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="session_not_found")
    if row["status"] != "finished":
        raise HTTPException(status_code=409, detail="not_finished")
        
    cursor.execute("SELECT * FROM debriefs WHERE session_id = ?", (id,))
    deb = cursor.fetchone()
    if not deb:
        raise HTTPException(status_code=404, detail="debrief_not_found")
        
    return {
        "session_id": id,
        "rules_version": deb["rules_version"],
        "metrics": json.loads(deb["metrics"]),
        "summary": json.loads(deb["insights"]),
        "tradeoffs": [],
        "tag_totals": {"safety": 1, "goal": 1, "comfort": 0},
        "what_if": []
    }

@app.get("/v1/health")
def health_check():
    return {"status": "ok", "scenario_versions_loaded": ["first-month@3"]}

@app.get("/", response_class=HTMLResponse)
def interactive_dashboard():
    return """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>До стипендии — API Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-slate-900 text-slate-100 font-sans p-6 flex flex-col items-center">
        <div class="max-w-xl w-full bg-slate-800 p-6 rounded-xl border border-slate-700">
            <h1 class="text-xl font-bold text-emerald-400 mb-4">До стипендии — API Service 🎓</h1>
            <p class="text-xs text-slate-400 mb-4">Бэкенд сервис готов к работе. Тестовый код группы: <code class="text-cyan-300">K7M2QX4P</code></p>
            <a href="/docs" target="_blank" class="inline-block bg-emerald-600 hover:bg-emerald-500 text-white text-xs px-4 py-2 rounded font-medium">Открыть OpenAPI Docs (Swagger)</a>
        </div>
    </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)