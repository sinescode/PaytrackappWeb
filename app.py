"""
PayTrack Web — Flask + SQLAlchemy ORM (PostgreSQL backend)

SQLAlchemy Models (replace Flutter SharedPreferences/file storage):
  TierDefinition  — named pay tiers (min_ok, max_ok, price_per_ok)
  UserTier        — assigns a user to a specific tier (many per user)
  CustomName      — maps user_id → display name
  CsvEntry        — one row per imported CSV record
  Balance         — payment adjustment per user

Run:
    pip install flask sqlalchemy psycopg2-binary python-dotenv
    python run.py
    → http://localhost:5000

.env file:
    DATABASE_URL=postgresql://user:password@localhost:5432/paytrack
    TELEGRAM_BOT_TOKEN=your_token_here
    PORT=5000
"""

import csv
import io
import json
import os
from datetime import datetime

import queue
import threading

from dotenv import load_dotenv

from flask import (Flask, redirect, render_template, request,
                   jsonify, url_for, flash, Response, stream_with_context)

from sqlalchemy import (create_engine, Column, Integer, String,
                        Float, ForeignKey, func)
from sqlalchemy.orm import (DeclarativeBase, relationship,
                            sessionmaker, scoped_session)

# ── Load .env ─────────────────────────────────────────────────────────────────

load_dotenv()

# ── App & DB setup ────────────────────────────────────────────────────────────

app = Flask(__name__)
app.secret_key = "paytrack-secret-2024"

DATABASE_URL        = os.environ["DATABASE_URL"]          # required — fails fast if missing
TELEGRAM_BOT_TOKEN  = os.environ["TELEGRAM_BOT_TOKEN"]   # required
PORT                = int(os.getenv("PORT", 5000))        # optional, default 5000

engine         = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionFactory = scoped_session(sessionmaker(bind=engine))


# ── SQLAlchemy models ─────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class TierDefinition(Base):
    """Named pay tier — mirrors Flutter TierDefinition model."""
    __tablename__ = "tier_definitions"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    name         = Column(String,  nullable=False)
    min_ok       = Column(Integer, nullable=False)
    max_ok       = Column(Integer, nullable=False)
    price_per_ok = Column(Float,   nullable=False)

    user_tiers   = relationship("UserTier", back_populates="tier_definition",
                                cascade="all, delete-orphan")

    def __repr__(self):
        return f"<TierDefinition {self.name} {self.min_ok}-{self.max_ok} @{self.price_per_ok}>"


class UserTier(Base):
    """Assigns a user to a specific tier — mirrors Flutter AppConfig.userTiers."""
    __tablename__ = "user_tiers"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    user_id         = Column(String,  nullable=False, index=True)
    tier_def_id     = Column(Integer, ForeignKey("tier_definitions.id"), nullable=False)

    tier_definition = relationship("TierDefinition", back_populates="user_tiers")

    def __repr__(self):
        return f"<UserTier user={self.user_id} tier={self.tier_def_id}>"


class CustomName(Base):
    """Custom display name — mirrors Flutter AppConfig.customNames."""
    __tablename__ = "custom_names"

    user_id      = Column(String, primary_key=True)
    display_name = Column(String, nullable=False)

    def __repr__(self):
        return f"<CustomName {self.user_id} → {self.display_name}>"


class CsvEntry(Base):
    """One imported CSV row — mirrors Flutter CsvEntry model."""
    __tablename__ = "csv_entries"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    filename     = Column(String,  nullable=False, index=True)
    user_id      = Column(String,  nullable=False, index=True)
    username     = Column(String)
    ok_count     = Column(Integer, nullable=False)
    price_per_ok = Column(Float,   nullable=False, default=0.0)
    total        = Column(Float,   nullable=False, default=0.0)
    bkash        = Column(String,  default="Not Provided")
    rocket       = Column(String,  default="Not Provided")
    paid_status  = Column(String,  default="")

    @property
    def date(self):
        """Mirrors Flutter CsvEntry.date getter."""
        return self.filename.replace(".csv", "")

    def __repr__(self):
        return f"<CsvEntry {self.filename} user={self.user_id} ok={self.ok_count}>"


class Balance(Base):
    """Payment adjustment — mirrors Flutter StorageService balances map."""
    __tablename__ = "balances"

    user_id = Column(String, primary_key=True)
    amount  = Column(Float,  nullable=False, default=0.0)

    def __repr__(self):
        return f"<Balance {self.user_id} {self.amount}>"


def init_db():
    Base.metadata.create_all(engine)


# ── Session helper ────────────────────────────────────────────────────────────

def get_session():
    return SessionFactory()


@app.teardown_appcontext
def remove_session(exc=None):
    SessionFactory.remove()


# ── Business logic ────────────────────────────────────────────────────────────

def calculate_total(ok_count, tiers):
    """Mirrors DataService.calculateTotal."""
    for tier in tiers:
        if tier.min_ok <= ok_count <= tier.max_ok:
            return tier.price_per_ok, ok_count * tier.price_per_ok
    return 0.0, 0.0


def get_user_tiers(session, user_id):
    return (
        session.query(TierDefinition)
        .join(UserTier, UserTier.tier_def_id == TierDefinition.id)
        .filter(UserTier.user_id == user_id)
        .order_by(TierDefinition.min_ok)
        .all()
    )


def get_global_tiers(session):
    return session.query(TierDefinition).order_by(TierDefinition.min_ok).all()


def get_display_name(session, user_id):
    row = session.query(CustomName).filter_by(user_id=user_id).first()
    return row.display_name if row else user_id


def get_balance(session, user_id):
    row = session.query(Balance).filter_by(user_id=user_id).first()
    return row.amount if row else 0.0


def get_net_pending(session, user_id):
    """Mirrors DataService.getNetPending."""
    entries_total = (
        session.query(func.coalesce(func.sum(CsvEntry.total), 0.0))
        .filter(CsvEntry.user_id == user_id)
        .scalar()
    )
    return entries_total + get_balance(session, user_id)


def build_user_summaries(session):
    """Mirrors DataService.buildUserSummaries — sorted by pending desc."""
    entry_ids   = {r.user_id for r in session.query(CsvEntry.user_id).distinct()}
    balance_ids = {r.user_id for r in
                   session.query(Balance.user_id).filter(Balance.amount != 0)}
    summaries = [
        {"user_id": uid,
         "display_name": get_display_name(session, uid),
         "pending": get_net_pending(session, uid)}
        for uid in (entry_ids | balance_ids)
    ]
    summaries.sort(key=lambda x: x["pending"], reverse=True)
    return summaries


def parse_csv_stream(stream, filename, session):
    """Updated logic:
       - If user has UserTier assignment → use tier price (ignore CSV Rate)
       - Else → use Rate from CSV (fallback to global tiers only if Rate missing/invalid)
    """
    global_tiers = get_global_tiers(session)
    content      = stream.read().decode("utf-8-sig", errors="replace")
    reader       = csv.DictReader(io.StringIO(content))
    rows = []
    for raw in reader:
        row         = {k.strip(): (v.strip() if v else "") for k, v in raw.items()}
        user_id     = row.get("User ID", "")
        username    = row.get("Username", "")
        ok_str      = row.get("OK Count", "")
        rate_str    = row.get("Rate", "").strip()
        bkash       = row.get("Bkash", "") or "Not Provided"
        rocket      = row.get("Rocket", "") or "Not Provided"
        paid_status = row.get("Paid Status", "").lower()

        try:
            ok_count = int(ok_str)
        except ValueError:
            continue
        if ok_count < 0:
            continue

        # ── NEW PRIORITY LOGIC ─────────────────────────────────────
        user_tiers = get_user_tiers(session, user_id)   # personal assignment?

        if user_tiers:                                      # ← Has assignment
            # Use the user's personal tier (3.8 etc.)
            price_per_ok, total = calculate_total(ok_count, user_tiers)
        else:                                               # ← No assignment
            # Use CSV Rate column (your 3.5)
            price_per_ok = 0.0
            total        = 0.0
            if rate_str:
                try:
                    rate = float(rate_str)
                    if rate > 0:
                        price_per_ok = rate
                        total        = ok_count * rate
                except ValueError:
                    pass

            # Only if CSV Rate was missing/invalid → fallback to global tiers
            if price_per_ok == 0.0:
                price_per_ok, total = calculate_total(ok_count, global_tiers)

        # Skip row if we still couldn't determine a price
        if price_per_ok == 0.0:
            continue

        rows.append({
            "filename":     filename,
            "user_id":      user_id,
            "username":     username,
            "ok_count":     ok_count,
            "price_per_ok": price_per_ok,
            "total":        total,
            "bkash":        bkash,
            "rocket":       rocket,
            "paid_status":  paid_status
        })
    return rows
# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def overview():
    session       = get_session()
    users         = build_user_summaries(session)
    total_pending = sum(u["pending"] for u in users)
    return render_template("overview.html", users=users,
                           total_pending=total_pending,
                           today=datetime.today().strftime("%Y-%m-%d"))


@app.route("/user/<user_id>")
def user_detail(user_id):
    session      = get_session()
    display_name = get_display_name(session, user_id)
    pending      = get_net_pending(session, user_id)
    balance      = get_balance(session, user_id)
    entries      = (session.query(CsvEntry)
                    .filter(CsvEntry.user_id == user_id)
                    .order_by(CsvEntry.filename).all())
    return render_template("user_detail.html", user_id=user_id,
                           display_name=display_name, pending=pending,
                           balance=balance, entries=entries,
                           today=datetime.today().strftime("%Y-%m-%d"))


@app.route("/user/<user_id>/transaction", methods=["POST"])
def apply_transaction(user_id):
    session = get_session()
    try:
        amount = float(request.form["amount"])
    except (KeyError, ValueError):
        flash("Invalid amount", "error")
        return redirect(url_for("user_detail", user_id=user_id))

    bal = session.query(Balance).filter_by(user_id=user_id).first()
    if bal:
        bal.amount -= amount
    else:
        session.add(Balance(user_id=user_id, amount=-amount))
    session.commit()
    flash(f"Transaction applied: ৳{amount:.2f}", "success")
    return redirect(url_for("user_detail", user_id=user_id))


@app.route("/upload", methods=["GET", "POST"])
def upload_csv():
    session = get_session()
    if request.method == "POST":
        files = request.files.getlist("csv_files")
        if not files or all(f.filename == "" for f in files):
            flash("No files selected", "error")
            return redirect(url_for("upload_csv"))

        imported, skipped = 0, []
        for f in files:
            if not f.filename.endswith(".csv"):
                skipped.append(f.filename)
                continue
            filename = os.path.basename(f.filename)
            session.query(CsvEntry).filter_by(filename=filename).delete()
            for r in parse_csv_stream(f.stream, filename, session):
                session.add(CsvEntry(**r))
                imported += 1
        session.commit()
        msg = f"Imported {imported} entries"
        if skipped:
            msg += f" (skipped: {', '.join(skipped)})"
        flash(msg, "success")
        return redirect(url_for("overview"))

    files_in_db = (session.query(CsvEntry.filename,
                                 func.count(CsvEntry.id).label("cnt"))
                   .group_by(CsvEntry.filename)
                   .order_by(CsvEntry.filename).all())
    return render_template("upload.html", files_in_db=files_in_db)


@app.route("/upload/delete/<filename>", methods=["POST"])
def delete_csv(filename):
    session = get_session()
    session.query(CsvEntry).filter_by(filename=filename).delete()
    session.commit()
    flash(f"Deleted entries for {filename}", "success")
    return redirect(url_for("upload_csv"))


@app.route("/settings")
def settings():
    session      = get_session()
    active_tab   = request.args.get("tab", "tiers")
    tier_defs    = session.query(TierDefinition).order_by(TierDefinition.min_ok).all()
    raw_assignments = (session.query(UserTier).join(TierDefinition)
                       .order_by(UserTier.user_id).all())
    # Build a quick user_id → display_name lookup
    name_map = {cn.user_id: cn.display_name
                for cn in session.query(CustomName).all()}
    # Serialize to dicts so the template can use a['tier_name'] etc.
    user_assignments = [
        {
            "id":           a.id,
            "user_id":      a.user_id,
            "display_name": name_map.get(a.user_id),   # None if no custom name
            "tier_def_id":  a.tier_def_id,
            "tier_name":    a.tier_definition.name,
            "min_ok":       a.tier_definition.min_ok,
            "max_ok":       a.tier_definition.max_ok,
            "price_per_ok": a.tier_definition.price_per_ok,
        }
        for a in raw_assignments
    ]
    custom_names = session.query(CustomName).order_by(CustomName.user_id).all()
    return render_template("settings.html", tier_defs=tier_defs,
                           user_assignments=user_assignments,
                           custom_names=custom_names,
                           active_tab=active_tab)


@app.route("/settings/tier/add", methods=["POST"])
def add_tier():
    session = get_session()
    try:
        session.add(TierDefinition(
            name=request.form["name"].strip(),
            min_ok=int(request.form["min_ok"]),
            max_ok=int(request.form["max_ok"]),
            price_per_ok=float(request.form["price_per_ok"]),
        ))
        session.commit()
        flash("Tier added", "success")
    except Exception as e:
        session.rollback()
        flash(f"Error: {e}", "error")
    return redirect(url_for("settings", tab="tiers"))


@app.route("/settings/tier/edit/<int:tier_id>", methods=["POST"])
def edit_tier(tier_id):
    session = get_session()
    tier = session.get(TierDefinition, tier_id)
    if not tier:
        flash("Tier not found", "error")
        return redirect(url_for("settings"))
    try:
        tier.name         = request.form["name"].strip()
        tier.min_ok       = int(request.form["min_ok"])
        tier.max_ok       = int(request.form["max_ok"])
        tier.price_per_ok = float(request.form["price_per_ok"])
        session.commit()
        flash("Tier updated", "success")
    except Exception as e:
        session.rollback()
        flash(f"Error: {e}", "error")
    return redirect(url_for("settings", tab="tiers"))


@app.route("/settings/tier/delete/<int:tier_id>", methods=["POST"])
def delete_tier(tier_id):
    session = get_session()
    tier = session.get(TierDefinition, tier_id)
    if tier:
        session.delete(tier)
        session.commit()
        flash("Tier deleted", "success")
    return redirect(url_for("settings", tab="tiers"))


@app.route("/settings/assignment/add", methods=["POST"])
def add_assignment():
    session     = get_session()
    user_id     = request.form["user_id"].strip()
    tier_def_id = int(request.form["tier_def_id"])
    if user_id:
        session.add(UserTier(user_id=user_id, tier_def_id=tier_def_id))
        session.commit()
        flash("Assignment added", "success")
    return redirect(url_for("settings", tab="assignments"))


@app.route("/settings/assignment/edit/<int:assign_id>", methods=["POST"])
def edit_assignment(assign_id):
    session     = get_session()
    ut          = session.get(UserTier, assign_id)
    if not ut:
        flash("Assignment not found", "error")
        return redirect(url_for("settings", tab="assignments"))
    user_id     = request.form.get("user_id", "").strip()
    tier_def_id = request.form.get("tier_def_id")
    if user_id and tier_def_id:
        ut.user_id     = user_id
        ut.tier_def_id = int(tier_def_id)
        session.commit()
        flash("Assignment updated", "success")
    return redirect(url_for("settings", tab="assignments"))


@app.route("/settings/assignment/delete/<int:assign_id>", methods=["POST"])
def delete_assignment(assign_id):
    session = get_session()
    ut = session.get(UserTier, assign_id)
    if ut:
        session.delete(ut)
        session.commit()
        flash("Assignment removed", "success")
    return redirect(url_for("settings", tab="assignments"))


@app.route("/settings/name/add", methods=["POST"])
def add_custom_name():
    session      = get_session()
    user_id      = request.form["user_id"].strip()
    display_name = request.form["display_name"].strip()
    if user_id and display_name:
        existing = session.query(CustomName).filter_by(user_id=user_id).first()
        if existing:
            existing.display_name = display_name
        else:
            session.add(CustomName(user_id=user_id, display_name=display_name))
        session.commit()
        flash("Name saved", "success")
    return redirect(url_for("settings", tab="names"))


@app.route("/settings/name/edit/<old_user_id>", methods=["POST"])
def edit_custom_name(old_user_id):
    session      = get_session()
    new_user_id  = request.form.get("user_id", "").strip()
    display_name = request.form.get("display_name", "").strip()
    if not new_user_id or not display_name:
        flash("Both fields required", "error")
        return redirect(url_for("settings", tab="names"))
    cn = session.query(CustomName).filter_by(user_id=old_user_id).first()
    if cn:
        if new_user_id != old_user_id:
            # user_id is the PK — delete old, insert new
            session.delete(cn)
            session.flush()
            session.add(CustomName(user_id=new_user_id, display_name=display_name))
        else:
            cn.display_name = display_name
        session.commit()
        flash("Name updated", "success")
    else:
        flash("Name not found", "error")
    return redirect(url_for("settings", tab="names"))


@app.route("/settings/name/delete/<user_id>", methods=["POST"])
def delete_custom_name(user_id):
    session = get_session()
    cn = session.query(CustomName).filter_by(user_id=user_id).first()
    if cn:
        session.delete(cn)
        session.commit()
        flash("Name removed", "success")
    return redirect(url_for("settings", tab="names"))


@app.route("/settings/export")
def export_config():
    """Mirrors StorageService.exportConfig."""
    session = get_session()
    payload = json.dumps({
        "config": {
            "tier_definitions": [
                {"id": t.id, "name": t.name, "min_ok": t.min_ok,
                 "max_ok": t.max_ok, "price_per_ok": t.price_per_ok}
                for t in session.query(TierDefinition).all()
            ],
            "user_tiers": {
                uid: [ut.tier_def_id for ut in uts]
                for uid, uts in
                __import__("itertools").groupby(
                    session.query(UserTier).order_by(UserTier.user_id).all(),
                    key=lambda x: x.user_id)
            },
            "custom_names": {cn.user_id: cn.display_name
                             for cn in session.query(CustomName).all()},
        },
        "balances": {b.user_id: b.amount
                     for b in session.query(Balance).all()},
    }, indent=2)
    return Response(payload, mimetype="application/json",
                    headers={"Content-Disposition":
                             "attachment; filename=paytrack_config.json"})


@app.route("/settings/import", methods=["POST"])
def import_config():
    """Mirrors StorageService.importConfigFromFile."""
    session = get_session()
    f = request.files.get("config_file")
    if not f:
        flash("No file selected", "error")
        return redirect(url_for("settings"))
    try:
        data = json.load(f)
        cfg  = data.get("config", {})

        # Always wipe dependent tables first to avoid FK conflicts
        session.query(UserTier).delete()
        session.query(TierDefinition).delete()
        session.flush()

        # Build a lookup: (min_ok, max_ok, price_per_ok) -> TierDefinition
        # so user_tiers objects can be matched to real DB rows.
        tier_lookup: dict[tuple, TierDefinition] = {}

        if "tier_definitions" in cfg:
            for t in cfg["tier_definitions"]:
                td = TierDefinition(
                    name         = t["name"],
                    min_ok       = int(t["min_ok"]),
                    max_ok       = int(t["max_ok"]),
                    price_per_ok = float(t["price_per_ok"]),
                )
                session.add(td)
                session.flush()   # get td.id assigned
                key = (td.min_ok, td.max_ok, td.price_per_ok)
                tier_lookup[key] = td

        if "user_tiers" in cfg:
            for uid, tier_list in cfg["user_tiers"].items():
                for entry in tier_list:
                    if isinstance(entry, int):
                        # New web-export format: entry is already a tier_def_id
                        session.add(UserTier(user_id=uid, tier_def_id=entry))
                    elif isinstance(entry, dict):
                        # Flutter export format: entry is {min_ok, max_ok, price_per_ok}
                        key = (
                            int(entry["min_ok"]),
                            int(entry["max_ok"]),
                            float(entry["price_per_ok"]),
                        )
                        if key in tier_lookup:
                            # Matched an existing TierDefinition exactly
                            session.add(UserTier(user_id=uid,
                                                 tier_def_id=tier_lookup[key].id))
                        else:
                            # No matching global tier — create a private one
                            td = TierDefinition(
                                name         = f"Imported ({entry['min_ok']}–{entry['max_ok']})",
                                min_ok       = int(entry["min_ok"]),
                                max_ok       = int(entry["max_ok"]),
                                price_per_ok = float(entry["price_per_ok"]),
                            )
                            session.add(td)
                            session.flush()
                            tier_lookup[key] = td
                            session.add(UserTier(user_id=uid, tier_def_id=td.id))

        if "custom_names" in cfg:
            session.query(CustomName).delete()
            for uid, name in cfg["custom_names"].items():
                session.add(CustomName(user_id=uid, display_name=name))
        if "balances" in data:
            session.query(Balance).delete()
            for uid, amt in data["balances"].items():
                session.add(Balance(user_id=uid, amount=float(amt)))
        session.commit()
        flash("Config imported successfully", "success")
    except Exception as e:
        session.rollback()
        flash(f"Import failed: {e}", "error")
    return redirect(url_for("settings"))


@app.route("/api/users")
def api_users():
    session = get_session()
    q       = request.args.get("q", "").lower()
    users   = build_user_summaries(session)
    if q:
        users = [u for u in users
                 if q in u["user_id"].lower() or q in u["display_name"].lower()]
    return jsonify(users)


# ── Card generation ───────────────────────────────────────────────────────────

@app.route("/card/<user_id>.png")
def user_card_png(user_id):
    """Generate and serve a single user photo card as PNG."""
    from card_generator import generate_card
    session      = get_session()
    display_name = get_display_name(session, user_id)
    pending      = get_net_pending(session, user_id)
    date         = datetime.today().strftime("%Y-%m-%d")
    png_bytes    = generate_card(user_id=user_id, display_name=display_name,
                                  pending=pending, date=date)
    return Response(png_bytes, mimetype="image/png",
                    headers={"Content-Disposition":
                             f"inline; filename={user_id}_card.png"})


@app.route("/api/card/<user_id>")
def api_card_base64(user_id):
    """Return card PNG as base64 JSON for inline preview."""
    import base64
    from card_generator import generate_card
    session      = get_session()
    display_name = get_display_name(session, user_id)
    pending      = get_net_pending(session, user_id)
    date         = datetime.today().strftime("%Y-%m-%d")
    png_bytes    = generate_card(user_id=user_id, display_name=display_name,
                                  pending=pending, date=date)
    return jsonify({
        "user_id":      user_id,
        "display_name": display_name,
        "pending":      pending,
        "image_b64":    base64.b64encode(png_bytes).decode(),
    })


# ── Telegram send — individual ────────────────────────────────────────────────

@app.route("/user/<user_id>/send", methods=["POST"])
def send_user_telegram(user_id):
    """Generate card and send via Telegram to this user. Returns JSON."""
    from card_generator import generate_card
    from telegram_service import send_photo_with_retry
    session      = get_session()
    display_name = get_display_name(session, user_id)
    pending      = get_net_pending(session, user_id)
    date         = datetime.today().strftime("%Y-%m-%d")
    try:
        png_bytes = generate_card(user_id=user_id, display_name=display_name,
                                   pending=pending, date=date)
        result    = send_photo_with_retry(user_id=user_id, photo_bytes=png_bytes, date=date)
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ── Telegram send — all users (SSE live stream) ───────────────────────────────

@app.route("/send-all/stream")
def send_all_stream():
    """
    SSE endpoint — streams JSON-lines events while sending cards to all users.
    Event types match Flutter _LogEntry statuses:
      {"type":"start",   "total": N}
      {"type":"log",     "uid": "...", "name": "...", "status": "pending|ok|err|blocked|retry", "msg": "..."}
      {"type":"update",  "uid": "...", "status": "ok|err|blocked|retry", "msg": "..."}
      {"type":"done",    "sent": N, "failed": N, "blocked": N}
    """
    from card_generator import generate_card
    from telegram_service import send_photo_with_retry
    import json as _json

    session = get_session()
    users   = build_user_summaries(session)
    date    = datetime.today().strftime("%Y-%m-%d")
    # Only retry user_ids passed via ?retry=uid1,uid2,...
    retry_ids = request.args.get("retry", "")
    if retry_ids:
        uid_set = set(retry_ids.split(","))
        users   = [u for u in users if u["user_id"] in uid_set]

    q = queue.Queue()

    def worker():
        sent = failed = blocked = 0
        q.put({"type": "start", "total": len(users)})

        # Phase 1 — generate all cards
        card_cache = {}
        for u in users:
            uid  = u["user_id"]
            name = u["display_name"]
            try:
                q.put({"type": "log", "uid": uid, "name": name,
                       "status": "pending", "msg": "generating card..."})
                card_cache[uid] = generate_card(
                    user_id=uid, display_name=name,
                    pending=u["pending"], date=date)
                q.put({"type": "update", "uid": uid,
                       "status": "pending", "msg": "card ready, sending..."})
            except Exception as e:
                q.put({"type": "update", "uid": uid, "status": "err",
                       "msg": f"card failed: {e}"})
                failed += 1

        # Phase 2 — send (5 concurrent threads)
        lock = threading.Lock()
        counters = {"sent": 0, "failed": 0, "blocked": 0}
        failed_uids = []

        def send_one(uid):
            if uid not in card_cache:
                return
            for attempt in range(1, 4):
                result = send_photo_with_retry(
                    user_id=uid, photo_bytes=card_cache[uid], date=date, max_retries=1)
                if result.get("ok"):
                    q.put({"type": "update", "uid": uid,
                           "status": "ok", "msg": "sent ✓"})
                    with lock:
                        counters["sent"] += 1
                    return
                elif result.get("blocked"):
                    q.put({"type": "update", "uid": uid,
                           "status": "blocked", "msg": "blocked by user"})
                    with lock:
                        counters["blocked"] += 1
                    return
                elif result.get("retryable") and attempt < 3:
                    q.put({"type": "update", "uid": uid,
                           "status": "retry", "msg": f"retry {attempt}..."})
                    time.sleep(0.5 + attempt * 0.5 + random.random() * 0.2)
                else:
                    break

            err = result.get("error", "failed")
            q.put({"type": "update", "uid": uid, "status": "err", "msg": err})
            with lock:
                counters["failed"] += 1
                failed_uids.append(uid)

        import time, random
        # Send in batches of 5 (Telegram rate limit safe)
        batch_size = 5
        uid_list   = list(card_cache.keys())
        for i in range(0, len(uid_list), batch_size):
            batch   = uid_list[i:i+batch_size]
            threads = [threading.Thread(target=send_one, args=(uid,)) for uid in batch]
            for t in threads: t.start()
            for t in threads: t.join()

        q.put({"type": "done",
               "sent": counters["sent"],
               "failed": counters["failed"],
               "blocked": counters["blocked"],
               "failed_uids": failed_uids})
        q.put(None)  # sentinel

    threading.Thread(target=worker, daemon=True).start()

    def generate():
        while True:
            item = q.get()
            if item is None:
                break
            yield f"data: {json.dumps(item)}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


if __name__ == "__main__":
    init_db()
    app.run(debug=True, host="0.0.0.0", port=PORT, threaded=True)
