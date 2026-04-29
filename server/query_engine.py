from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


PST = ZoneInfo("America/Los_Angeles")

DB_TABLE = {"kien": "IoT_virtual", "alex": "sensor_data_virtual"}
HOUSE_LABEL = {"A": "House A (Kien)", "B": "House B (Alex)"}
PAYLOAD_META_KEYS = {"timestamp", "topic", "parent_asset_uid", "asset_uid", "board_name"}

# Q3 electricity: ammeter readings are in Amps. Assume household 120 V mains.
LINE_VOLTAGE = 120.0
LITERS_PER_GALLON = 3.785411784
# Assumption: a dishwasher cycle is one hour. The DataNiz simulator publishes
# continuous flow readings instead of bracketed cycle events, so we segment by
# clock-hour rather than by reading-gap. Documented in the report.
DISHWASHER_CYCLE_MINUTES = 60.0


def _now_utc():
    return datetime.now(timezone.utc)


def _fmt_pst(t: datetime) -> str:
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.astimezone(PST).strftime("%Y-%m-%d %H:%M:%S PST")


def _extract_readings(payload: dict):
    """Yield (sensor_name, float_value) for every non-meta key in a DataNiz
    payload row. Kien's gateway packs multiple sensors per row; Alex's packs
    one — this handles both."""
    for k, v in payload.items():
        if k in PAYLOAD_META_KEYS:
            continue
        try:
            yield k, float(v)
        except (TypeError, ValueError):
            continue


class QueryEngine:
    def __init__(self, db_kien, db_alex, registry):
        self.dbs = {"kien": db_kien, "alex": db_alex}
        self.registry = registry

    # ---------- low-level fetch with cross-DB gap fill ----------

    def _fetch(self, source_db: str, board_uids, t_start, t_end):
        if not board_uids:
            return []
        table = DB_TABLE[source_db]
        rows = self.dbs[source_db].execute(
            f'SELECT time, payload FROM "{table}" '
            f"WHERE time >= %s AND time < %s "
            f"AND payload->>'asset_uid' = ANY(%s);",
            (t_start, t_end, list(board_uids)),
        ) or []
        return [(t, p, source_db) for t, p in rows]

    def fetch_with_gap_fill(self, primary_db, secondary_db, board_uids, t_start, t_end):
        """Query the metadata-owning DB; if its history starts after t_start,
        pull the missing prefix from the teammate's DB. Returns
        (rows, summary_dict). Rows are deduped by (asset_uid, payload-timestamp,
        sensor_name) so that MQTT cross-publication doesn't double-count."""
        primary_rows = self._fetch(primary_db, board_uids, t_start, t_end)
        filled = []
        if primary_rows:
            min_local = min(r[0] for r in primary_rows)
            if min_local > t_start:
                filled = self._fetch(secondary_db, board_uids, t_start, min_local)
        else:
            # No primary coverage at all in window — try secondary for the whole range.
            filled = self._fetch(secondary_db, board_uids, t_start, t_end)

        # Dedup by (asset_uid, payload-timestamp). Each payload row carries
        # one or more sensor readings together; once we've kept a row from one
        # source DB, the duplicate from the other DB adds nothing new.
        seen = set()
        merged = []
        for t, p, src in primary_rows + filled:
            key = (p.get("asset_uid"), p.get("timestamp"))
            if key in seen:
                continue
            seen.add(key)
            merged.append((t, p, src))

        return merged, {
            "primary_rows": len(primary_rows),
            "gap_fill_rows": len(filled),
            "kept_rows": len(merged),
            "primary_db": primary_db,
            "secondary_db": secondary_db,
        }

    # ---------- helpers shared by handlers ----------

    def _board_uids_for_sensors(self, sensor_uids):
        boards = set()
        for su in sensor_uids:
            entry = self.registry.get(su)
            if entry and entry.get("board_uid"):
                boards.add(entry["board_uid"])
        return boards

    def _sensor_lookup_by_board_and_name(self, sensor_uids):
        out = {}
        for su in sensor_uids:
            e = self.registry.get(su)
            if e:
                out[(e["board_uid"], e["sensor_name"])] = e
        return out

    def _collect_per_house(self, kind):
        """Returns {house: {'sensors': [...uid...], 'primary_db': db, 'secondary_db': db}}."""
        per_house = {}
        for house in ("A", "B"):
            sensors = self.registry.sensors_for(house, kind)
            if not sensors:
                continue
            primary_db = self.registry.parent_db_of(sensors[0])
            secondary_db = "alex" if primary_db == "kien" else "kien"
            per_house[house] = {
                "sensors": sensors,
                "primary_db": primary_db,
                "secondary_db": secondary_db,
            }
        return per_house

    def _gather_readings(self, kind, t_start, t_end):
        """For a query kind, fetch readings for every house. Returns
        {house: {'rows': [(time_utc, value, sensor_entry), ...], 'summary': {...}}}.
        Each value is the raw float from the payload (no unit conversion yet)."""
        out = {}
        per_house = self._collect_per_house(kind)
        for house, info in per_house.items():
            board_uids = self._board_uids_for_sensors(info["sensors"])
            lookup = self._sensor_lookup_by_board_and_name(info["sensors"])
            rows, summary = self.fetch_with_gap_fill(
                info["primary_db"], info["secondary_db"], board_uids, t_start, t_end
            )
            readings = []
            for t, payload, _src in rows:
                # Cross-attribute by metadata: only keep rows whose owning house
                # matches this house (the row's asset_uid -> registry -> house).
                if self.registry.house_of(payload.get("asset_uid")) != house:
                    continue
                for sensor_name, value in _extract_readings(payload):
                    entry = lookup.get((payload.get("asset_uid"), sensor_name))
                    if entry is None:
                        continue
                    readings.append((t, value, entry))
            readings.sort(key=lambda r: r[0])
            out[house] = {"rows": readings, "summary": summary}
        return out

    # ---------- Q1: fridge moisture (avg, in %) ----------

    def q1_fridge_moisture(self):
        now = _now_utc()
        windows = [("past hour", now - timedelta(hours=1)),
                   ("past week", now - timedelta(days=7)),
                   ("past month", now - timedelta(days=30))]

        lines = ["Avg moisture inside kitchen fridges (linear scale, sensor min/max -> 0-100%):"]
        for label, t_start in windows:
            data = self._gather_readings("fridge_moisture", t_start, now)
            lines.append(f"  {label} (since {_fmt_pst(t_start)}):")
            if not data:
                lines.append("    no fridge moisture sensors registered")
                continue
            for house in sorted(data.keys()):
                rows = data[house]["rows"]
                if not rows:
                    lines.append(f"    {HOUSE_LABEL[house]}: no readings")
                    continue
                pct_total = 0.0
                n = 0
                for _t, v, entry in rows:
                    mn = float(entry.get("min_value") or 0.0)
                    mx = float(entry.get("max_value") or 0.0)
                    if mx <= mn:
                        continue
                    pct = max(0.0, min(100.0, (v - mn) / (mx - mn) * 100.0))
                    pct_total += pct
                    n += 1
                if n == 0:
                    lines.append(f"    {HOUSE_LABEL[house]}: no usable readings")
                else:
                    s = data[house]["summary"]
                    lines.append(
                        f"    {HOUSE_LABEL[house]}: {pct_total / n:.2f}% "
                        f"(n={n}, primary={s['primary_db']}={s['primary_rows']}, "
                        f"gap-fill={s['secondary_db']}={s['gap_fill_rows']})"
                    )
        return "\n".join(lines)

    # ---------- Q2: dishwasher water consumption per cycle (gallons) ----------

    def q2_dishwasher_water(self):
        now = _now_utc()
        windows = [("past hour", now - timedelta(hours=1)),
                   ("past week", now - timedelta(days=7)),
                   ("past month", now - timedelta(days=30))]

        lines = ["Avg water consumption per cycle across smart dishwashers (gallons/cycle):"]
        for label, t_start in windows:
            data = self._gather_readings("dishwasher_water", t_start, now)
            lines.append(f"  {label} (since {_fmt_pst(t_start)}):")
            if not data:
                lines.append("    no dishwasher water sensors registered")
                continue
            for house in sorted(data.keys()):
                rows = data[house]["rows"]
                s = data[house]["summary"]
                if len(rows) < 2:
                    lines.append(f"    {HOUSE_LABEL[house]}: not enough readings to compute a cycle")
                    continue
                # Trapezoidal integration of L/min over time -> total liters.
                liters = 0.0
                for prev, curr in zip(rows, rows[1:]):
                    dt_min = (curr[0] - prev[0]).total_seconds() / 60.0
                    avg_lpm = (prev[1] + curr[1]) / 2.0
                    liters += avg_lpm * dt_min
                window_minutes = (rows[-1][0] - rows[0][0]).total_seconds() / 60.0
                cycles = max(window_minutes / DISHWASHER_CYCLE_MINUTES, 1.0)
                gallons_per_cycle = (liters / LITERS_PER_GALLON) / cycles
                lines.append(
                    f"    {HOUSE_LABEL[house]}: {gallons_per_cycle:.3f} gal/cycle "
                    f"(~{cycles:.1f} 1-hr cycles, primary={s['primary_db']}={s['primary_rows']}, "
                    f"gap-fill={s['secondary_db']}={s['gap_fill_rows']})"
                )
        return "\n".join(lines)

    # ---------- Q3: electricity past 24h, per house ----------

    def q3_electricity_24h(self):
        now = _now_utc()
        t_start = now - timedelta(hours=24)
        data = self._gather_readings("ammeter", t_start, now)

        header = (
            f"Electricity usage — past 24 h "
            f"(PST window: {_fmt_pst(t_start)} -> {_fmt_pst(now)}; "
            f"assumed line voltage {LINE_VOLTAGE:.0f} V):"
        )
        per_house_kwh = {}
        breakdown = []
        for house in sorted(data.keys()):
            rows = data[house]["rows"]
            if not rows:
                per_house_kwh[house] = 0.0
                breakdown.append(f"  {HOUSE_LABEL[house]}: no ammeter readings in window")
                continue
            # Per ammeter, integrate amps over time -> amp-hours -> kWh.
            by_sensor = {}
            for t, v, entry in rows:
                by_sensor.setdefault(entry["sensor_uid"], []).append((t, v))
            kwh_total = 0.0
            for su, samples in by_sensor.items():
                samples.sort(key=lambda x: x[0])
                amp_hours = 0.0
                for prev, curr in zip(samples, samples[1:]):
                    dt_h = (curr[0] - prev[0]).total_seconds() / 3600.0
                    avg_a = (prev[1] + curr[1]) / 2.0
                    amp_hours += avg_a * dt_h
                kwh_total += amp_hours * LINE_VOLTAGE / 1000.0
            per_house_kwh[house] = kwh_total
            s = data[house]["summary"]
            breakdown.append(
                f"  {HOUSE_LABEL[house]}: {kwh_total:.3f} kWh "
                f"(ammeters={len(by_sensor)}, primary={s['primary_db']}={s['primary_rows']}, "
                f"gap-fill={s['secondary_db']}={s['gap_fill_rows']})"
            )

        verdict = ""
        if "A" in per_house_kwh and "B" in per_house_kwh:
            a, b = per_house_kwh["A"], per_house_kwh["B"]
            if a == 0 and b == 0:
                verdict = "Neither house consumed measurable electricity in the window."
            else:
                higher = "A" if a > b else "B"
                lower = "B" if higher == "A" else "A"
                diff = abs(a - b)
                base = max(per_house_kwh[lower], 1e-9)
                pct = diff / base * 100.0
                verdict = (
                    f"{HOUSE_LABEL[higher]} consumed more by {diff:.3f} kWh "
                    f"({pct:.1f}% more than {HOUSE_LABEL[lower]})."
                )
        return "\n".join([header, *breakdown, verdict]) if verdict else "\n".join([header, *breakdown])

    # ---------- public dispatch ----------

    def handle(self, query: str) -> str:
        q = query.strip()
        if q == "What is the average moisture inside our kitchen fridges in the past hours, week and month?":
            return self.q1_fridge_moisture()
        if q == "What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?":
            return self.q2_dishwasher_water()
        if q == "Which house consumed more electricity in the past 24 hours, and by how much?":
            return self.q3_electricity_24h()
        return "Unsupported query."
