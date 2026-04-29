class DeviceRegistry:
    def __init__(self):
        # asset_uid -> entry. Sensor leaves carry the full per-sensor record;
        # board/device entries carry only house + parent_db so house_of() works
        # for any uid that appears in a payload (sensor, board, or device).
        self._by_uid = {}
        # (house, kind) -> list[sensor_uid]
        self._by_kind = {}

    def add_node(self, asset_uid, house, parent_db, **extra):
        if not asset_uid:
            return
        entry = {"house": house, "parent_db": parent_db, **extra}
        self._by_uid[asset_uid] = entry

    def index_sensor(self, kind, house, sensor_uid):
        self._by_kind.setdefault((house, kind), []).append(sensor_uid)

    def house_of(self, asset_uid):
        entry = self._by_uid.get(asset_uid)
        return entry["house"] if entry else None

    def parent_db_of(self, asset_uid):
        entry = self._by_uid.get(asset_uid)
        return entry["parent_db"] if entry else None

    def get(self, asset_uid):
        return self._by_uid.get(asset_uid)

    def sensors_for(self, house, kind):
        return list(self._by_kind.get((house, kind), []))

    def all_sensor_uids(self):
        return [uid for uid, e in self._by_uid.items() if e.get("is_sensor")]

    def __repr__(self):
        sensors = sum(1 for e in self._by_uid.values() if e.get("is_sensor"))
        return f"DeviceRegistry(nodes={len(self._by_uid)}, sensors={sensors})"
