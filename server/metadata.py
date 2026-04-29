from registry import DeviceRegistry


# Each teammate's metadata table maps to one house.
HOUSE_OF_DB = {"kien": "A", "alex": "B"}

# How to classify a sensor into the kinds the three queries care about.
# Kind is decided by (device_type, sensor_name) substrings, case-insensitive.
def _classify(device_type: str, sensor_name: str):
    dt = (device_type or "").lower()
    sn = (sensor_name or "").lower()
    is_fridge = "fridge" in dt or "refrigerator" in dt
    # Note: Kien's metadata has the typo "dishwwasher" — match on "wash"
    # (which appears in both "dishwasher" and "dishwwasher") so the registry
    # picks up dishwashers from either teammate's environment.
    is_dishwasher = "wash" in dt
    if is_fridge and ("moisture" in sn or "fc-28" in sn):
        return "fridge_moisture"
    if is_dishwasher and "water" in sn:
        return "dishwasher_water"
    if "ammeter" in sn or "acs712" in sn:
        return "ammeter"
    return None


def load_registry(db_kien, db_alex) -> DeviceRegistry:
    """Walk both metadata tables and build a unified DeviceRegistry."""
    reg = DeviceRegistry()
    _ingest(reg, db_kien, "IoT_metadata", parent_db="kien")
    _ingest(reg, db_alex, "sensor_data_metadata", parent_db="alex")
    return reg


def _ingest(reg: DeviceRegistry, db, table: str, parent_db: str) -> None:
    house = HOUSE_OF_DB[parent_db]
    rows = db.execute(f'SELECT "assetUid", "assetType", "customAttributes" FROM "{table}";')
    if not rows:
        return
    for device_uid, device_type, ca in rows:
        device_name = (ca or {}).get("name")
        reg.add_node(
            device_uid,
            house=house,
            parent_db=parent_db,
            is_device=True,
            device_uid=device_uid,
            device_name=device_name,
            device_type=device_type,
        )
        for board in (ca or {}).get("children", []) or []:
            board_uid = board.get("assetUid")
            bca = board.get("customAttributes", {}) or {}
            board_name = bca.get("name")
            reg.add_node(
                board_uid,
                house=house,
                parent_db=parent_db,
                is_board=True,
                device_uid=device_uid,
                device_name=device_name,
                device_type=device_type,
                board_uid=board_uid,
                board_name=board_name,
            )
            for sensor in bca.get("children", []) or []:
                sensor_uid = sensor.get("assetUid")
                sca = sensor.get("customAttributes", {}) or {}
                sensor_name = sca.get("name")
                kind = _classify(device_type, sensor_name)
                reg.add_node(
                    sensor_uid,
                    house=house,
                    parent_db=parent_db,
                    is_sensor=True,
                    device_uid=device_uid,
                    device_name=device_name,
                    device_type=device_type,
                    board_uid=board_uid,
                    board_name=board_name,
                    sensor_uid=sensor_uid,
                    sensor_name=sensor_name,
                    unit=sca.get("unit"),
                    min_value=sca.get("minValue"),
                    max_value=sca.get("maxValue"),
                    kind=kind,
                )
                if kind:
                    reg.index_sensor(kind, house, sensor_uid)
