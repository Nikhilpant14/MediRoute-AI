import json
import math
import os
import sqlite3
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

VENDOR_PATH = Path(__file__).resolve().with_name("vendor")
if VENDOR_PATH.exists():
    sys.path.insert(0, str(VENDOR_PATH))

from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO, join_room

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")
dispatch_lock = threading.RLock()
route_update_interval_seconds = 1
average_ambulance_speed_kmph = 62
emergency_speed_multiplier = 2.65

DATABASE_PATH = Path(__file__).resolve().with_name("dispatch.db")

SEED_AMBULANCES = [
    {"id": 1, "lat": 30.2708, "lng": 78.0115, "status": "available", "phase": "idle", "available_in_seconds": 0, "destination_name": None},
    {"id": 2, "lat": 30.3167, "lng": 78.0328, "status": "available", "phase": "idle", "available_in_seconds": 0, "destination_name": None},
    {"id": 3, "lat": 30.3256, "lng": 78.0437, "status": "available", "phase": "idle", "available_in_seconds": 0, "destination_name": None},
    {"id": 4, "lat": 30.3737, "lng": 78.0747, "status": "available", "phase": "idle", "available_in_seconds": 0, "destination_name": None},
    {"id": 5, "lat": 30.2878, "lng": 78.0219, "status": "available", "phase": "idle", "available_in_seconds": 0, "destination_name": None},
    {"id": 6, "lat": 30.3038, "lng": 78.0552, "status": "available", "phase": "idle", "available_in_seconds": 0, "destination_name": None},
    {"id": 7, "lat": 30.3494, "lng": 78.0678, "status": "available", "phase": "idle", "available_in_seconds": 0, "destination_name": None},
    {"id": 8, "lat": 30.2415, "lng": 78.0483, "status": "available", "phase": "idle", "available_in_seconds": 0, "destination_name": None},
]

SEED_HOSPITALS = [
    {"name": "Velmed Hospital", "lat": 30.2706, "lng": 78.0106},
    {"name": "Doon Hospital", "lat": 30.3250, "lng": 78.0430},
    {"name": "CMI Hospital", "lat": 30.3168, "lng": 78.0438},
    {"name": "Max Super Speciality Hospital", "lat": 30.3737, "lng": 78.0747},
    {"name": "AIIMS Rishikesh", "lat": 30.0869, "lng": 78.2676},
]


def load_local_properties():
    properties = {}
    local_properties_path = Path(__file__).resolve().parents[1] / "local.properties"
    if not local_properties_path.exists():
        return properties

    for raw_line in local_properties_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        properties[key.strip()] = value.strip()

    return properties


LOCAL_PROPERTIES = load_local_properties()


def get_db_connection():
    connection = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


def rows_to_dicts(rows):
    return [dict(row) for row in rows]


def init_database():
    with get_db_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS ambulances (
                id INTEGER PRIMARY KEY,
                lat REAL NOT NULL,
                lng REAL NOT NULL,
                status TEXT NOT NULL,
                phase TEXT NOT NULL,
                available_in_seconds INTEGER NOT NULL DEFAULT 0,
                destination_name TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS hospitals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                lat REAL NOT NULL,
                lng REAL NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS dispatch_requests (
                request_id TEXT PRIMARY KEY,
                requester_id TEXT,
                ambulance_id INTEGER,
                patient_lat REAL NOT NULL,
                patient_lng REAL NOT NULL,
                hospital_name TEXT,
                hospital_lat REAL,
                hospital_lng REAL,
                status TEXT NOT NULL,
                phase TEXT NOT NULL,
                eta_to_patient INTEGER,
                eta_to_hospital INTEGER,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                completed_at REAL,
                error_message TEXT
            )
            """
        )

        connection.executemany(
            """
            INSERT OR IGNORE INTO ambulances (id, lat, lng, status, phase, available_in_seconds, destination_name)
            VALUES (:id, :lat, :lng, :status, :phase, :available_in_seconds, :destination_name)
            """,
            SEED_AMBULANCES,
        )

        connection.executemany(
            "INSERT OR IGNORE INTO hospitals (name, lat, lng) VALUES (:name, :lat, :lng)",
            SEED_HOSPITALS,
        )

        connection.commit()


def get_ambulances_snapshot():
    with get_db_connection() as connection:
        rows = connection.execute(
            "SELECT id, lat, lng, status, phase, available_in_seconds, destination_name FROM ambulances ORDER BY id"
        ).fetchall()
    return rows_to_dicts(rows)


def get_hospitals_snapshot():
    with get_db_connection() as connection:
        rows = connection.execute(
            "SELECT id, name, lat, lng FROM hospitals ORDER BY id"
        ).fetchall()
    return rows_to_dicts(rows)


def create_dispatch_request_record(
    requester_id,
    patient,
    ambulance_id=None,
    hospital=None,
    eta_to_patient=None,
    eta_to_hospital=None,
    status="searching",
    phase="searching",
    error_message=None,
):
    request_id = str(uuid.uuid4())
    now = time.time()
    with get_db_connection() as connection:
        connection.execute(
            """
            INSERT INTO dispatch_requests (
                request_id, requester_id, ambulance_id, patient_lat, patient_lng,
                hospital_name, hospital_lat, hospital_lng, status, phase,
                eta_to_patient, eta_to_hospital, created_at, updated_at, completed_at, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                requester_id,
                ambulance_id,
                patient[0],
                patient[1],
                hospital["name"] if hospital else None,
                hospital["lat"] if hospital else None,
                hospital["lng"] if hospital else None,
                status,
                phase,
                eta_to_patient,
                eta_to_hospital,
                now,
                now,
                None,
                error_message,
            ),
        )
        connection.commit()
    emit_dashboard_update()
    return request_id


def update_dispatch_request(
    request_id,
    *,
    ambulance_id=None,
    hospital=None,
    status=None,
    phase=None,
    eta_to_patient=None,
    eta_to_hospital=None,
    error_message=None,
    completed=False,
):
    fields = []
    values = []

    if ambulance_id is not None:
        fields.append("ambulance_id = ?")
        values.append(ambulance_id)
    if hospital is not None:
        fields.extend(["hospital_name = ?", "hospital_lat = ?", "hospital_lng = ?"])
        values.extend([hospital["name"], hospital["lat"], hospital["lng"]])
    if status is not None:
        fields.append("status = ?")
        values.append(status)
    if phase is not None:
        fields.append("phase = ?")
        values.append(phase)
    if eta_to_patient is not None:
        fields.append("eta_to_patient = ?")
        values.append(eta_to_patient)
    if eta_to_hospital is not None:
        fields.append("eta_to_hospital = ?")
        values.append(eta_to_hospital)
    if error_message is not None:
        fields.append("error_message = ?")
        values.append(error_message)
    if completed:
        fields.append("completed_at = ?")
        values.append(time.time())

    fields.append("updated_at = ?")
    values.append(time.time())
    values.append(request_id)

    with get_db_connection() as connection:
        connection.execute(
            f"UPDATE dispatch_requests SET {', '.join(fields)} WHERE request_id = ?",
            values,
        )
        connection.commit()
    emit_dashboard_update()


def get_dispatch_request(request_id):
    with get_db_connection() as connection:
        row = connection.execute(
            """
            SELECT request_id, requester_id, ambulance_id, patient_lat, patient_lng,
                   hospital_name, hospital_lat, hospital_lng, status, phase,
                   eta_to_patient, eta_to_hospital, created_at, updated_at,
                   completed_at, error_message
            FROM dispatch_requests
            WHERE request_id = ?
            """,
            (request_id,),
        ).fetchone()
    return dict(row) if row else None


def list_dispatch_requests(limit=25, active_only=False):
    query = """
        SELECT request_id, requester_id, ambulance_id, patient_lat, patient_lng,
               hospital_name, hospital_lat, hospital_lng, status, phase,
               eta_to_patient, eta_to_hospital, created_at, updated_at,
               completed_at, error_message
        FROM dispatch_requests
    """
    parameters = []
    if active_only:
        query += " WHERE status IN ('searching', 'assigned', 'active')"
    query += " ORDER BY updated_at DESC LIMIT ?"
    parameters.append(limit)

    with get_db_connection() as connection:
        rows = connection.execute(query, parameters).fetchall()
    return rows_to_dicts(rows)


init_database()


def distance(lat1, lon1, lat2, lon2):
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    haversine = (
        math.sin(delta_lat / 2) ** 2 +
        math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    return 6371 * 2 * math.atan2(math.sqrt(haversine), math.sqrt(1 - haversine))


def estimate_eta_minutes(distance_km, average_speed_kmph=average_ambulance_speed_kmph):
    return max(1, math.ceil((distance_km / average_speed_kmph) * 60))


def estimate_route_duration_seconds(distance_km, average_speed_kmph=average_ambulance_speed_kmph):
    return max(60, int(math.ceil((distance_km / average_speed_kmph) * 3600)))


def distance_meters(point_a, point_b):
    return distance(point_a[0], point_a[1], point_b[0], point_b[1]) * 1000


def serialize_ambulance(ambulance):
    return {
        "id": ambulance["id"],
        "lat": ambulance["lat"],
        "lng": ambulance["lng"],
        "status": ambulance["status"],
        "phase": ambulance.get("phase"),
        "available_in_seconds": ambulance.get("available_in_seconds"),
        "destination_name": ambulance.get("destination_name"),
    }


def build_fleet_payload():
    with dispatch_lock:
        ambulances = get_ambulances_snapshot()
        available_ambulances = sum(1 for ambulance in ambulances if ambulance["status"] == "available")
        busy_ambulances = len(ambulances) - available_ambulances
        next_available = min(
            (
                ambulance
                for ambulance in ambulances
                if ambulance["status"] != "available" and (ambulance.get("available_in_seconds") or 0) > 0
            ),
            key=lambda ambulance: ambulance.get("available_in_seconds") or float("inf"),
            default=None,
        )
        return {
            "total_ambulances": len(ambulances),
            "available_ambulances": available_ambulances,
            "busy_ambulances": busy_ambulances,
            "next_available_unit_id": next_available["id"] if next_available else None,
            "next_available_in_seconds": next_available.get("available_in_seconds") if next_available else None,
            "next_available_in_minutes": max(1, math.ceil((next_available.get("available_in_seconds") or 0) / 60)) if next_available else None,
            "ambulances": [serialize_ambulance(ambulance) for ambulance in ambulances],
        }


def build_dashboard_payload():
    fleet_payload = build_fleet_payload()
    active_requests = list_dispatch_requests(limit=20, active_only=True)
    recent_requests = list_dispatch_requests(limit=40, active_only=False)

    with get_db_connection() as connection:
        metrics = connection.execute(
            """
            SELECT
                COUNT(*) AS total_requests,
                SUM(CASE WHEN status IN ('searching', 'assigned', 'active') THEN 1 ELSE 0 END) AS active_requests,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_requests,
                SUM(CASE WHEN status = 'busy' THEN 1 ELSE 0 END) AS busy_rejections
            FROM dispatch_requests
            """
        ).fetchone()

    total_ambulances = fleet_payload["total_ambulances"] or 1
    active_dispatches = metrics["active_requests"] or 0

    return {
        "generated_at": time.time(),
        "metrics": {
            "total_requests": metrics["total_requests"] or 0,
            "active_requests": active_dispatches,
            "completed_requests": metrics["completed_requests"] or 0,
            "busy_rejections": metrics["busy_rejections"] or 0,
            "available_ambulances": fleet_payload["available_ambulances"],
            "busy_ambulances": fleet_payload["busy_ambulances"],
            "fleet_utilization_percent": round((fleet_payload["busy_ambulances"] / total_ambulances) * 100, 1),
            "request_pressure_percent": round((active_dispatches / total_ambulances) * 100, 1),
        },
        "fleet": fleet_payload,
        "active_requests": active_requests,
        "recent_requests": recent_requests,
        "hospitals": get_hospitals_snapshot(),
    }


def emit_dashboard_update():
    socketio.emit("dashboard_update", build_dashboard_payload(), to="dashboard")


def emit_fleet_update():
    socketio.emit("fleet_update", build_fleet_payload())
    emit_dashboard_update()


def get_directions_api_key():
    return (
        os.getenv("DIRECTIONS_API_KEY") or
        os.getenv("GOOGLE_WEB_SERVICE_API_KEY") or
        LOCAL_PROPERTIES.get("DIRECTIONS_API_KEY") or
        LOCAL_PROPERTIES.get("GOOGLE_WEB_SERVICE_API_KEY") or
        LOCAL_PROPERTIES.get("MAPS_API_KEY") or
        ""
    )


def decode_polyline(encoded):
    points = []
    index = 0
    latitude = 0
    longitude = 0

    while index < len(encoded):
        shift = 0
        result = 0
        while True:
            byte = ord(encoded[index]) - 63
            index += 1
            result |= (byte & 0x1F) << shift
            shift += 5
            if byte < 0x20:
                break
        latitude += ~(result >> 1) if result & 1 else result >> 1

        shift = 0
        result = 0
        while True:
            byte = ord(encoded[index]) - 63
            index += 1
            result |= (byte & 0x1F) << shift
            shift += 5
            if byte < 0x20:
                break
        longitude += ~(result >> 1) if result & 1 else result >> 1

        points.append((latitude / 1e5, longitude / 1e5))

    return points


def encode_polyline(points):
    encoded = []
    previous_latitude = 0
    previous_longitude = 0

    for latitude, longitude in points:
        current_latitude = int(round(latitude * 1e5))
        current_longitude = int(round(longitude * 1e5))
        encoded.append(encode_polyline_value(current_latitude - previous_latitude))
        encoded.append(encode_polyline_value(current_longitude - previous_longitude))
        previous_latitude = current_latitude
        previous_longitude = current_longitude

    return "".join(encoded)


def encode_polyline_value(value):
    current_value = ~(value << 1) if value < 0 else value << 1
    chunks = []
    while current_value >= 0x20:
        chunks.append(chr((0x20 | (current_value & 0x1F)) + 63))
        current_value >>= 5
    chunks.append(chr(current_value + 63))
    return "".join(chunks)


def normalize_route_points(points):
    normalized = []
    for point in points:
        if not normalized or normalized[-1] != point:
            normalized.append(point)
    return normalized


def densify_route_points(points, max_segment_meters=18.0):
    clean_points = normalize_route_points(points)
    if len(clean_points) < 2:
        return clean_points

    dense_points = [clean_points[0]]
    for index in range(1, len(clean_points)):
        start = clean_points[index - 1]
        end = clean_points[index]
        segment_distance = distance_meters(start, end)
        steps = max(1, math.ceil(segment_distance / max_segment_meters))
        for step in range(1, steps + 1):
            fraction = step / steps
            dense_points.append(
                (
                    start[0] + ((end[0] - start[0]) * fraction),
                    start[1] + ((end[1] - start[1]) * fraction),
                )
            )

    return dense_points


def interpolate_along_points(points, target_distance_meters):
    if len(points) == 1:
        return points[0]

    travelled = 0.0
    for index in range(1, len(points)):
        start = points[index - 1]
        end = points[index]
        segment_distance = distance_meters(start, end)
        if segment_distance <= 0:
            continue

        next_travelled = travelled + segment_distance
        if target_distance_meters <= next_travelled:
            fraction = (target_distance_meters - travelled) / segment_distance
            return (
                start[0] + ((end[0] - start[0]) * fraction),
                start[1] + ((end[1] - start[1]) * fraction),
            )
        travelled = next_travelled

    return points[-1]


def sample_polyline(points, duration_seconds):
    clean_points = normalize_route_points(points)
    if len(clean_points) < 2:
        return []

    total_distance = 0.0
    for index in range(1, len(clean_points)):
        total_distance += distance_meters(clean_points[index - 1], clean_points[index])

    if total_distance <= 0:
        return []

    sample_count = max(len(clean_points) - 1, math.ceil(duration_seconds / route_update_interval_seconds))
    sample_count = max(1, sample_count)

    samples = []
    for sample_index in range(1, sample_count + 1):
        fraction = sample_index / sample_count
        samples.append(interpolate_along_points(clean_points, total_distance * fraction))

    return samples


def fetch_road_route(origin, destination):
    directions_api_key = get_directions_api_key()
    if directions_api_key:
        params = urlencode(
            {
                "origin": f"{origin[0]},{origin[1]}",
                "destination": f"{destination[0]},{destination[1]}",
                "mode": "driving",
                "departure_time": "now",
                "alternatives": "true",
                "key": directions_api_key,
            }
        )
        directions_url = f"https://maps.googleapis.com/maps/api/directions/json?{params}"

        try:
            with urlopen(directions_url, timeout=15) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception:
            payload = None

        if payload and payload.get("status") == "OK":
            routes = payload.get("routes") or []
            route = min(
                routes,
                key=lambda candidate: (
                    float(((candidate.get("legs") or [{}])[0].get("duration", {}) or {}).get("value", float("inf"))),
                    float(((candidate.get("legs") or [{}])[0].get("distance", {}) or {}).get("value", float("inf"))),
                ),
                default={},
            )
            leg = (route.get("legs") or [{}])[0]
            overview_polyline = route.get("overview_polyline", {}).get("points", "")
            route_points = decode_polyline(overview_polyline) if overview_polyline else []
            steps = []
            combined_step_points = []

            for step in leg.get("steps", []):
                step_polyline = step.get("polyline", {}).get("points", "")
                step_points = decode_polyline(step_polyline) if step_polyline else []
                if len(step_points) < 2:
                    continue
                combined_step_points.extend(normalize_route_points(step_points))
                steps.append(
                    {
                        "points": normalize_route_points(step_points),
                        "distance_meters": float(step.get("distance", {}).get("value", 0)),
                        "duration_seconds": float(step.get("duration", {}).get("value", 0)),
                    }
                )

            if not steps and len(route_points) >= 2:
                steps.append(
                    {
                        "points": normalize_route_points(route_points),
                        "distance_meters": float(leg.get("distance", {}).get("value", 0)),
                        "duration_seconds": float(leg.get("duration", {}).get("value", 0)),
                    }
                )

            route_points = densify_route_points(
                combined_step_points if len(combined_step_points) >= 2 else route_points
            )
            overview_polyline = encode_polyline(route_points) if len(route_points) >= 2 else overview_polyline

            return {
                "provider": "google",
                "mode": "road",
                "overview_polyline": overview_polyline,
                "points": normalize_route_points(route_points),
                "distance_meters": float(leg.get("distance", {}).get("value", 0)),
                "duration_seconds": float(leg.get("duration", {}).get("value", 0)),
                "steps": steps,
            }

    osrm_url = (
        "https://router.project-osrm.org/route/v1/driving/"
        f"{origin[1]},{origin[0]};{destination[1]},{destination[0]}"
        "?overview=full&geometries=polyline&steps=true&alternatives=true"
    )

    try:
        with urlopen(osrm_url, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return None

    if payload.get("code") != "Ok":
        return None

    routes = payload.get("routes") or []
    route = min(
        routes,
        key=lambda candidate: (
            float(candidate.get("duration", float("inf"))),
            float(candidate.get("distance", float("inf"))),
        ),
        default={},
    )
    leg = (route.get("legs") or [{}])[0]
    overview_polyline = route.get("geometry", "")
    route_points = decode_polyline(overview_polyline) if overview_polyline else []
    steps = []
    combined_step_points = []

    for step in leg.get("steps", []):
        step_polyline = step.get("geometry", "")
        step_points = decode_polyline(step_polyline) if step_polyline else []
        if len(step_points) < 2:
            continue
        combined_step_points.extend(normalize_route_points(step_points))
        steps.append(
            {
                "points": normalize_route_points(step_points),
                "distance_meters": float(step.get("distance", 0)),
                "duration_seconds": float(step.get("duration", 0)),
            }
        )

    if not steps and len(route_points) >= 2:
        steps.append(
            {
                "points": normalize_route_points(route_points),
                "distance_meters": float(route.get("distance", 0)),
                "duration_seconds": float(route.get("duration", 0)),
            }
        )

    route_points = densify_route_points(
        combined_step_points if len(combined_step_points) >= 2 else route_points
    )
    overview_polyline = encode_polyline(route_points) if len(route_points) >= 2 else overview_polyline

    return {
        "provider": "osrm",
        "mode": "road",
        "overview_polyline": overview_polyline,
        "points": normalize_route_points(route_points),
        "distance_meters": float(route.get("distance", 0)),
        "duration_seconds": float(route.get("duration", 0)),
        "steps": steps,
    }


def build_straight_route(origin, destination, total_duration_seconds):
    steps = max(12, max(1, int(total_duration_seconds)) // 10)
    points = [origin]
    for step in range(0, steps + 1):
        progress = step / steps
        points.append(
            (
                origin[0] + ((destination[0] - origin[0]) * progress),
                origin[1] + ((destination[1] - origin[1]) * progress),
            )
        )

    total_distance_meters = distance_meters(origin, destination)
    normalized_points = normalize_route_points(points)
    return {
        "mode": "simulated",
        "overview_polyline": encode_polyline(normalized_points),
        "points": normalized_points,
        "distance_meters": total_distance_meters,
        "duration_seconds": float(total_duration_seconds),
        "steps": [
            {
                "points": normalized_points,
                "distance_meters": total_distance_meters,
                "duration_seconds": float(total_duration_seconds),
            }
        ],
    }


def accelerate_route_plan(route_plan, speed_multiplier=emergency_speed_multiplier):
    if route_plan is None:
        return None

    accelerated_steps = []
    for step in route_plan.get("steps", []):
        accelerated_steps.append(
            {
                **step,
                "duration_seconds": max(1.0, float(step.get("duration_seconds") or 1.0) / speed_multiplier),
            }
        )

    accelerated_duration = sum(step["duration_seconds"] for step in accelerated_steps)
    if accelerated_duration <= 0:
        accelerated_duration = max(1.0, float(route_plan.get("duration_seconds") or 1.0) / speed_multiplier)

    return {
        **route_plan,
        "duration_seconds": accelerated_duration,
        "steps": accelerated_steps,
    }


def build_route_plan(origin, destination):
    road_route = fetch_road_route(origin, destination)
    if road_route:
        return accelerate_route_plan(road_route)

    fallback_distance_km = distance(origin[0], origin[1], destination[0], destination[1])
    fallback_duration_seconds = estimate_route_duration_seconds(
        fallback_distance_km,
        average_ambulance_speed_kmph * emergency_speed_multiplier,
    )
    return build_straight_route(origin, destination, fallback_duration_seconds)


def build_route_updates(route_plan):
    route_duration_seconds = max(1.0, float(route_plan.get("duration_seconds") or 1.0))
    updates = []
    elapsed_seconds = 0.0

    for step in route_plan.get("steps", []):
        step_points = normalize_route_points(step.get("points") or [])
        if len(step_points) < 2:
            continue

        step_duration_seconds = max(1.0, float(step.get("duration_seconds") or 1.0))
        step_distance_meters = float(step.get("distance_meters") or 0.0)
        sampled_points = sample_polyline(step_points, step_duration_seconds)
        if not sampled_points:
            continue

        delay_seconds = step_duration_seconds / len(sampled_points)
        speed_kmph = max(1.0, (step_distance_meters / step_duration_seconds) * 3.6)

        for point in sampled_points:
            elapsed_seconds += delay_seconds
            remaining_seconds = max(0, int(round(route_duration_seconds - elapsed_seconds)))
            updates.append(
                {
                    "lat": point[0],
                    "lng": point[1],
                    "speed_kmph": round(speed_kmph, 1),
                    "delay_seconds": delay_seconds,
                    "remaining_seconds": remaining_seconds,
                }
            )

    return updates


def get_available_ambulances():
    with dispatch_lock:
        return [
            serialize_ambulance(ambulance)
            for ambulance in get_ambulances_snapshot()
            if ambulance["status"] == "available"
        ]


def get_ranked_ambulance_routes(patient, ambulances_snapshot=None):
    ranked_routes = []
    candidate_ambulances = ambulances_snapshot or get_ambulances_snapshot()
    for ambulance in candidate_ambulances:
        origin = (ambulance["lat"], ambulance["lng"])
        route_plan = build_route_plan(origin, patient)
        ranked_routes.append((ambulance, route_plan))

    ranked_routes.sort(
        key=lambda item: (
            float(item[1].get("duration_seconds") or float("inf")),
            float(item[1].get("distance_meters") or float("inf")),
            item[0]["id"],
        )
    )
    return ranked_routes


def get_best_ambulance_route(patient):
    ranked_routes = get_ranked_ambulance_routes(patient, ambulances_snapshot=get_available_ambulances())
    if not ranked_routes:
        return None, None
    return ranked_routes[0]


def get_best_hospital_route(origin):
    best_hospital = None
    best_route_plan = None
    best_duration_seconds = float("inf")

    for hospital in get_hospitals_snapshot():
        destination = (hospital["lat"], hospital["lng"])
        route_plan = build_route_plan(origin, destination)
        route_duration_seconds = float(route_plan.get("duration_seconds") or 0.0)

        if route_duration_seconds < best_duration_seconds:
            best_duration_seconds = route_duration_seconds
            best_hospital = hospital
            best_route_plan = route_plan

    return best_hospital, best_route_plan


def claim_ambulance(ambulance_id):
    with dispatch_lock:
        with get_db_connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT id, lat, lng, status, phase, available_in_seconds, destination_name
                FROM ambulances
                WHERE id = ?
                """,
                (ambulance_id,),
            ).fetchone()
            if row is None or row["status"] != "available":
                connection.rollback()
                return None, None

            connection.execute(
                """
                UPDATE ambulances
                SET status = ?, phase = ?, available_in_seconds = ?, destination_name = ?
                WHERE id = ?
                """,
                ("busy", "assigned", 0, "Pickup place", ambulance_id),
            )
            connection.commit()
            updated = connection.execute(
                """
                SELECT id, lat, lng, status, phase, available_in_seconds, destination_name
                FROM ambulances
                WHERE id = ?
                """,
                (ambulance_id,),
            ).fetchone()
        if updated is not None:
            ambulance = dict(updated)
            return ambulance, serialize_ambulance(ambulance)
    return None, None


def set_ambulance_progress(ambulance, phase, remaining_seconds, destination_name):
    with dispatch_lock:
        ambulance["phase"] = phase
        ambulance["available_in_seconds"] = max(0, int(round(remaining_seconds)))
        ambulance["destination_name"] = destination_name
        with get_db_connection() as connection:
            connection.execute(
                """
                UPDATE ambulances
                SET status = ?, phase = ?, available_in_seconds = ?, destination_name = ?, lat = ?, lng = ?
                WHERE id = ?
                """,
                (
                    ambulance["status"],
                    ambulance["phase"],
                    ambulance["available_in_seconds"],
                    ambulance["destination_name"],
                    ambulance["lat"],
                    ambulance["lng"],
                    ambulance["id"],
                ),
            )
            connection.commit()


def build_phase_updates(origin, destination, route_plan=None):
    resolved_route_plan = route_plan or build_route_plan(origin, destination)
    updates = build_route_updates(resolved_route_plan)
    if updates:
        return resolved_route_plan, updates

    fallback_duration_seconds = estimate_route_duration_seconds(
        distance(origin[0], origin[1], destination[0], destination[1])
    )
    fallback_route_plan = build_straight_route(origin, destination, fallback_duration_seconds)
    return fallback_route_plan, build_route_updates(fallback_route_plan)


def emit_ambulance_event(
    request_id,
    ambulance,
    latitude,
    longitude,
    phase,
    remaining_seconds,
    speed_kmph,
    route_mode,
    hospital_name,
    destination_name,
    destination_lat=None,
    destination_lng=None,
    route_polyline=None,
    route_duration_seconds=None,
    route_distance_meters=None,
):
    payload = {
        "request_id": request_id,
        "id": ambulance["id"],
        "lat": latitude,
        "lng": longitude,
        "phase": phase,
        "speed_kmph": speed_kmph,
        "remaining_seconds": remaining_seconds,
        "route_mode": route_mode,
        "hospital_name": hospital_name,
        "destination_name": destination_name,
    }
    if destination_lat is not None and destination_lng is not None:
        payload["destination_lat"] = destination_lat
        payload["destination_lng"] = destination_lng
    if route_polyline is not None:
        payload["route_polyline"] = route_polyline
    if route_duration_seconds is not None:
        payload["route_duration_seconds"] = route_duration_seconds
    if route_distance_meters is not None:
        payload["route_distance_meters"] = route_distance_meters

    socketio.emit(
        "ambulance_location",
        payload,
        to=f"request:{request_id}",
    )


def drive_phase(
    request_id,
    ambulance,
    origin,
    destination,
    phase,
    route_plan,
    hospital_name,
    destination_name,
    next_leg_remaining_seconds=0,
):
    resolved_route_plan, updates = build_phase_updates(origin, destination, route_plan)
    route_duration_seconds = int(round(float(resolved_route_plan.get("duration_seconds") or 0.0)))
    route_distance_meters = int(round(float(resolved_route_plan.get("distance_meters") or 0.0)))
    initial_speed_kmph = round(
        max(
            1.0,
            (float(resolved_route_plan.get("distance_meters") or 0.0) / max(float(resolved_route_plan.get("duration_seconds") or 1.0), 1.0)) * 3.6,
        ),
        1,
    )
    set_ambulance_progress(
        ambulance=ambulance,
        phase=phase,
        remaining_seconds=route_duration_seconds + max(0, int(round(next_leg_remaining_seconds))),
        destination_name=destination_name,
    )
    update_dispatch_request(
        request_id,
        ambulance_id=ambulance["id"],
        status="active",
        phase=phase,
    )
    emit_ambulance_event(
        request_id=request_id,
        ambulance=ambulance,
        latitude=origin[0],
        longitude=origin[1],
        phase=phase,
        remaining_seconds=route_duration_seconds,
        speed_kmph=initial_speed_kmph,
        route_mode=resolved_route_plan.get("mode", "simulated"),
        hospital_name=hospital_name,
        destination_name=destination_name,
        destination_lat=destination[0],
        destination_lng=destination[1],
        route_polyline=resolved_route_plan.get("overview_polyline", ""),
        route_duration_seconds=route_duration_seconds,
        route_distance_meters=route_distance_meters,
    )
    emit_fleet_update()

    for update in updates:
        latitude = update["lat"]
        longitude = update["lng"]
        with dispatch_lock:
            ambulance["lat"] = latitude
            ambulance["lng"] = longitude
            ambulance["phase"] = phase
            ambulance["available_in_seconds"] = max(
                0,
                int(round(update["remaining_seconds"] + max(0, int(round(next_leg_remaining_seconds))))),
            )
            ambulance["destination_name"] = destination_name
            with get_db_connection() as connection:
                connection.execute(
                    """
                    UPDATE ambulances
                    SET lat = ?, lng = ?, phase = ?, available_in_seconds = ?, destination_name = ?
                    WHERE id = ?
                    """,
                    (
                        ambulance["lat"],
                        ambulance["lng"],
                        ambulance["phase"],
                        ambulance["available_in_seconds"],
                        ambulance["destination_name"],
                        ambulance["id"],
                    ),
                )
                connection.commit()

        emit_ambulance_event(
            request_id=request_id,
            ambulance=ambulance,
            latitude=latitude,
            longitude=longitude,
            phase=phase,
            remaining_seconds=update["remaining_seconds"],
            speed_kmph=update["speed_kmph"],
            route_mode=resolved_route_plan.get("mode", "simulated"),
            hospital_name=hospital_name,
            destination_name=destination_name,
            destination_lat=destination[0],
            destination_lng=destination[1],
        )
        emit_fleet_update()

        if update["remaining_seconds"] > 0:
            time.sleep(update["delay_seconds"])

    with dispatch_lock:
        ambulance["lat"] = destination[0]
        ambulance["lng"] = destination[1]

    return resolved_route_plan


def move_ambulance(request_id, ambulance, patient, hospital, route_to_patient=None, route_to_hospital=None):
    patient_origin = (ambulance["lat"], ambulance["lng"])
    hospital_leg_seconds = int(round(float((route_to_hospital or {}).get("duration_seconds") or 0.0)))
    drive_phase(
        request_id=request_id,
        ambulance=ambulance,
        origin=patient_origin,
        destination=patient,
        phase="to_patient",
        route_plan=route_to_patient,
        hospital_name=hospital["name"],
        destination_name="Pickup place",
        next_leg_remaining_seconds=hospital_leg_seconds,
    )

    scene_origin = (ambulance["lat"], ambulance["lng"])
    recalculated_hospital, recalculated_hospital_route = get_best_hospital_route(scene_origin)
    if recalculated_hospital and recalculated_hospital_route:
        hospital = recalculated_hospital
        route_to_hospital = recalculated_hospital_route
    elif route_to_hospital is None:
        hospital_destination = (hospital["lat"], hospital["lng"])
        route_to_hospital = build_route_plan(scene_origin, hospital_destination)

    hospital_destination = (hospital["lat"], hospital["lng"])
    update_dispatch_request(request_id, hospital=hospital, status="active", phase="to_hospital")
    drive_phase(
        request_id=request_id,
        ambulance=ambulance,
        origin=scene_origin,
        destination=hospital_destination,
        phase="to_hospital",
        route_plan=route_to_hospital,
        hospital_name=hospital["name"],
        destination_name=hospital["name"],
        next_leg_remaining_seconds=0,
    )

    with dispatch_lock:
        ambulance["status"] = "available"
        ambulance["phase"] = "idle"
        ambulance["available_in_seconds"] = 0
        ambulance["destination_name"] = hospital["name"]
        ambulance["lat"] = hospital_destination[0]
        ambulance["lng"] = hospital_destination[1]
        with get_db_connection() as connection:
            connection.execute(
                """
                UPDATE ambulances
                SET status = ?, phase = ?, available_in_seconds = ?, destination_name = ?, lat = ?, lng = ?
                WHERE id = ?
                """,
                (
                    ambulance["status"],
                    ambulance["phase"],
                    ambulance["available_in_seconds"],
                    ambulance["destination_name"],
                    ambulance["lat"],
                    ambulance["lng"],
                    ambulance["id"],
                ),
            )
            connection.commit()
    update_dispatch_request(request_id, status="completed", phase="completed", hospital=hospital, completed=True)

    emit_ambulance_event(
        request_id=request_id,
        ambulance=ambulance,
        latitude=hospital_destination[0],
        longitude=hospital_destination[1],
        phase="completed",
        remaining_seconds=0,
        speed_kmph=0.0,
        route_mode="completed",
        hospital_name=hospital["name"],
        destination_name=hospital["name"],
        destination_lat=hospital_destination[0],
        destination_lng=hospital_destination[1],
    )
    emit_fleet_update()


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/admin")
@app.get("/admin/dashboard")
def admin_dashboard():
    return render_template("dashboard.html")


@app.get("/admin/api/overview")
def admin_overview():
    return jsonify(build_dashboard_payload())


@app.get("/fleet_status")
def fleet_status():
    return jsonify(build_fleet_payload())


@app.get("/request_status/<request_id>")
def request_status(request_id):
    dispatch_request = get_dispatch_request(request_id)
    if dispatch_request is None:
        return jsonify({"error": "Request not found"}), 404

    return jsonify(dispatch_request)


@socketio.on("subscribe_request")
def subscribe_request(payload):
    request_id = None
    if isinstance(payload, dict):
        request_id = payload.get("request_id")
    if request_id:
        join_room(f"request:{request_id}")


@socketio.on("subscribe_dashboard")
def subscribe_dashboard(_payload=None):
    join_room("dashboard")
    socketio.emit("dashboard_update", build_dashboard_payload(), to="dashboard")


@app.post("/request_ambulance")
def request_ambulance():
    data = request.get_json(force=True)
    patient = (data["lat"], data["lng"])
    requester_id = data.get("requester_id")

    hospital, hospital_route_plan = get_best_hospital_route(patient)
    if not hospital or not hospital_route_plan:
        return jsonify({"error": "Could not determine the nearest hospital route"}), 500

    request_id = create_dispatch_request_record(
        requester_id=requester_id,
        patient=patient,
        hospital=hospital,
        status="searching",
        phase="searching",
    )

    claimed_ambulance = None
    assigned_ambulance = None
    route_to_patient = None
    dispatch_message = None
    ranked_candidates = get_ranked_ambulance_routes(patient)
    nearest_route = ranked_candidates[0] if ranked_candidates else None

    for ambulance_candidate, candidate_route_plan in ranked_candidates:
        if ambulance_candidate["status"] != "available":
            continue

        claimed_ambulance, assigned_ambulance = claim_ambulance(ambulance_candidate["id"])
        if claimed_ambulance is not None:
            route_to_patient = candidate_route_plan
            break

    if claimed_ambulance is None or assigned_ambulance is None or route_to_patient is None:
        update_dispatch_request(
            request_id,
            hospital=hospital,
            status="busy",
            phase="busy",
            error_message="All ambulances are busy right now",
        )
        return jsonify(
            {
                "request_id": request_id,
                "error": "All ambulances are busy right now",
                "busy": True,
                **build_fleet_payload(),
            }
        ), 409

    route_duration_seconds = int(round(float(route_to_patient.get("duration_seconds") or 0.0)))
    route_distance_meters = int(round(float(route_to_patient.get("distance_meters") or 0.0)))
    hospital_route_duration_seconds = int(round(float(hospital_route_plan.get("duration_seconds") or 0.0)))
    hospital_route_distance_meters = int(round(float(hospital_route_plan.get("distance_meters") or 0.0)))
    initial_speed_kmph = round(max(1.0, (route_distance_meters / max(route_duration_seconds, 1)) * 3.6), 1)
    eta_to_patient = max(1, math.ceil(route_duration_seconds / 60))
    eta_to_hospital = max(1, math.ceil(hospital_route_duration_seconds / 60))
    if nearest_route is not None:
        nearest_ambulance, nearest_route_plan = nearest_route
        if nearest_ambulance["id"] != assigned_ambulance["id"]:
            nearest_eta_minutes = max(1, math.ceil(float(nearest_route_plan.get("duration_seconds") or 0.0) / 60))
            dispatch_message = (
                f"Nearest ambulance Unit #{nearest_ambulance['id']} is busy. "
                f"Dispatching Unit #{assigned_ambulance['id']} instead, so response may take around {eta_to_patient} min."
            )

    set_ambulance_progress(
        ambulance=claimed_ambulance,
        phase="to_patient",
        remaining_seconds=route_duration_seconds + hospital_route_duration_seconds,
        destination_name="Pickup place",
    )
    update_dispatch_request(
        request_id,
        ambulance_id=assigned_ambulance["id"],
        hospital=hospital,
        status="assigned",
        phase="to_patient",
        eta_to_patient=eta_to_patient,
        eta_to_hospital=eta_to_hospital,
        error_message=dispatch_message,
    )
    emit_fleet_update()

    threading.Thread(
        target=move_ambulance,
        args=(request_id, claimed_ambulance, patient, hospital, route_to_patient, hospital_route_plan),
        daemon=True
    ).start()

    return jsonify(
        {
            "request_id": request_id,
            "ambulance_id": assigned_ambulance["id"],
            "ambulance_lat": assigned_ambulance["lat"],
            "ambulance_lng": assigned_ambulance["lng"],
            "phase": "to_patient",
            "eta_to_patient": eta_to_patient,
            "route_duration_seconds": route_duration_seconds,
            "route_distance_meters": route_distance_meters,
            "route_polyline": route_to_patient["overview_polyline"],
            "route_mode": route_to_patient["mode"],
            "route_polyline_to_patient": route_to_patient["overview_polyline"],
            "route_mode_to_patient": route_to_patient["mode"],
            "initial_speed_kmph": initial_speed_kmph,
            "hospital": hospital["name"],
            "hospital_lat": hospital["lat"],
            "hospital_lng": hospital["lng"],
            "eta_to_hospital": eta_to_hospital,
            "route_duration_seconds_to_hospital": hospital_route_duration_seconds,
            "route_distance_meters_to_hospital": hospital_route_distance_meters,
            "route_polyline_to_hospital": hospital_route_plan["overview_polyline"],
            "route_mode_to_hospital": hospital_route_plan["mode"],
            "route_provider_to_hospital": hospital_route_plan.get("provider", "fallback"),
            "route_provider_to_patient": route_to_patient.get("provider", "fallback"),
            "priority_message": dispatch_message,
            "selected_due_to_busy": dispatch_message is not None,
        }
    )


if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, allow_unsafe_werkzeug=True)
