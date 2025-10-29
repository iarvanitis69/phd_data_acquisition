#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Continuous ημερήσιος downloader (single-pass, with FDSN fail-stop)
-------------------------------------------------------------------
• Πάει στην επόμενη ημέρα μόνο όταν όλοι οι σταθμοί της τρέχουσας έχουν DONE.flag
• Κατεβάζει μόνο σταθμούς με πλήρη κανάλια: HHN, HHE, HHZ
• Δεν δημιουργεί day folders αν δεν κατεβάζει
• Ενιαίος κατάλογος Stations/ με Stations_All.json & Stations_All.xml (χωρίς διπλότυπα)
• Σταματάει αμέσως αν ο FDSN server δεν ανταποκρίνεται (Bad Gateway / Timeout / ConnectionError)
• Δημιουργεί υποκατάλογο EventsInfo/ με QuakeML και method.json για κάθε ημέρα
"""

import os
import json
import logging
from datetime import datetime, timedelta, date, timezone
from obspy import UTCDateTime, read_events
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.mass_downloader import MassDownloader, CircularDomain, Restrictions
import requests

# ==========================
# ΡΥΘΜΙΣΕΙΣ
BASE_DIR = "/media/iarv/Samsung/Continuous_3"
START_DATE = "2012-01-01"
END_DATE   = "2012-12-31"

REF_LAT = 36.618712
REF_LON = 25.682873
STATION_RADIUS_DEG = 1.82
PROVIDER = "https://eida.gein.noa.gr"
NETWORKS = "HL,HA,HC"
CHANNELS_REQUIRED = {"HHN", "HHE", "HHZ"}

# ==========================
# ΦΑΚΕΛΟΙ & ΑΡΧΕΙΑ
LOGS_DIR     = os.path.join(BASE_DIR, "logs")
STATIONS_DIR = os.path.join(BASE_DIR, "Stations")
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(STATIONS_DIR, exist_ok=True)

STATIONS_JSON = os.path.join(STATIONS_DIR, "Stations_All.json")
STATIONS_XML  = os.path.join(STATIONS_DIR, "Stations_All.xml")

ACQUISITION_LOG = os.path.join(LOGS_DIR, "acquisitionLogs.log")
ERRORS_LOG      = os.path.join(LOGS_DIR, "acquisitionErrors.log")

# ==========================
# Σίγαση verbose logs ObsPy
logging.getLogger("obspy.clients.fdsn.mass_downloader").setLevel(logging.WARNING)
logging.getLogger("obspy.clients.fdsn.client").setLevel(logging.WARNING)


def log_append(file_path, message):
    with open(file_path, "a") as lf:
        lf.write(f"{datetime.now(timezone.utc)} | {message}\n")


def _load_existing_stations_json():
    if os.path.exists(STATIONS_JSON):
        try:
            with open(STATIONS_JSON, "r") as jf:
                return json.load(jf)
        except Exception:
            return []
    return []


def _merge_inventory_file(inv_to_add):
    """Συγχωνεύει inv_to_add μέσα στο STATIONS_XML χωρίς διπλότυπα."""
    from obspy import read_inventory
    if not os.path.exists(STATIONS_XML):
        inv_to_add.write(STATIONS_XML, format="STATIONXML")
        return
    old_inv = read_inventory(STATIONS_XML)
    merged  = old_inv + inv_to_add
    uniq = merged.__class__()
    seen = set()
    for net in merged:
        for sta in net:
            key = (net.code, sta.code)
            if key in seen:
                continue
            subset = merged.select(network=net.code, station=sta.code)
            if subset:
                uniq += subset
                seen.add(key)
    uniq.write(STATIONS_XML, format="STATIONXML")


def save_station_metadata_if_new(inv):
    """Αποθήκευση metadata (JSON + XML) για νέους σταθμούς."""
    existing = _load_existing_stations_json()
    existing_codes = {(d["network"], d["station"]) for d in existing}
    added_any = False
    for n in inv:
        for s in n:
            key = (n.code, s.code)
            if key in existing_codes:
                continue
            for c in s:
                resp = c.response
                sens_val = sens_freq = stage_gain = None
                poles = zeros = []
                if resp:
                    if resp.instrument_sensitivity:
                        sens_val = resp.instrument_sensitivity.value
                        sens_freq = resp.instrument_sensitivity.frequency
                    if len(resp.response_stages) > 0:
                        stage = resp.response_stages[0]
                        if hasattr(stage, "poles"):
                            poles = [complex(p).__repr__() for p in stage.poles]
                        if hasattr(stage, "zeros"):
                            zeros = [complex(z).__repr__() for z in stage.zeros]
                        stage_gain = getattr(stage, "stage_gain", None)
                existing.append({
                    "network": n.code,
                    "station": s.code,
                    "channel": c.code,
                    "location_code": c.location_code,
                    "latitude": s.latitude,
                    "longitude": s.longitude,
                    "elevation_m": s.elevation,
                    "depth_m": getattr(c, "depth", None),
                    "start_date": str(c.start_date),
                    "end_date": str(c.end_date),
                    "sampling_rate": c.sample_rate,
                    "sensor_model": getattr(c.sensor, "model", None) if c.sensor else None,
                    "sensor_manufacturer": getattr(c.sensor, "manufacturer", None) if c.sensor else None,
                    "data_logger_model": getattr(c.data_logger, "model", None) if c.data_logger else None,
                    "sensitivity_value": sens_val,
                    "sensitivity_frequency": sens_freq,
                    "stage_gain": stage_gain,
                    "poles": poles,
                    "zeros": zeros
                })
            existing_codes.add(key)
            added_any = True
            print(f"   ✅ Προστέθηκε στο Stations catalog: {n.code}.{s.code}")
    if added_any:
        with open(STATIONS_JSON, "w") as jf:
            json.dump(existing, jf, indent=2)
        _merge_inventory_file(inv)


def get_all_stations_with_full_channels(for_date: date):
    """Επιστρέφει (λίστα σταθμών, πλήρες inventory) για τη συγκεκριμένη ημέρα.
       Αν ο FDSN server δεν ανταποκρίνεται → σταματά το πρόγραμμα."""
    client = Client("NOA")
    starttime = UTCDateTime(datetime.combine(for_date, datetime.min.time()))
    endtime   = UTCDateTime(datetime.combine(for_date, datetime.max.time()))
    try:
        inv = client.get_stations(
            starttime=starttime,
            endtime=endtime,
            latitude=REF_LAT,
            longitude=REF_LON,
            maxradius=STATION_RADIUS_DEG,
            network=NETWORKS,
            level="response"
        )
    except requests.exceptions.RequestException as e:
        print(f"⛔ Σφάλμα δικτύου: {e}\n   Διακόπτω το πρόγραμμα (πιθανή διακοπή σύνδεσης ή Bad Gateway).")
        raise SystemExit(1)
    except Exception as e:
        if "bad gateway" in str(e).lower() or "502" in str(e):
            print(f"⛔ Διακοπή: FDSN server απέστειλε Bad Gateway (502).")
            raise SystemExit(1)
        raise

    result = []
    for n in inv:
        for s in n:
            chans = {c.code for c in s}
            if CHANNELS_REQUIRED.issubset(chans):
                result.append((n.code, s.code, s.latitude, s.longitude, s.elevation))
            else:
                missing = CHANNELS_REQUIRED - chans
                print(f"   ⚠️ Παράλειψη {n.code}.{s.code} (λείπουν: {', '.join(sorted(missing))})")
    return result, inv


def _day_done_flag_path(year_dir, net, sta, day):
    return os.path.join(year_dir, f"{net}.{sta}", day.strftime("%Y-%m-%d"), "DONE.flag")


def is_day_complete(day: date):
    """Η ημέρα είναι complete όταν όλοι οι σταθμοί της έχουν DONE.flag."""
    try:
        stations, _ = get_all_stations_with_full_channels(day)
    except SystemExit:
        raise
    except Exception as e:
        print(f"❌ Αδυναμία ανάκτησης σταθμών για {day}: {e}")
        return False
    if not stations:
        return True
    year_dir = os.path.join(BASE_DIR, str(day.year))
    for (net, sta, *_rest) in stations:
        done_flag = _day_done_flag_path(year_dir, net, sta, day)
        if not os.path.exists(done_flag):
            return False
    return True


def find_first_incomplete_day(start_date: date, end_date: date):
    """Επιστρέφει την πρώτη ημέρα που δεν είναι complete."""
    day = start_date
    while day <= end_date:
        if not is_day_complete(day):
            return day
        day += timedelta(days=1)
    return None


def download_daily_waveforms(start_date_str, end_date_str):
    start_day = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_day   = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    first_incomplete = find_first_incomplete_day(start_day, end_day)
    if first_incomplete is None:
        print("✅ Όλες οι ημέρες μέχρι το END_DATE είναι ολοκληρωμένες.")
        return

    current_day = first_incomplete
    print(f"⏩ Ξεκινώ από την πρώτη μη-ολοκληρωμένη ημέρα: {current_day}")

    while current_day <= end_day:
        print(f"\n📅 Ημέρα: {current_day}")
        try:
            stations_all, full_inventory = get_all_stations_with_full_channels(current_day)
        except SystemExit:
            return
        except Exception as e:
            print(f"❌ Αδυναμία λήψης σταθμών για {current_day}: {e}")
            break

        if not stations_all:
            print(f"⚠️ Δεν βρέθηκαν σταθμοί για {current_day}.")
            current_day += timedelta(days=1)
            continue

        year_dir = os.path.join(BASE_DIR, str(current_day.year))
        os.makedirs(year_dir, exist_ok=True)
        total = len(stations_all)
        client = Client("NOA")

        for i, (net, sta, lat, lon, elev) in enumerate(stations_all, 1):
            done_flag = _day_done_flag_path(year_dir, net, sta, current_day)
            if os.path.exists(done_flag):
                print(f"   ✅ Ήδη ΟΚ: {net}.{sta} ({current_day})")
                continue

            try:
                inv_station = full_inventory.select(network=net, station=sta)
                save_station_metadata_if_new(inv_station)

                station_dir = os.path.join(year_dir, f"{net}.{sta}")
                day_dir     = os.path.join(station_dir, current_day.strftime("%Y-%m-%d"))
                os.makedirs(station_dir, exist_ok=True)
                os.makedirs(day_dir, exist_ok=True)

                st_start = UTCDateTime(datetime.combine(current_day, datetime.min.time()))
                st_end   = st_start + 24 * 3600
                domain = CircularDomain(latitude=lat, longitude=lon,
                                        minradius=0.0, maxradius=STATION_RADIUS_DEG)
                restrictions = Restrictions(
                    starttime=st_start,
                    endtime=st_end - 1,
                    network=net,
                    station=sta,
                    channel="HH*",
                    reject_channels_with_gaps=True,
                    chunklength_in_sec=86400,
                    minimum_length=0.95,
                    sanitize=True
                )

                print(f"[{i}/{total}] 📥 Κατεβάζω: {net}.{sta} | HHN,HHE,HHZ | {current_day}")
                mdl = MassDownloader(providers=[PROVIDER])
                mdl.download(domain=domain, restrictions=restrictions,
                             mseed_storage=day_dir,
                             stationxml_storage=STATIONS_DIR,
                             threads_per_client=3,
                             print_report=False)

                # =======================================================
                # 🧭 Λήψη και αποθήκευση Event Catalog & Method Info
                # =======================================================
                try:
                    event_dir = os.path.join(day_dir, "EventsInfo")
                    os.makedirs(event_dir, exist_ok=True)

                    cat = client.get_events(
                        starttime=st_start,
                        endtime=st_end,
                        latitude=REF_LAT,
                        longitude=REF_LON,
                        maxradius=STATION_RADIUS_DEG,
                        minmagnitude=0.0
                    )

                    if len(cat) > 0:
                        xml_path = os.path.join(event_dir, f"events_{current_day}.xml")
                        cat.write(xml_path, format="QUAKEML")
                        print(f"   🧾 Αποθηκεύτηκε Event Catalog: {xml_path}")

                        info_list = []
                        for ev in cat:
                            orig = ev.preferred_origin() or (ev.origins[0] if ev.origins else None)
                            if not orig:
                                continue
                            phases = [arr.phase for arr in getattr(orig, "arrivals", [])]
                            info = {
                                "event_id": ev.resource_id.id if ev.resource_id else None,
                                "time": str(orig.time),
                                "latitude": getattr(orig, "latitude", None),
                                "longitude": getattr(orig, "longitude", None),
                                "depth_km": getattr(orig.depth, "value", None) / 1000.0 if getattr(orig, "depth", None) else None,
                                "method": str(getattr(orig, "method_id", None)),
                                "earth_model": str(getattr(orig, "earth_model_id", None)),
                                "used_phase_count": getattr(orig.quality, "used_phase_count", None) if orig.quality else None,
                                "used_station_count": getattr(orig.quality, "used_station_count", None) if orig.quality else None,
                                "standard_error": getattr(orig.quality, "standard_error", None) if orig.quality else None,
                                "azimuthal_gap": getattr(orig.quality, "azimuthal_gap", None) if orig.quality else None,
                                "unique_phases": sorted(list(set(phases))) if phases else [],
                            }
                            info_list.append(info)

                        json_path = os.path.join(event_dir, "method.json")
                        with open(json_path, "w") as jf:
                            json.dump(info_list, jf, indent=2)
                        print(f"   💾 Αποθηκεύτηκαν πληροφορίες μεθόδου: {json_path}")
                    else:
                        print(f"   ⚠️ Δεν βρέθηκαν γεγονότα για {current_day} στην περιοχή.")
                except Exception as e:
                    print(f"   ⚠️ Αδυναμία λήψης ή αποθήκευσης event info: {e}")

                with open(done_flag, "w") as df:
                    df.write(f"Completed at {datetime.now(timezone.utc)}\n")
                log_append(ACQUISITION_LOG, f"OK | {net}.{sta} | {current_day}")
                print(f"   ✅ Ολοκληρώθηκε: {net}.{sta}")

            except Exception as e:
                msg = f"ERROR | {net}.{sta} | {current_day} | {e}"
                log_append(ERRORS_LOG, msg)
                log_append(ACQUISITION_LOG, msg)
                print(f"   ❌ Σφάλμα {net}.{sta}: {e}")

        if is_day_complete(current_day):
            print(f"🟢 Ημέρα {current_day} ολοκληρώθηκε πλήρως.")
            current_day += timedelta(days=1)
        else:
            print(f"⛔ Διακόπτω χωρίς αλλαγή ημέρας: απομένουν σταθμοί για {current_day}.")
            break

    print(f"\n✅ Τελευταία επεξεργασμένη ημέρα: {current_day}")


def main():
    download_daily_waveforms(START_DATE, END_DATE)


if __name__ == "__main__":
    main()
