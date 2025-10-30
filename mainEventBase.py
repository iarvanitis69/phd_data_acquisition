#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dynamic station-radius downloader for Santorini region (πολλαπλά έτη)
----------------------------------------------------------------------
Κατεβάζει σεισμούς από το EMSC (μόνο >4.0) και waveforms/stations από EIDA.
Το info.txt εμπλουτίστηκε με πλήρη πληροφορία EventInfo:
  - σύστημα/μέθοδος υπολογισμού επικέντρου
  - agency
  - evaluation mode/status
  - τύπος origin, phase arrivals (P, S, PKS)
  - και αναλυτική ερμηνεία μεθόδου (HypoInverse, Hypo71, κ.λπ.)
"""

import os
import sys
import shutil
from obspy import UTCDateTime
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.mass_downloader import MassDownloader, CircularDomain, Restrictions


# ==========================================
# Command-line arguments για ΕΤΟΣ_ΑΡΧΗΣ και ΕΤΟΣ_ΤΕΛΟΥΣ
# ==========================================
if len(sys.argv) >= 3:
    try:
        START_YEAR = int(sys.argv[1])
        END_YEAR = int(sys.argv[2])
    except ValueError:
        print("⚠️ Invalid arguments. Χρήση: python main.py 2010 2012")
        sys.exit(1)
else:
    print("⚠️ Δεν δόθηκαν σωστά έτη. Χρήση: python main.py 2010 2012")
    sys.exit(1)

print(f"🗕️ Λήψη σεισμών από {START_YEAR} έως {END_YEAR}")

# ==========================================
# BASE DIRECTORY (αποθήκευση events και κοινών stations)
# ==========================================
BASE_EVENTS_DIR = "/media/iarv/Samsung/Events"
SHARED_STATION_DIR = os.path.join(BASE_EVENTS_DIR, "Stations")
os.makedirs(SHARED_STATION_DIR, exist_ok=True)

# ==========================================
# PARAMETERS ΠΕΡΙΟΧΗΣ
# ==========================================
REF_LAT = 36.618712   # Σαντορίνη
REF_LON = 25.682873
MAX_EVENT_RADIUS_KM = 50
MIN_MAG_FOR_EXTERNAL = 3.0

NOA_ONLY = ["https://eida.gein.noa.gr"]
EXTERNAL_PROVIDERS = [
    "https://eida.gein.noa.gr",
    # "https://eida.koeri.boun.edu.tr",
    # "https://eida.ingv.it",
    # "https://eida.niep.ro"
]


def magnitude_to_radius_linear(mag):
    km_min, km_max = 10.0, 50.0
    return round(km_max / 111.19, 2)


def get_local_events(year):
    start = UTCDateTime(f"{year}-01-01T00:00:00")
    end = UTCDateTime(f"{year}-12-31T23:59:59")
    client = Client("EMSC")
    print(f"📱 Αναζήτηση σεισμών για {year}...")

    events = client.get_events(
        starttime=start,
        endtime=end,
        latitude=REF_LAT,
        longitude=REF_LON,
        maxradius=MAX_EVENT_RADIUS_KM / 111.19,
        minmagnitude=MIN_MAG_FOR_EXTERNAL,
        maxmagnitude=10.0
    )

    return sorted(events, key=lambda ev: ev.origins[0].time, reverse=True)


def download_waveforms(events, year, base_dir=BASE_EVENTS_DIR, pre=30, post=180, channel="HH*"):
    year_dir = os.path.join(base_dir, str(year))
    os.makedirs(year_dir, exist_ok=True)
    total = len(events)
    downloaded = 0

    for ev in events:
        o = ev.origins[0]
        event_time = o.time
        event_lat, event_lon = o.latitude, o.longitude
        event_depth_km = round(o.depth / 1000, 1) if o.depth else 0
        mag = ev.magnitudes[0].mag if ev.magnitudes else 0.0

        station_radius = magnitude_to_radius_linear(mag)
        event_id = f"{event_time.strftime('%Y%m%dT%H%M%S')}_{event_lat:.2f}_{event_lon:.2f}_{event_depth_km:.1f}km_M{mag:.1f}"

        final_dir = os.path.join(year_dir, event_id)
        info_txt_path = os.path.join(final_dir, "info.txt")
        os.makedirs(final_dir, exist_ok=True)

        if os.path.exists(final_dir) and any(
            fname.endswith(".mseed") for root, _, files in os.walk(final_dir) for fname in files
        ):
            print(f"⏩ Παράλειψη {event_id} – ήδη κατεβασμένο")
            continue

        print(f"\n🌐 Κατέβασμα Event: {event_id}")
        print(f"   ➔ Magnitude: {mag:.1f}")
        print(f"   ➔ Station radius: {station_radius:.2f}°")

        domain = CircularDomain(latitude=event_lat, longitude=event_lon, minradius=0.0, maxradius=station_radius)
        restrictions = Restrictions(
            starttime=event_time - pre,
            endtime=event_time + post,
            network="*",
            station="*",
            channel=channel,
            reject_channels_with_gaps=True,
            sanitize=True
        )

        temp_dir = os.path.join(final_dir, "_temp")
        os.makedirs(temp_dir, exist_ok=True)
        success = False
        successful_providers = []

        for provider in EXTERNAL_PROVIDERS:
            print(f"   🔗 Λήψη από: {provider}")
            try:
                mdl = MassDownloader(providers=[provider])
                mdl.download(
                    domain=domain,
                    restrictions=restrictions,
                    mseed_storage=os.path.join(temp_dir, "mseed"),
                    stationxml_storage=os.path.join(temp_dir, "Stations"),
                    threads_per_client=3,
                    print_report=False
                )
                if os.path.exists(os.path.join(temp_dir, "mseed")) and any(
                    f.endswith(".mseed") for f in os.listdir(os.path.join(temp_dir, "mseed"))
                ):
                    success = True
                    successful_providers.append(provider)
                    break
            except Exception as e:
                print(f"   ⚠️ Σφάλμα από {provider}: {e}")

        if not success:
            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"   🚫 Καμία καταγραφή – παράλειψη")
            continue

        # Μεταφορά των .mseed στους σταθμούς
        temp_mseed = os.path.join(temp_dir, "mseed")
        for fname in os.listdir(temp_mseed):
            parts = fname.split(".")
            if len(parts) >= 2:
                network_code = parts[0]
                station_code = parts[1]
                net_sta = f"{network_code}.{station_code}"
                station_dir = os.path.join(final_dir, net_sta)
                os.makedirs(os.path.join(station_dir, "mseed"), exist_ok=True)
                shutil.move(os.path.join(temp_mseed, fname), os.path.join(station_dir, "mseed", fname))

        # Μεταφορά .xml στο κοινό Stations dir
        temp_stations = os.path.join(temp_dir, "Stations")
        for xmlfile in os.listdir(temp_stations):
            xml_src = os.path.join(temp_stations, xmlfile)
            xml_dst = os.path.join(SHARED_STATION_DIR, xmlfile)
            if not os.path.exists(xml_dst):
                shutil.copy(xml_src, xml_dst)

        shutil.rmtree(temp_dir, ignore_errors=True)

        # ==============================
        # 🔍 Εμπλουτισμένο info.txt με ερμηνεία HypoInverse
        # ==============================
        origin = ev.origins[0]
        creation = getattr(origin, "creation_info", None)
        agency = getattr(creation, "agency_id", "Unknown")
        method_id = str(getattr(origin, "method_id", "Unknown"))
        origin_type = str(getattr(origin, "type", "Unknown"))
        eval_mode = getattr(origin, "evaluation_mode", "automatic")
        eval_status = getattr(origin, "evaluation_status", "preliminary")
        earth_model = getattr(origin, "earth_model_id", "Unknown")

        # Έλεγχος για PKS, P, S κύματα
        used_phases = set()
        if hasattr(origin, "arrivals"):
            for arr in origin.arrivals:
                if hasattr(arr, "phase") and arr.phase:
                    used_phases.add(arr.phase.upper())

        # ➤ Λογική ερμηνεία μεθόδου
        if "hypoinverse" in method_id.lower():
            method_label = "HypoInverse (arrival-time method)"
        elif "hypo71" in method_id.lower():
            method_label = "Hypo71 (arrival-time method)"
        elif method_id == "Unknown" or "NA" in method_id:
            method_label = "Undeclared (likely HypoInverse or similar local algorithm)"
        else:
            method_label = method_id

        # ➤ Περιγραφή φάσεων
        if used_phases:
            phase_list = ", ".join(sorted(used_phases))
            if "PKS" in used_phases:
                phase_comment = "⚙️ PKS phases were used in epicenter determination."
            else:
                phase_comment = "⚙️ PKS phases not reported — likely only P and S arrivals used."
        else:
            phase_list = "Unknown"
            phase_comment = "⚙️ No phase data reported — possibly automatic centroid solution."

        # ✍️ Δημιουργία του αρχείου info.txt
        with open(info_txt_path, "w") as info:
            info.write("==== EVENT INFORMATION ====\n")
            info.write(f"Event ID: {event_id}\n")
            info.write(f"Time (UTC): {event_time}\n")
            info.write(f"Latitude: {event_lat}\n")
            info.write(f"Longitude: {event_lon}\n")
            info.write(f"Depth (km): {event_depth_km}\n")
            info.write(f"Magnitude: {mag:.1f}\n")
            info.write(f"Downloaded from: {', '.join(successful_providers)}\n")
            info.write("\n==== EPICENTER DETERMINATION ====\n")
            info.write(f"Agency: {agency}\n")
            info.write(f"Method: {method_label}\n")
            info.write(f"Origin Type: {origin_type}\n")
            info.write(f"Evaluation Mode: {eval_mode}\n")
            info.write(f"Evaluation Status: {eval_status}\n")
            info.write(f"Earth Model: {earth_model}\n")
            info.write(f"Used Phases: {phase_list}\n")
            info.write(f"{phase_comment}\n")

            # ➤ Ανάλυση υπολογισμού επικέντρου
            info.write("\n==== INTERPRETATION ====\n")
            if "HypoInverse" in method_label or "Hypo71" in method_label:
                info.write("Local epicenter determined via arrival-time inversion of P and S phases.\n")
            elif "Undeclared" in method_label:
                info.write("Method not declared — likely computed by a local network using P and S arrivals (e.g. HypoInverse).\n")
            else:
                info.write("Epicenter method explicitly provided by agency.\n")

            info.write("\n==== DATA ATTRIBUTES ====\n")
            info.write(f"Resource ID: {ev.resource_id}\n")
            info.write(f"Origin Resource ID: {origin.resource_id}\n")
            info.write(f"Creation Time: {getattr(creation, 'creation_time', 'Unknown')}\n")
            info.write(f"Author: {getattr(creation, 'author', 'Unknown')}\n")

        downloaded += 1
        print(f"   ✅ Ολοκληρώθηκε: {event_id}")

    print(f"📊 Έτος {year}: {downloaded}/{total} γεγονότα")


def main():
    for year in range(START_YEAR, END_YEAR + 1):
        events = get_local_events(year)
        download_waveforms(events, year)
    print("\n📅 Λήψη ολοκληρώθηκε.")


if __name__ == "__main__":
    main()
