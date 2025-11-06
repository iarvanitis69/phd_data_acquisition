import os
import sys
import shutil
import json
from obspy import UTCDateTime
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.mass_downloader import MassDownloader, CircularDomain, Restrictions

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

BASE_EVENTS_DIR = "/media/iarv/Samsung/Events3"
REF_LAT = 36.618712
REF_LON = 25.682873
MAX_EVENT_RADIUS_KM = 50
MIN_MAG_FOR_EXTERNAL = 3.0
EXTERNAL_PROVIDERS = ["https://eida.gein.noa.gr"]

def magnitude_to_radius_linear(mag):
    km_min, km_max = 10.0, 50.0
    return round(km_max / 111.19, 2)

# def extract_uncertainty(value_str):
#     if "[uncertainty=" in value_str:
#         try:
#             return float(value_str.split("[uncertainty=")[1].rstrip("]"))
#         except:
#             return None
#     return None

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
import re

def extract_uncertainty_from_field(field_obj):
    """
    Παίρνει το uncertainty από το string '[uncertainty=...]' αν υπάρχει.
    """
    s = str(field_obj)
    match = re.search(r'\[uncertainty=([\d\.]+)\]', s)
    return float(match.group(1)) if match else None

def download_waveforms(events, year, base_dir=BASE_EVENTS_DIR, pre=30, post=180, channel="HH*"):
    year_dir = os.path.join(base_dir, str(year))
    os.makedirs(year_dir, exist_ok=True)
    total = len(events)
    downloaded = 0

    for ev in events:
        o = ev.origins[0]
        event_time = o.time
        event_lat = o.latitude
        event_lon = o.longitude
        event_depth_km = round(o.depth / 1000, 1) if o.depth else 0
        mag = ev.magnitudes[0].mag if ev.magnitudes else 0.0

        lat_unc_val = o.longitude_errors.uncertainty
        lon_unc_val = o.latitude_errors.uncertainty
        time_unc_val = o.time_errors.uncertainty
        qual = getattr(o, "quality", None)
        if None == o.depth_errors.uncertainty:
            depth_unc_val = getattr(qual, "standard_error", None)
        else:
            depth_unc_val = o.depth_errors.uncertainty
        origin_unc = getattr(o, "origin_uncertainty", None)

        # lat_unc_val = getattr(lat_unc, "uncertainty", None)
        # lon_unc_val = getattr(lon_unc, "uncertainty", None)
        # time_unc_val = getattr(time_unc, "uncertainty", None)
        # depth_unc_val = getattr(depth_unc, "uncertainty", None)

        quality_dict = {
            "used_phase_count": getattr(qual, "used_phase_count", None),
            "used_station_count": getattr(qual, "used_station_count", None),
            "standard_error": getattr(qual, "standard_error", None),
            "azimuthal_gap": getattr(qual, "azimuthal_gap", None),
            "minimum_distance": getattr(qual, "minimum_distance", None),
            "maximum_distance": getattr(qual, "maximum_distance", None),
            "azimuth_max_horizontal_uncertainty": getattr(origin_unc, "azimuth_max_horizontal_uncertainty", None)
        }

        station_radius = magnitude_to_radius_linear(mag)
        event_id = f"{event_time.strftime('%Y%m%dT%H%M%S')}_{event_lat:.2f}_{event_lon:.2f}_{event_depth_km:.1f}km_M{mag:.1f}"
        final_dir = os.path.join(year_dir, event_id)
        info_json_path = os.path.join(final_dir, "info.json")

        if os.path.exists(final_dir) and any(
            fname.endswith(".mseed") for root, _, files in os.walk(final_dir) for fname in files
        ):
            print(f"⏩ Παράλειψη {event_id} – ήδη κατεβασμένο")
            continue

        print(f"\n🌐 Κατέβασμα Event: {event_id}")

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

        temp_dir = os.path.join(base_dir, "_temp_download")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
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
            print(f"   🚫 Καμία καταγραφή – δεν δημιουργείται φάκελος event.")
            continue

        os.makedirs(final_dir, exist_ok=True)

        for subfolder in ["mseed", "Stations"]:
            subdir = os.path.join(temp_dir, subfolder)
            if not os.path.exists(subdir):
                continue
            for fname in os.listdir(subdir):
                parts = fname.split(".")
                if len(parts) >= 2:
                    net_sta = f"{parts[0]}.{parts[1]}"
                    station_dir = os.path.join(final_dir, net_sta)
                    os.makedirs(station_dir, exist_ok=True)
                    shutil.move(os.path.join(subdir, fname), os.path.join(station_dir, fname))

        shutil.rmtree(temp_dir, ignore_errors=True)

        origin = o
        creation = getattr(origin, "creation_info", None)
        agency = getattr(creation, "agency_id", "Unknown")
        method_id = str(getattr(origin, "method_id", "Unknown"))
        origin_type = str(getattr(origin, "type", "Unknown"))
        eval_mode = getattr(origin, "evaluation_mode", "automatic")
        eval_status = getattr(origin, "evaluation_status", "preliminary")
        earth_model = getattr(origin, "earth_model_id", "Unknown")

        info_dict = {
            "event_id": event_id,
            "time_utc": str(event_time),
            "latitude": event_lat,
            "latitude_uncertainty_km": lat_unc_val,
            "longitude": event_lon,
            "longitude_uncertainty_km": lon_unc_val,
            "depth_km": event_depth_km,
            "depth_uncertainty_km": depth_unc_val,
            "time_uncertainty_sec": time_unc_val,
            "magnitude": mag,
            "downloaded_from": successful_providers,
            "epicenter_determination": {
                "agency": agency,
                "method": method_id,
                "origin_type": origin_type,
                "evaluation_mode": eval_mode,
                "evaluation_status": eval_status,
                "earth_model": earth_model
            },
            "interpretation": "Local epicenter determined via arrival-time inversion if HypoInverse/Hypo71 is indicated.",
            "data_attributes": {
                "resource_id": str(ev.resource_id),
                "origin_resource_id": str(origin.resource_id),
                "creation_time": str(getattr(creation, "creation_time", "Unknown")),
                "author": getattr(creation, "author", "Unknown")
            },
            "quality_metrics": quality_dict
        }

        with open(info_json_path, "w", encoding="utf-8") as f:
            json.dump(info_dict, f, indent=2, ensure_ascii=False)

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
