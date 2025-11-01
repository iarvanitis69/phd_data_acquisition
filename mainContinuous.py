#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Κατεβάζει σεισμούς από EMSC και waveforms + stationXML από EIDA,
τοποθετώντας τα station metadata **ανά ημερομηνία και σταθμό**.

Δομή εξόδου:
./2010/HL.APE/2010-01-01/mseed/*.mseed
./2010/HL.APE/2010-01-01/Stations/HL.APE.xml
"""

import os
import shutil
from obspy import UTCDateTime
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.mass_downloader import MassDownloader, CircularDomain, Restrictions

BASE_DIR = "/media/iarv/Samsung/Continuous_3"
NETWORK = "HL"
STATIONS = ["APE"]
YEAR = 2010

def download_day_waveforms(year, network, station):
    client = Client("https://eida.gein.noa.gr")
    start = UTCDateTime(f"{year}-01-01")
    end = UTCDateTime(f"{year}-12-31")

    for day in range((end - start).days):
        day_start = start + 86400 * day
        day_end = day_start + 86400

        date_str = day_start.strftime("%Y-%m-%d")
        folder = os.path.join(BASE_DIR, str(year), f"{network}.{station}", date_str)
        mseed_path = os.path.join(folder, "mseed")
        stationxml_path = os.path.join(folder, "Stations")

        if os.path.exists(mseed_path) and len(os.listdir(mseed_path)) >= 3:
            print(f"✅ Ήδη υπάρχει: {folder}")
            continue

        print(f"⬇️ {network}.{station} – {date_str}")
        os.makedirs(mseed_path, exist_ok=True)
        os.makedirs(stationxml_path, exist_ok=True)

        domain = CircularDomain(latitude=36.6, longitude=25.7, minradius=0.0, maxradius=0.5)
        restrictions = Restrictions(
            starttime=day_start,
            endtime=day_end,
            network=network,
            station=station,
            location="*",
            channel="HH*",
            reject_channels_with_gaps=True,
            sanitize=True,
        )

        temp_dir = os.path.join(BASE_DIR, "_temp")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        os.makedirs(temp_dir)

        try:
            downloader = MassDownloader(providers=["https://eida.gein.noa.gr"])
            downloader.download(
                domain,
                restrictions,
                mseed_storage=os.path.join(temp_dir, "mseed"),
                stationxml_storage=os.path.join(temp_dir, "Stations"),
                threads_per_client=3
            )
        except Exception as e:
            print(f"❌ Σφάλμα: {e}")
            shutil.rmtree(temp_dir)
            continue

        # Μετακίνηση mseed
        for f in os.listdir(os.path.join(temp_dir, "mseed")):
            shutil.move(os.path.join(temp_dir, "mseed", f), os.path.join(mseed_path, f))

        # Μετακίνηση stationXML για τον σταθμό
        for f in os.listdir(os.path.join(temp_dir, "Stations")):
            if f.startswith(f"{network}.{station}") and f.endswith(".xml"):
                shutil.move(os.path.join(temp_dir, "Stations", f), os.path.join(stationxml_path, f))

        shutil.rmtree(temp_dir)

def main():
    for station in STATIONS:
        download_day_waveforms(YEAR, NETWORK, station)

if __name__ == "__main__":
    main()
