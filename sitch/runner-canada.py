#!/usr/bin/python
import datetime
import gzip
import os
import sitchlib
import shutil
from dateutil.parser import parse as dt_parse
from LatLon.lat_lon import LatLon

import pdb

""" Outputs files like state.csv.gz.  These contain CSV data from the
FCC license database.  Use for determining GPS distance from tower.  Also
useful for correlating ARFCN/MCC/MNC for sanity and correct BTS ownership and
licensing representation."""

fcc_fields = ["LICENSE_ID", "SOURCE_SYSTEM", "CALLSIGN", "FACILITY_ID", "FRN",
              "LIC_NAME", "COMMON_NAME", "RADIO_SERVICE_CODE",
              "RADIO_SERVICE_DESC", "ROLLUP_CATEGORY_CODE",
              "ROLLUP_CATEGORY_DESC", "GRANT_DATE", "EXPIRED_DATE",
              "CANCELLATION_DATE", "LAST_ACTION_DATE", "LIC_STATUS_CODE",
              "LIC_STATUS_DESC", "ROLLUP_STATUS_CODE", "ROLLUP_STATUS_DESC",
              "ENTITY_TYPE_CODE", "ENTITY_TYPE_DESC", "ROLLUP_ENTITY_CODE",
              "ROLLUP_ENTITY_DESC", "LIC_ADDRESS", "LIC_CITY", "LIC_STATE",
              "LIC_ZIP_CODE", "CONTACT_COMPANY", "CONTACT_EMAIL",
              "MARKET_CODE", "MARKET_DESC", "CHANNEL_BLOCK", "LOC_TYPE_CODE",
              "LOC_TYPE_DESC", "LOC_CITY", "LOC_COUNTY_CODE",
              "LOC_COUNTY_NAME", "LOC_STATE", "LOC_RADIUS_OP", "LOC_SEQ_ID",
              "LOC_LAT_DEG", "LOC_LAT_MIN", "LOC_LAT_SEC", "LOC_LAT_DIR",
              "LOC_LONG_DEG", "LOC_LONG_MIN", "LOC_LONG_SEC", "LOC_LONG_DIR",
              "HGT_STRUCTURE", "ASR_NUM", "ANTENNA_ID", "ANT_SEQ_ID",
              "ANT_MAKE", "ANT_MODEL", "ANT_TYPE_CODE", "ANT_TYPE_DESC",
              "AZIMUTH", "BEAMWIDTH", "POLARIZATION_CODE", "FREQUENCY_ID",
              "FREQ_SEQ_ID", "FREQ_CLASS_STATION_CODE",
              "FREQ_CLASS_STATION_DESC", "POWER_ERP", "POWER_OUTPUT",
              "FREQUENCY_ASSIGNED", "FREQUENCY_UPPER_BAND", "UNIT_OF_MEASURE",
              "GROUND_ELEVATION", "ARFCN"]

ised_fields = ["NEW_ACCOUNT","NEW_LICNO","OLD_ACCOUNT","OLD_LICNO","LICENSEE","SERVICE","TRANSMIT_FREQ","RECEIVE_FREQ","TRANSMIT_LOWER","TRANSMIT_UPPER","TRANSMIT_BW","LOCATION","PROV","LATITUDE","LONGITUDE","SITE_ELEV","STUCT_HT","TX_MFR","TX_MODEL","TX_PWR","TX_PWR_TYPE","BW_EMISSION","TX_ANT_MFR","TX_ANT_MODEL","TX_ANT_HT","TX_ANT_AZIM","TX_ANT_ELEV_ANGLE","TX_ANT_GAIN","TX_ANT_DIRECTIONAL","TX_LINE_LOSS","ZONE_ENHANCER","LAST_MOD_DATE","LAST_UPLOAD_DATE"]

ocid_fields = ["radio", "mcc", "net", "area", "cell", "unit", "lon",
               "lat", "range", "samples", "changeable", "created",
               "updated", "averageSignal", "carrier"]

feed_directory = "/var/production/"  # This is where the finished feed goes


def compress_and_remove_original(infiles):
    for uncompressed in list(infiles):
        infile = uncompressed
        outfile = "%s.gz" % infile
        with open(infile, 'rb') as f_in, gzip.open(outfile, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        print "Written: %s" % outfile
        os.remove(infile)
        print "Removed: %s" % infile


def get_now_string():
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return now


def write_statusfile(file_path, fcc_date, ocid_date):
    datestring = get_now_string()
    str_1 = "# SITCH Sensor Feed"
    str_2 = "## Processed: %s" % datestring
    str_3 = "Derived from:"
    str_4 = "* The OpenCellID DB http://opencellid.org\n  * Newest record: %s\n  * CGI DB\n  * CC by SA 3.0 https://creativecommons.org/licenses/by-sa/3.0/" % epoch_to_iso8601(ocid_date)  # NOQA
    str_5 = "* The FCC License DB http://data.fcc.gov\n  * Newest record: %s\n  * ARFCN DB" % epoch_to_iso8601(fcc_date)  # NOQA
    str_6 = "* Twilio's API: https://twilio.com\n  * CGI to provider correlation"  # NOQA
    master_str = "\n".join([str_1, str_2, str_3, str_4, str_5, str_6])
    with open(file_path, 'w') as out_file:
        out_file.write(master_str)
    return


def epoch_to_iso8601(unix_time):
    """Transform epoch time to ISO8601 format."""
    cleaned = float(unix_time)
    return datetime.datetime.utcfromtimestamp(cleaned).isoformat()


def iso8601_to_epoch(iso_time):
    """Transform iso time into a unix timestamp."""
    return int((dt_parse(iso_time) -
                datetime.datetime(1970, 1, 1)).total_seconds())


def convert_dec_latlon_to_deg(dec_latitude, dec_longtitude):
    latlon_position = LatLon(dec_latitude, dec_longtitude) 
    latlon_strs = latlon_position.to_string('d% %m% %S% %H')
    lat_deg = latlon_strs[0].split()
    long_deg = latlon_strs[1].split()

    return [lat_deg, long_deg]

def populate_fcc_from_ised(fcc_row, ised_row):
    fcc_lat_log = convert_dec_latlon_to_deg(float(ised_row["LATITUDE"]), float(ised_row["LONGITUDE"]))
    fcc_row["COMMON_NAME"] = ised_row["LICENSEE"]
    fcc_row["LOC_STATE"]  = ised_row["PROV"]
    
    fcc_row["LOC_LAT_DEG"] = fcc_lat_log[0][0]
    fcc_row["LOC_LAT_MIN"] = fcc_lat_log[0][1]
    fcc_row["LOC_LAT_SEC"] = fcc_lat_log[0][2]
    fcc_row["LOC_LAT_DIR"] = fcc_lat_log[0][3]
    fcc_row["LOC_LONG_DEG"] = fcc_lat_log[1][0]
    fcc_row["LOC_LONG_MIN"] = fcc_lat_log[1][1]
    fcc_row["LOC_LONG_SEC"] = fcc_lat_log[1][2]
    fcc_row["LOC_LONG_DIR"] = fcc_lat_log[1][3]

    fcc_row["LAST_ACTION_DATE"] = datetime.datetime.today().strftime('%m/%d/%Y %H:%M:%S')#06/08/2017 15:27:17
                                                            
def main():
    sitchlib.OutfileHandler.ensure_path_exists(feed_directory)
    arfcn_comparator = sitchlib.ArfcnComparator()
    config = sitchlib.ConfigHelper()
    fileout = sitchlib.OutfileHandler(config.base_path,
                                      fcc_fields, ocid_fields)
    #twilio_c = sitchlib.TwilioCarriers(config.twilio_sid,
    #                                   config.twilio_token)
    #mcc_mnc_carriers = twilio_c.get_providers_for_country(config.iso_country)
    #carrier_enricher = sitchlib.CarrierEnricher(mcc_mnc_carriers)
    fcc_feed_obj = sitchlib.FccCsv(config.fcc_destination_file)
    
    newest_ocid_record = 0
    newest_fcc_record = 0
    print "Splitting FCC license file into feed files..."
    for row in fcc_feed_obj:
        f_min = row["TRANSMIT_FREQ"]
        try:
            f_max = str(float(f_min) + float(row["TRANSMIT_BW"] and row["TRANSMIT_BW"] or "0" ))
        except ValueError:
            pdb.set_trace()
            print row["TRANSMIT_FREQ"], row["TRANSMIT_BW"]
        arfcns = arfcn_comparator.arfcn_from_downlink_range(f_min, f_max)
        net_row = {}
        for column in fcc_fields:
            try:
                net_row[column] = ""#row[column]
            except KeyError:
                pass
        populate_fcc_from_ised(net_row, row)
        for arfcn in arfcns:
            net_row["ARFCN"] = arfcn
            fileout.write_fcc_record(net_row)
        # if iso8601_to_epoch(row["LAST_ACTION_DATE"]) > newest_fcc_record:
        #     newest_fcc_record = iso8601_to_epoch(row["LAST_ACTION_DATE"])
    #pdb.set_trace()
    print "Compressing FCC feed files"
    compress_and_remove_original(fileout.feed_files)
    fileout = None
    fileout = sitchlib.OutfileHandler(config.base_path,
                                      fcc_fields, ocid_fields)
    # ocid feeds are not needed
    # ocid_feed_obj = sitchlib.OcidCsv(config.ocid_destination_file)
    # print "Splitting OpenCellID feed into MCC files..."
    # for row in ocid_feed_obj:
    #     if row["radio"] != config.target_radio:
    #         continue
    #     row["carrier"] = carrier_enricher.get_carrier(row["mcc"], row["net"])
    #     if int(row["updated"]) > int(newest_ocid_record):
    #         newest_ocid_record = int(row["updated"])
    #     fileout.write_ocid_record(row)
    # print "Compressing OpenCellID feed files"
    # compress_and_remove_original(fileout.feed_files)
    print "Moving to drop directory..."
    staged_files = os.listdir(config.base_path)
    for staged_file in staged_files:
        full_src_file_name = os.path.join(config.base_path, staged_file)
        full_dst_file_name = os.path.join(feed_directory, staged_file)
        if (os.path.isfile(full_src_file_name)):
            shutil.copy(full_src_file_name, full_dst_file_name)
    write_statusfile("/opt/feed/README.md", newest_fcc_record, newest_ocid_record)
    print "ALL DONE!!!"

if __name__ == "__main__":
    main()
