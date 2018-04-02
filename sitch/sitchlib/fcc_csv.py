import csv
import gzip


class FccCsv(object):
    """
    This object wraps the FCC license CSV dataset. The license
    dataset includes _all_ FCC licensees, including things like TV
    and broadcast radio. So we filter down to a few types specific
    to cellular communications.
    """
    def __init__(self, csv_gzfile):
        self.csv_gzfile = csv_gzfile
        self.fields = self.get_fields()
        self.radio_service_code = frozenset({
            # # legacy cellular service bands https://www.fcc.gov/wireless/bureau-divisions/mobility-division/cellular-service
            # #   - 850MHz "CLR"
            # #   - at&t, vzw
            # "CL",
            # # PCS https://www.fcc.gov/wireless/bureau-divisions/mobility-division/broadband-personal-communications-service-pcs
            # #   - 1900MHz
            # #   - at&t, sprint, t-mobile, vzw
            # "CW",
            # "CY",
            # # AWS https://www.fcc.gov/wireless/bureau-divisions/mobility-division/advanced-wireless-services-aws
            # #   - 1700/2100MHz
            # #   - at&t, t-mobile, vzw
            # "AW",
            # "AD",
            # # auctioned SMR https://www.fcc.gov/wireless/bureau-divisions/broadband-division/specialized-mobile-radio-service-smr
            # #   - 800MHz
            # #   - sprint "ESMR"
            # #   - LTE band 27
            # "YC",
            # "YD",
            # "YH",
            # ### per the data dictionary, a few other things http://data.fcc.gov/download/license-view/fcc-license-view-data-dictionary.doc
            # # BRS 2500MHz sprint (LTE B41)
            # # https://www.fcc.gov/tags/broadband-radio-service-brs
            # "BR",
            # # 700MHz Lower Bands; at&t, t-mobile, vzw
            # "WU",
            # "WX",
            # "WY",
            # "WZ",
            # # WCS 2300MHz; at&t; LTE band 30
            # "WS"
            # # ? CBRS 3600MHz (3550-3700MHz) https://www.fcc.gov/rulemaking/12-354
            # "NN",
            "AWS", # Advanced Wireless Service 1700 / 2100 MHz
            "BRS", # Broadband Radio Service 2500 - 2690 MHz
            "BWA24", # Broadband Wireless Access 24 GHz
            "BWA38", # Broadband Wireless Access 38 GHz
            "CELL", # Cellular 850 MHz
            "FCFS34", #First come, first served grid-cell based spectrum licences 3.4 GHz
            "FCFS38", # First come, first served grid-cell based spectrum licences in the 38 GHz band 38 GHz
            "FWA", # Fixed Wireless Access 3450 - 3650 MHz
            "MBS", #Mobile Broadband Service 700 MHz
            "NMCS", #Narrowband Multipoint Communication Service (Automated Meter Reading) 1.4 GHz
            "PCS", #Personal Communication Service 1.8/1.9 GHz
            "WBS", #Wireless Broadband Service 3650 - 3700 MHz
            "WCS", #Wireless Communication Service 2300 MHz

        })
        #we are going to use all possible service for canada for now

    def __iter__(self):
        with gzip.open(self.csv_gzfile) as gz_feed:
            feed = csv.DictReader(gz_feed, fieldnames=self.fields)
            for row in feed:
                if row["SERVICE"] in self.radio_service_code: 
                    yield row

    def get_fields(self):
        with gzip.open(self.csv_gzfile) as gz_feed:
            topline = gz_feed.readline()
            fields = self.get_fields_from_topline(topline)
            return fields

    def get_fields_from_topline(self, topline):
        cleaned_up = topline.replace('#', '', 1).replace('\n', '').replace('"', '')
        fields = cleaned_up.split(',')
        return fields
