import os


class ConfigHelper:
    def __init__(self):
        #self.ocid_key = os.getenv("OCID_KEY")
        self.base_path = "/opt/feed/"
        self.iso_country = "CA"
        #self.twilio_sid = ConfigHelper.get_from_env("TWILIO_SID")
        #self.twilio_token = ConfigHelper.get_from_env("TWILIO_TOKEN")
        self.ocid_destination_file = "../../feed_canada/opencellid/cell_towers.csv.gz"  # NOQA
        self.fcc_destination_file = "../../feed_canada/ised_feed.csv.gz"
        self.target_radio = "GSM"
        return

    @classmethod
    def get_from_env(cls, k):
        retval = os.getenv(k)
        if retval is None:
            print "Required config variable not set: %s" % k
            print "Unable to continue.  Exiting."
            raise KeyError
        return retval
