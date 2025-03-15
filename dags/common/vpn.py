from airflow.exceptions import AirflowException
from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN

"""
Make connection to VPN
Make call to IP check service to verify VPN is being used
return
"""

def initialize_and_test_vpn():
    initialize_VPN()
