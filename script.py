# -*- coding: utf-8 -*-
"""
@author: Max
"""

import requests
import os
import re

def web_site_online(timeout=5):
    "" 
    ""
    url = "https://duo.nl/open_onderwijsdata/publicaties/indicatoren/indic.jsp"
    try:
        req = requests.get(url, timeout=timeout)
        req.raise_for_status()
        return True
    except requests.HTTPError as e:
        print("Checking internet connection failed, status code {0}.".format(
        e.response.status_code))
    except requests.ConnectionError:
        print("No internet connection available.")
    return False

def files_downloader():
    "" 
    ""
    folder = "Mbo_indicatoren"
    if not os.path.exists(folder):
        os.mkdir(folder)
    
    url = "https://duo.nl/open_onderwijsdata/publicaties/indicatoren/indic.jsp"
    source = requests.get(url).text
    docs = re.findall(r'(https?:/)?(/?[\w_\-&%?./]*?)\.(xlsx|xls)',source, re.M)
    
    for doc in docs:
        remote = "https://duo.nl" + doc[1] + "." + doc[2];
        filename = folder + "/" + doc[1].split('/')[-1] + "." + doc[2]
        print("Copying from " + remote + " to " + filename)
        if not os.path.exists(filename):
            f = open(filename, 'wb')
            f.write(requests.get(remote).content)
            f.close()
    
if __name__ == "__main__":
    if web_site_online():
        files_downloader()