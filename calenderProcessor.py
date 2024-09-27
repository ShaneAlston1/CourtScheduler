import pyodbc
import re
import urllib3
import hashlib
from bs4 import BeautifulSoup
from urllib.parse import urlencode
from datetime import datetime, timedelta
from tqdm import tqdm
from pyodbc import DatabaseError, IntegrityError, ProgrammingError, InternalError


def date_iterator(start_date, days):
    """Generate dates in MM%2FDD%2FYY format for the next specified number of days."""
    current_date = start_date
    for _ in range(days):
        formatted_date = current_date.strftime('%m%%2F%d%%2F%y')
        yield formatted_date
        current_date += timedelta(days=1)


def run_curl_command(hearing_day, page_num):
    http = urllib3.PoolManager()

    # Required headers
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': 'ASP.NET_SessionId=owoepj2nzb3rbxyn2nlhmkxy',
        'Origin': 'https://civilinquiry.jud.ct.gov',
        'Pragma': 'no-cache',
        'Referer': 'https://civilinquiry.jud.ct.gov/CourtEventsSearchByDate.aspx?ctl00%24ContentPlaceHolder1%24txtDate=09%2f09%2f24&ctl00%24ContentPlaceHolder1%24ckSmallClaimsOnly=&ctl00%24ContentPlaceHolder1%24btnSubmit=Search&ctl00_ContentPlaceHolder1_ddlCaseCategory=FA&ctl00_ContentPlaceHolder1_ddlLocation=ALL&ctl00_ContentPlaceHolder1_ddlSortOrder=time',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    # Make the initial GET request to retrieve the form data
    url = 'https://civilinquiry.jud.ct.gov/CourtEventsSearchByDate.aspx'
    page_num = f"Page${page_num}"
    # Encode the form data
    # encoded_data = urlencode(data)

    # Make the POST request with the form data
    response = http.request('POST', url, headers=headers,
                            body=f"""ctl00_MyScriptManager_HiddenField=%3B%3BAjaxControlToolkit%2C+Version%3D3.5.7.607%2C+Culture%3Dneutral%2C+PublicKeyToken%3D28f01b0e84b6d53e%3Aen-US%3Ae96eec5b-f5fc-47c8-9cba-2a4f2f4c43f0%3Aeffe2a26%3Ae00da949&__EVENTTARGET=&__EVENTARGUMENT={page_num}&__LASTFOCUS=&__VIEWSTATE=%2FwEPDwUKMTU0MjUxMjU3OQ9kFgJmD2QWAgIDD2QWCAIBDxQrAAJkEBYBZhYBFgIeC05hdmlnYXRlVXJsBRZodHRwczovL3d3dy5qdWQuY3QuZ292FgFmZAIFDw8WAh4HVmlzaWJsZWhkZAIHD2QWAgIBD2QWBAIBDzwrAA0BDDwrAB0FAxQrAAIWAh4EVGV4dAXCAUF0dG9ybmV5L0Zpcm0gSnVyaXMgTnVtYmVyIExvb2stdXAmbmJzcDs8aW1nIGJvcmRlcj0nMCcgd2lkdGg9JzEwJyBoZWlnaHQ9JzEwJyBhbGlnbj1jZW50ZXIgYWx0PSdMaW5rIHdpbGwgb3BlbiBpbiBuZXcgd2luZG93JyBzcmM9J2h0dHBzOi8vY2l2aWxpbnF1aXJ5Lmp1ZC5jdC5nb3YvaW1hZ2VzL25ld3dpbmRvdy1hcnJvdy5wbmcnIC8%2BZBIUKwACFgIeCFNlbGVjdGVkZ2QWFCsAAhYEHwIFE0xlZ2FsIE5vdGljZXMmbmJzcDsfAAU0aHR0cHM6Ly9jaXZpbGlucXVpcnkuanVkLmN0Lmdvdi9MZWdhbE5vdGljZUxpc3QuYXNweGQYFCsAAhYEHwIFrwFQZW5kaW5nIEZvcmVjbG9zdXJlIFNhbGVzJm5ic3A7PGltZyBib3JkZXI9MCB3aWR0aD0xMCBoZWlnaHQ9MTAgYWxpZ249Y2VudGVyIGFsdD0iTGluayB3aWxsIG9wZW4gaW4gbmV3IHdpbmRvdyIgc3JjPWh0dHBzOi8vY2l2aWxpbnF1aXJ5Lmp1ZC5jdC5nb3YvaW1hZ2VzL25ld3dpbmRvdy1hcnJvdy5wbmc%2BHwAFWmh0dHBzOi8vc3NvLmVzZXJ2aWNlcy5qdWQuY3QuZ292L2ZvcmVjbG9zdXJlcy9QdWJsaWMvUGVuZFBvc3RieVRvd25MaXN0LmFzcHg%2FQ2FsbGVyPVB1YmxpY2QcFCsAAhYCHwAFJGh0dHBzOi8vd3d3Lmp1ZC5jdC5nb3YvY29udGFjdHVzLmh0bWRkAgMPZBYEAgEPDxYEHwAFIWh0dHBzOi8vd3d3Lmp1ZC5jdC5nb3YvSG93RG9JLmh0bR4HVG9vbFRpcAUkTXkgSVA6MzIuMjE3LjY5LjYxKGJveDpIQU1TSEkxVi0uLi4pZGQCAw8PFgIfAAUkaHR0cHM6Ly93d3cuanVkLmN0Lmdvdi9jb250YWN0dXMuaHRtZGQCCQ9kFgICAQ9kFghmD2QWAmYPZBYGAgEPDxYCHwJlZGQCAw8PFgIfAmVkZAIFD2QWAgIBDw8WAh8CBRRDb3VydCBFdmVudHMgQnkgRGF0ZWRkAgEPZBYCZg9kFgICAw8PFgQeIU5vQm90X1Jlc3BvbnNlVGltZUtleV9jdGwwMCROb0JvdAYnPbN%2FGbfcSB4fTm9Cb3RfU2Vzc2lvbktleUtleV9jdGwwMCROb0JvdAUvTm9Cb3RfU2Vzc2lvbktleV9jdGwwMCROb0JvdF82Mzg1ODY1NjczMjQyODYyNDdkFgICAQ8WAh4PQ2hhbGxlbmdlU2NyaXB0BQR%2BNDI4ZAICD2QWAmYPZBYCAgEPZBYCAgEPZBYEAgkPEGQQFSYDQWxsIS0tIEpVRElDSUFMIERJU1RSSUNUIExPQ0FUSU9OUyAtLQ5BQU4tTWlsZm9yZCBKRA5EQkQtRGFuYnVyeSBKRBFGQlQtQnJpZGdlcG9ydCBKRA9GU1QtU3RhbWZvcmQgSkQSSEhCLU5ldyBCcml0YWluIEpED0hIRC1IYXJ0Zm9yZCBKRBFLTkwtTmV3IExvbmRvbiBKRA5LTk8tTm9yd2ljaCBKRBFMTEktTGl0Y2hmaWVsZCBKRBFNTVgtTWlkZGxldG93biBKRBBOTkgtTmV3IEhhdmVuIEpEDk5OSS1NZXJpZGVuIEpEElRTUi1Sb2NrdmlsbGUtR0ExORBUVEQtUm9ja3ZpbGxlIEpEEFVXWS1XYXRlcmJ1cnkgSkQNV1dNLVB1dG5hbSBKRBYtLSBIT1VTSU5HIFNFU1NJT05TIC0tHkJQSC1CcmlkZ2Vwb3J0IEhvdXNpbmcgU2Vzc2lvbhxIRkgtSGFydGZvcmQgSG91c2luZyBTZXNzaW9uH05CSC1OZXcgQnJpdGFpbiBIb3VzaW5nIFNlc3Npb24dTkhILU5ldyBIYXZlbiBIb3VzaW5nIFNlc3Npb24cTldILVN0YW1mb3JkIEhvdXNpbmcgU2Vzc2lvbh1XVEgtV2F0ZXJidXJ5IEhvdXNpbmcgU2Vzc2lvbgstLSBVSUZTQSAtLRM2NDAtQnJpZGdlcG9ydCAtIFVGETY0MS1IYXJ0Zm9yZCAtIFVGFDY0Mi1OZXcgQnJpdGFpbiAtIFVGEzY0My1NaWRkbGV0b3duIC0gVUYSNjQ0LU5ldyBIYXZlbiAtIFVGEDY0NS1Ob3J3aWNoIC0gVUYPNjQ2LVB1dG5hbSAtIFVGEjY0Ny1Sb2NrdmlsbGUgLSBVRhE2NDgtU3RhbWZvcmQgLSBVRhI2NTAtV2F0ZXJidXJ5IC0gVUYTNjUyLVRvcnJpbmd0b24gLSBVRhA2NTMtRGFuYnVyeSAtIFVGFSYDQUxMAi0xA0FBTgNEQkQDRkJUA0ZTVANISEIDSEhEA0tOTANLTk8DTExJA01NWANOTkgDTk5JA1RTUgNUVEQDVVdZA1dXTQItMgNCUEgDSEZIA05CSANOSEgDTldIA1dUSAItMwM2NDADNjQxAzY0MgM2NDMDNjQ0AzY0NQM2NDYDNjQ3AzY0OAM2NTADNjUyAzY1MxQrAyZnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2RkAg0PEGRkFgECAWQCAw9kFgJmD2QWAgIDD2QWBgIBDw8WAh8CBbcGPGEgaHJlZj0naHR0cHM6Ly93d3cuanVkLmN0Lmdvdi9hdHRvcm5leXMuaHRtJz5BdHRvcm5leXM8L2E%2BIHwgPGEgaHJlZj0naHR0cHM6Ly93d3cuanVkLmN0Lmdvdi9qdWQyLmh0bSc%2BQ2FzZSBMb29rLXVwPC9hPiB8IDxhIGhyZWY9J2h0dHBzOi8vd3d3Lmp1ZC5jdC5nb3YvY291cnRzLmh0bSc%2BQ291cnRzPC9hPiB8IDxhIGhyZWY9J2h0dHBzOi8vd3d3Lmp1ZC5jdC5nb3YvZGlyZWN0b3JpZXMuaHRtJz5EaXJlY3RvcmllczwvYT4gfCA8YSBocmVmPSdodHRwczovL3d3dy5qdWQuY3QuZ292L0VkUmVzb3VyY2VzLmh0bSc%2BRWR1Y2F0aW9uYWxSZXNvdXJjZXM8L2E%2BIHwgPGEgaHJlZj0naHR0cHM6Ly9lc2VydmljZXMuanVkLmN0Lmdvdic%2BRS1TZXJ2aWNlczwvYT4gfCA8YSBocmVmPSdodHRwczovL3d3dy5qdWQuY3QuZ292L2ZhcS9EZWZhdWx0Lmh0bSc%2BRkFRJ3M8L2E%2BIHwgPGEgaHJlZj0naHR0cHM6Ly93d3cuanVkLmN0Lmdvdi9qdXJ5Lyc%2BSnVyb3IgSW5mb3JtYXRpb248L2E%2BIHwgPGEgaHJlZj0naHR0cHM6Ly93d3cuanVkLmN0Lmdvdi9leHRlcm5hbC9uZXdzL2RlZmF1bHQuaHRtJz5OZXdzICZhbXA7IFVwZGF0ZXM8L2E%2BIHwgPGEgaHJlZj0naHR0cHM6Ly93d3cuanVkLmN0Lmdvdi9vcGluaW9ucy5odG0nPk9waW5pb25zPC9hPiB8IDxhIGhyZWY9J2h0dHBzOi8vd3d3Lmp1ZC5jdC5nb3Yvb3Bwb3J0dW5pdGllcy5odG0nPk9wcG9ydHVuaXRpZXM8L2E%2BIHwgPGEgaHJlZj0naHR0cHM6Ly93d3cuanVkLmN0Lmdvdi9zZWxmaGVscC5odG0nPlNlbGYtSGVscDwvYT4gfCA8YSBocmVmPSdodHRwczovL3d3dy5qdWQuY3QuZ292Jz5Ib21lPC9hPmRkAgMPDxYCHwIFhwI8YSBocmVmPSdodHRwczovL3d3dy5qdWQuY3QuZ292L2xlZ2FsdGVybXMuaHRtJz5Db21tb24gTGVnYWwgVGVybXM8L2E%2BIHwgPGEgaHJlZj0naHR0cHM6Ly93d3cuanVkLmN0Lmdvdi9jb250YWN0dXMuaHRtJz5Db250YWN0IFVzPC9hPiB8IDxhIGhyZWY9J2h0dHBzOi8vd3d3Lmp1ZC5jdC5nb3Yvc2l0ZW1hcC5odG0nPlNpdGUgTWFwPC9hPiB8IDxhIGhyZWY9J2h0dHBzOi8vd3d3Lmp1ZC5jdC5nb3YvcG9saWNpZXMuaHRtJz5XZWJzaXRlIFBvbGljaWVzPC9hPmRkAgUPDxYCHwIFbkNvcHlyaWdodCDCqSAyMDI0LCBTdGF0ZSBvZiBDb25uZWN0aWN1dCBKdWRpY2lhbCBCcmFuY2ggPGJyPjxicj5QYWdlIENyZWF0ZWQgb24gOC83LzIwMjQgYXQgMzoyODoyNyBQTTxicj48YnI%2BZGQYAgUeX19Db250cm9sc1JlcXVpcmVQb3N0QmFja0tleV9fFgEFK2N0bDAwJENvbnRlbnRQbGFjZUhvbGRlcjEkY2tTbWFsbENsYWltc09ubHkFEWN0bDAwJG1udVN0YW5kYXJkDw9kBRFDb3VydEV2ZW50c0J5RGF0ZWTp9EdkzqQF6WPDqMd21FzuyVglVA%3D%3D&__VIEWSTATEGENERATOR=2B0FF020&__EVENTVALIDATION=%2FwEdADJNeNWZ74xlJNxxlzVsYeK5cUPSIFJArai%2BPsrufnX6rf94bjsHc%2B8EOXckZKMOFGmszukQlB2tduKy1kLuQhnMRfR5fqUbL4s17IRiim%2Fj%2BgXiKDHVmxzdbfyBGjZFSjRu1pmaQMmdWGwTaTg0jbVyYQvctQ4Hh%2B3P7c%2BKqoAwNBTd7EblFjj0PGQyzkfTWYP6Az51tLnPlxV%2BpfhQrlJAQNHwtpodleutolJgsfBOfDQn7HkRlt5n9l2V4sh%2B5Bfjtd3%2F4pAdACEhh9yrD9t5EtDWZTv58ucL7oWHHroP55hctXM8r2UWepMgtNc7ETh6sqHiLqvlD2qps3QIcNJdg1y2QgBU7YeQclh3ee%2FBGiN55V1aZ8cu44eI5q9Jh%2BO7FjIcGg1HNZla70nzy6HbCvnGdyKmi4M%2FCXoT8M6Hy0LcQl5Akvk7vuy7HZQMSJKYQ6WAJuIjbC58jSDd2YEP2Abkr%2BALkuMRQLlbao8TDeIyG0tBe%2FUlAxhKKGpGOp%2F%2BdMq1bo6%2F%2BjldhP%2FEqdn5TAWa5xuJnByM9yXOc6cheGumCpoqnurYnfmj%2FNh1%2FnpDL2kRa13Eb%2BHsHp4Mmea%2B%2FDFuBhcIINEsz0eukGBjGEp7d6Ifl59%2Fjm3L3IgG5QjgT6SbtUTpwBjxMK%2BRLLU2qYzAHSshOlYfuxWAWQlYPwxgaXKVHNWOT790VUgg9HSbgzFzCBGn1iV3oSdtOII9SIcGUzxyTXoMioAGf4wjJxpUO%2FjEQZtDkiNJZIS35cHT9KVYOPBucDGtwEMRyzERqwfZ0%2FErJyod7a9E5sl0oCUbMaX7ky6FWTFWiJ6RGh8cB36K3qpXH3Vj6ljMBXHfjMm44qUT5pn8vrlaFlAVrQH42bdEAr%2BJ5zX4vqJdsEqOseAU%2B8f17B85hZcz9EWTie0s6Jvv49vQeSvEarbq%2BIj1jB0uaH%2B%2BB46%2Fcwek4ojJMTpQ242avdoccp1tn9Y6EM1fvscPVxeAcXEVlqB669db6kMeyaSVoW9QP3sm%2FA7vhwMmrc6NN229vqV6RO4mzR%2B4SC0fZYF0H1gj%2BXgVi7eIt%2FCU%2FAZnOgOO4OOHfl%2Bh2Ig%2F&ctl00%24NoBot%24NoBot_NoBotExtender_ClientState=-429&ctl00%24ContentPlaceHolder1%24txtDate={hearing_day}&ctl00%24ContentPlaceHolder1%24ddlLocation=ALL&ctl00%24ContentPlaceHolder1%24ddlCaseCategory=FA&ctl00%24ContentPlaceHolder1%24ddlSortOrder=time&ctl00%24ContentPlaceHolder1%24btnSubmit=Search""")
    if response.status == 200:
        print("POST request successful")
        return response
    else:
        print(f"POST request failed with status code: {response.status}")


def get_hearing_id(docket_datetime, docket_id, docket_court):
    """
    Combines three variables into a single string and returns the SHA-256 hash.

    Parameters:
    var1 (str): The first variable.
    var2 (str): The second variable.
    var3 (str): The third variable.

    Returns:
    str: The SHA-256 hash of the combined variables.
    """
    # Convert all variables to string and combine them
    combined_string = f"{str(docket_datetime)}{str(docket_id)}{str(docket_court)}"

    # Create a sha256 hash object
    sha256_hash = hashlib.sha256()

    # Update the hash object with the combined string
    sha256_hash.update(combined_string.encode('utf-8'))

    # Get the hexadecimal digest of the hash
    return sha256_hash.hexdigest()

def convert_to_bit(value):
    """ Convert a string representation of a bit to an actual bit value (1 or 0). """
    return 1 if value.lower() in ['Proceeding', '1', 'yes', 'Remote'] else 0


def upsert_hearing(hearing_id, hearing_datetime, hearing_date, hearing_time, docket_id, hearing_type, hearing_court, hearing_status, previous_hearing_status_1='0', previous_hearing_status_2='0', previous_hearing_status_3='0', is_remote='O'):
    # Convert string representations of bit values to actual bit values
    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.0.70;DATABASE=eservices;UID=sa;PWD=SKInut19**'
    # Connect to the SQL Server database
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    try:
        
        previous_hearing_status_1 = convert_to_bit(previous_hearing_status_1)
        previous_hearing_status_2 = convert_to_bit(previous_hearing_status_2)
        previous_hearing_status_3 = convert_to_bit(previous_hearing_status_3)
        is_remote = convert_to_bit(is_remote)



        # Execute the stored procedure
        cursor.execute("""
            EXEC [dbo].[UpsertHearing]  
            @hearing_id=?,
            @hearing_datetime=?, 
            @hearing_date=?, 
            @hearing_time=?, 
            @docket_id=?, 
            @hearing_type=?, 
            @hearing_court=?, 
            @hearing_status=?, 
            @previous_hearing_status_1=?, 
            @previous_hearing_status_2=?, 
            @previous_hearing_status_3=?, 
            @is_remote=?
        """, (str(hearing_id), hearing_datetime, hearing_date, hearing_time, docket_id, hearing_type, hearing_court, hearing_status, previous_hearing_status_1, previous_hearing_status_2, previous_hearing_status_3, is_remote))

        # Commit the transaction
        conn.commit()

        # Close the connection
        cursor.close()
       
    except DatabaseError as e:
        print(f"Database error: {e}")
        raise e
    except IntegrityError as e:
        print(f"Integrity error: {e}")
        raise e
    except ProgrammingError as e:
        print(f"Programming error: {e}")
        raise e
    except InternalError as e:
        print(f"Internal error: {e}")
        raise e
    except Exception as e:
        print(f"Error: {e}")
        raise e
 
            


def convert_hearing_datetime(hearing_date, hearing_time):
    """
    Convert the hearing date and time to a datetime object.

    Parameters:
    hearing_date (str): The hearing date in MM/DD/YY format or YYYY-MM-DD format.
    hearing_time (str): The hearing time in hh:mm AM/PM format or HH:MM:SS format.

    Returns:
    datetime: A datetime object representing the combined date and time.
    """
    formats = [
        ('%m/%d/%y %I:%M %p', '%m/%d/%y', '%I:%M %p'),  # MM/DD/YY hh:mm AM/PM
        ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%H:%M:%S')   # YYYY-MM-DD HH:MM:SS
    ]

    for fmt, date_fmt, time_fmt in formats:
        try:
            hearing_datetime = datetime.strptime(
                f"{str(hearing_date).split(' ')[0]} {str(hearing_time).split(' ')[1]}", fmt)
            return hearing_datetime
        except ValueError:
            pass

    # If all formats fail, raise an error with more details
    raise ValueError(
        f"Time data '{hearing_date} {hearing_time}' does not match any expected format")



def get_hearing_date(hearing_date):
    # Convert the hearing date to a datetime object
    hearing_date = datetime.strptime(hearing_date.replace('\n','').strip(), '%m/%d/%Y')
    return hearing_date


def get_hearing_time(hearing_time):
    # Convert the hearing time to a datetime object
    hearing_time = datetime.strptime(hearing_time.replace('\n','').strip(), '%I:%M %p')
    return hearing_time


def clean_hearing_docket_id(hearing_docket_id):
    hearing_docket_id = hearing_docket_id.replace('\n', '').strip()
    # Clean the hearing docket ID
    cleaned_docket_id = hearing_docket_id.strip().replace('-FA-', '-FA').replace('\n', '')
    return cleaned_docket_id


def get_hearing_court_id(docket_id):
    # Get the hearing court ID from the docket ID
    hearing_court_id = docket_id.replace('\xa0', '').replace('\n', '').split('-')[0].strip()
   
    return hearing_court_id


def get_hearing_status(hearing_status):
    status = hearing_status.strip().replace('\n', '')
   
    pattern = re.compile(r'Proceeding')
    if pattern.search(status):
        return 1
    else:
        return 0
    
def main():
    connection_string = (
        'DRIVER={{ODBC Driver 17 for SQL Server}};'
        'SERVER=TF-SQL-1;'
        'DATABASE=eservices;'
        'UID=sa;'
        'PWD=SKInut19**'
    )
    # Start date
    start_date = datetime.now()
    # Number of days to iterate over
    days = 180
    # Create the iterator
    date_iter = date_iterator(start_date, days)
    hearingsTotal = 0

    # Initialize the progress bar
    with tqdm(total=days * 4) as pbar:
        # Print the dates
        for date in date_iter:
            for page_num in range(1, 5):
                response = run_curl_command(date, page_num)
                soup = BeautifulSoup(response.data, 'lxml')
                table = soup.find(
                    id='ctl00_ContentPlaceHolder1_gvCourtEventsResults')
                if table is not None:
                    for row in table.find_all('tr'):
                        if len(row) == 8:
                            hearingData = [
                                cell.text for cell in row.find_all('td')]
                            
                            if len(hearingData) == 6:
                                hearing_date = get_hearing_date(
                                    hearingData[0].strip())
                                hearing_time = get_hearing_time(
                                    hearingData[1].strip())
                                hearing_datetime = convert_hearing_datetime(
                                    hearing_date, hearing_time)
                                hearing_type = hearingData[2].strip()
                                hearing_docket_id = clean_hearing_docket_id(
                                    hearingData[3])
                                
                                hearing_status = get_hearing_status(hearingData[5])
                                hearing_court = get_hearing_court_id(hearing_docket_id)
                                hearing_id = get_hearing_id(
                                    hearing_datetime, hearing_docket_id, hearing_court)
                                try:
                                    upsert_hearing(hearing_id, hearing_datetime, hearing_date, hearing_time, hearing_docket_id, hearing_type, hearing_court, hearing_status)
                                except Exception as e:
                                    print(f"Error: {e}")
                                    continue
                            #
                # Update the progress bar
                pbar.update(1)
                print("\033[F", end='')
                # print('',end='\r')



if __name__ == "__main__":
    main()
