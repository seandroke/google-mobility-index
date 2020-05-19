import schedule
import time
import urllib.request

def job():
    urllib.request.urlretrieve("https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv?cachebust=911a386b6c9c230f", '/var/www/html/sean.droke/CIS4517-Project-2/mobility.csv')
    return

schedule.every().day.at("22:44").do(job)

while True:
    schedule.run_pending()
    time.sleep(60) # wait one minute


    