import urllib.request

for y in range(2013,2018):
    for m in range(1,13):
        for d in range(1,32):
            ystr = str(y)
            if m < 10:
                mstr = "0" + str(m)
            else:
                mstr = str(m)
            if d < 10:
                dstr = "0" + str(d)
            else:
                dstr = str(d)
            url = "http://cran-logs.rstudio.com/" \
                  + ystr + "/" + ystr + "-" + mstr + "-" + dstr + "-r.csv.gz"
            fn = "data/" + ystr + "-" + mstr + "-" + dstr + "-r.csv.gz"
            try:
                urllib.request.urlretrieve(url, fn)
            except:
                pass