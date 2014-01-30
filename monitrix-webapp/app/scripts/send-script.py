
def URLRequest(url, params, method="GET"):
    if method == "POST":
        return urllib2.Request(url, data=urllib.urlencode(params))
    else:
        return urllib2.Request(url + "?" + urllib.urlencode(params))

TODO

- Re-implement the send script
- Allow H3 endpoint, user and pw to be specd at CLI (or in a local file and/or ENV var?)
- Allow the job and the script and any parameters to be defined at the CLI.

