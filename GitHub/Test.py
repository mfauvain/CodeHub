import json
import sys

payloadarg = list(sys.argv)[1]
jsondata = json.loads(payloadarg)
payloadparsed = jsondata['state']
print(payloadparsed)