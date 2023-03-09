import json
lock = (json.load(open('lockfile.json')))['lock']
print(lock)