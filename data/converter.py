

maxRange = 0x1<<(32-24)

maxIp = 16777216 + maxRange - 2

print maxIp

parts = "1.0.0.0/24".split("/")
ip = parts[0]

if len(parts) < 2:
    prefix = 0
else:
    prefix = int(parts[1])


mask = 0xffffffff << (32 - prefix)
print("Prefix=" + str(prefix))
print("Address=" + ip)


value = mask
hostsCount = 2**(32 - prefix) - 2;

bytes = [(value >> 24), (value >> 16 & 0xff), (value >> 8 & 0xff), (value & 0xff)]
print(hostsCount)