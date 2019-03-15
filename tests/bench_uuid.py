import uuid
from uuid import UUID as std_UUID
from edgedb.protocol.protocol import UUID as c_UUID

import time


ubytes = uuid.uuid4().bytes
ustr = str(std_UUID(bytes=ubytes))

print(str(std_UUID(bytes=ubytes)))
print(str(c_UUID(ubytes)))

N = 1_000_000

assert isinstance(c_UUID(ubytes), std_UUID)
assert c_UUID(ubytes).bytes == std_UUID(bytes=ubytes).bytes, \
    f'{ubytes}: {c_UUID(ubytes).bytes}'
assert c_UUID(ubytes).int == std_UUID(bytes=ubytes).int
assert c_UUID(str(std_UUID(bytes=ubytes))).int == std_UUID(bytes=ubytes).int
assert str(c_UUID(ubytes)) == str(std_UUID(bytes=ubytes))


print()

print(repr(std_UUID(bytes=ubytes)))
print(repr(c_UUID(ubytes)))


print()

st = time.monotonic()
for _ in range(N):
    std_UUID(bytes=ubytes)
std_total = time.monotonic() - st
print(f'std_UUID(bytes):\t  {std_total:.4f}')

st = time.monotonic()
for _ in range(N):
    c_UUID(ubytes)
c_total = time.monotonic() - st
print(f'c_UUID(bytes):\t\t* {c_total:.4f} ({std_total / c_total:.2f}x)')

st = time.monotonic()
for _ in range(N):
    object()
print(f'object():\t\t  {time.monotonic() - st:.4f}')


print()

st = time.monotonic()
for _ in range(N):
    std_UUID(ustr)
std_total = time.monotonic() - st
print(f'std_UUID(str):\t\t  {std_total:.4f}')

st = time.monotonic()
for _ in range(N):
    c_UUID(ustr)
c_total = time.monotonic() - st
print(f'c_UUID(str):\t\t* {c_total:.4f} ({std_total / c_total:.2f}x)')


print()

u = std_UUID(bytes=ubytes)
st = time.monotonic()
for _ in range(N):
    str(u)
std_total = time.monotonic() - st
print(f'str(std_UUID()):\t  {std_total:.4f}')

u = c_UUID(ubytes)
st = time.monotonic()
for _ in range(N):
    str(u)
c_total = time.monotonic() - st
print(f'str(c_UUID()):\t\t* {c_total:.4f} ({std_total / c_total:.2f}x)')

u = object()
st = time.monotonic()
for _ in range(N):
    str(u)
print(f'str(object()):\t\t  {time.monotonic() - st:.4f}')


print()

u = std_UUID(bytes=ubytes)
st = time.monotonic()
for _ in range(N):
    u.bytes
std_total = time.monotonic() - st
print(f'std_UUID().bytes:\t  {std_total:.4f}')


u = c_UUID(ubytes)
st = time.monotonic()
for _ in range(N):
    u.bytes
c_total = time.monotonic() - st
print(f'c_UUID().bytes:\t\t* {c_total:.4f} ({std_total / c_total:.2f}x)')


u = object()
st = time.monotonic()
for _ in range(N):
    str(u)
print(f'str(object()):\t\t  {time.monotonic() - st:.4f}')


print()

u = std_UUID(bytes=ubytes)
st = time.monotonic()
for _ in range(N):
    u.int
std_total = time.monotonic() - st
print(f'std_UUID().int:\t\t  {std_total:.4f}')


u = c_UUID(ubytes)
st = time.monotonic()
for _ in range(N):
    u.int
c_total = time.monotonic() - st
print(f'c_UUID().int:\t\t* {c_total:.4f}')
