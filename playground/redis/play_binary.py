FILE = "testfile.txt"

with open(FILE, 'wb') as f:
    # f.write(b'%d\r\n' % 2)
    pass

with open(FILE, 'rb') as f:
    while True:
        try:
            data = f.read(1)
            print(f"Expect binary, actual {type(data)}")

            print(f"read: {data}")
            print(f"string: {data.decode("utf-8")}")
            print(f"string stripped: {data.decode("utf-8").rstrip('\r\n')}")

            nextline = f.read(1)
            print(nextline)

            nextline = f.read(1)
            print(nextline)
            print("FINISH")
        except KeyError:
            pass