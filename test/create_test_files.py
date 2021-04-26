import tempfile

from sh import ipfs

TESTDATA = "ipfsspec test data"


def create_file(data, *args, **kwargs):
    with tempfile.NamedTemporaryFile(mode="w") as tf:
        tf.write(data)
        tf.flush()
        return ipfs.add(tf.name, "-q", pin=False, *args, **kwargs).stdout.decode("ascii").strip()


def create_testdata(name, data, *args, **kwargs):
    cid = create_file(data, *args, **kwargs)
    ipfs.files.cp("/ipfs/" + cid, "/ipfsspec_testdata/" + name)


def main():
    ipfs.files.mkdir("/ipfsspec_testdata")
    create_testdata("default", TESTDATA)
    create_testdata("multi", TESTDATA, s="size-2")
    create_testdata("raw", TESTDATA, "--raw-leaves")
    create_testdata("raw_multi", TESTDATA, "--raw-leaves", s="size-2")
    ipfs.files.write("-e", "-p", "-t", "--raw-leaves", "/ipfsspec_testdata/write", _in=TESTDATA)
    cid = ipfs.files.stat("/ipfsspec_testdata", "--hash").stdout.decode("ascii").strip()
    print(f"testdata cid: {cid}")


if __name__ == "__main__":
    main()
