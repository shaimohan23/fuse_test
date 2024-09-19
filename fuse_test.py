from __future__ import with_statement

import os
import sys
import time
import errno 
import requests
import urllib.parse

import subprocess

# from fuse import FUSE, FuseOSError, Operations
import fuse
from fuse import Fuse


if not hasattr(fuse, '__version__'):
    raise RuntimeError("your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)

fuse.feature_assert('stateful_files', 'has_init')

class Passthrough(Fuse):
    def __init__(self, *args, **kw):
        self.root = os.getcwd()
        self.api_response_data = {}

        Fuse.__init__(self, *args, **kw)

    # Helpers
    # =======
    def getattr(self, path):
        # return os.lstat("." + path)
        full_path = os.path.join(self.mount, path.lstrip('/'))
        
        # Create a new fuse.Stat() object to store file attributes
        st = fuse.Stat()

        # Default to the stat of the underlying file
        try:
            # If it's a regular file, use lstat
            stat_result = os.lstat("." + path)
            st.st_mode = stat_result.st_mode
            st.st_ino = stat_result.st_ino
            st.st_dev = stat_result.st_dev
            st.st_nlink = stat_result.st_nlink
            st.st_uid = stat_result.st_uid
            st.st_gid = stat_result.st_gid
            st.st_size = stat_result.st_size  # Default size (will be overridden if it's API data)
            st.st_atime = stat_result.st_atime
            st.st_mtime = stat_result.st_mtime
            st.st_ctime = stat_result.st_ctime
        except FileNotFoundError:
            pass

        # Check if the file has API response data
        if full_path in self.api_response_data:
            # Set the correct size of the file based on the response data length
            size = len(self.api_response_data[full_path])
            st.st_size = size  # Set the correct size
            print(f"Setting file size for {path}: {size} bytes")

        return st

    def readlink(self, path):
        return os.readlink("." + path)

    def readdir(self, path, offset):
        for e in os.listdir("." + path):
            yield fuse.Direntry(e)

    def unlink(self, path):
        os.unlink("." + path)

    def rmdir(self, path):
        os.rmdir("." + path)

    def symlink(self, path, path1):
        os.symlink(path, "." + path1)

    def rename(self, path, path1):
        os.rename("." + path, "." + path1)

    def link(self, path, path1):
        os.link("." + path, "." + path1)

    def chmod(self, path, mode):
        os.chmod("." + path, mode)

    def chown(self, path, user, group):
        os.chown("." + path, user, group)

    def truncate(self, path, len):
        f = open("." + path, "a")
        f.truncate(len)
        f.close()

    def mknod(self, path, mode, dev):
        os.mknod("." + path, mode, dev)

    def mkdir(self, path, mode):
        os.mkdir("." + path, mode)

    def utime(self, path, times):
        os.utime("." + path, times)

    def access(self, path, mode):
        if not os.access("." + path, mode):
            return -errno.EACCES

    def statfs(self):
        """
        Should return an object with statvfs attributes (f_bsize, f_frsize...).
        Eg., the return value of os.statvfs() is such a thing (since py 2.2).
        If you are not reusing an existing statvfs object, start with
        fuse.StatVFS(), and define the attributes.

        To provide usable information (i.e., you want sensible df(1)
        output, you are suggested to specify the following attributes:

            - f_bsize - preferred size of file blocks, in bytes
            - f_frsize - fundamental size of file blcoks, in bytes
                [if you have no idea, use the same as blocksize]
            - f_blocks - total number of blocks in the filesystem
            - f_bfree - number of free blocks
            - f_files - total number of file inodes
            - f_ffree - nunber of free file inodes
        """

        return os.statvfs(".")

    def fsinit(self):
        os.chdir(self.root)


    def _full_path(self, partial):
        partial = partial.lstrip("/")
        path = os.path.join(self.root, partial)
        return path


    # File methods
    # ============

    def open(self, path, flags):
        full_path = os.path.join(self.mount, path.lstrip('/'))
        path_split = full_path.split("/")

        if len(path_split) >= 3 and (path_split[-1].endswith(".h5") or path_split[-1].endswith(".hdf5")):
            udf_fields = path_split[-3:]
            udf, bucket_name, file_name = udf_fields

            formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            print(f"Making API call at {formatted_time}")
            try:
                params = {
                    "f": "json",
                    "loginId": "test",
                    "password": "test",
                    "application": "test",
                    "source": "flight",
                }

                encoded_params = urllib.parse.urlencode(params)
                url = f"http://127.0.0.1:8010/admin/login?{encoded_params}"

                response = requests.get(url)
                response.raise_for_status()
                json_data = response.json()
                # print(f"{json_data=}")
                user_token = json_data["AdminResponse"]["token"]

                # print(f"{bucket_name=}, {udf=}, {file_name=}")

                body = {
                    "bucketName": bucket_name,
                    "objectName": file_name,
                    "udf": udf,
                    "input": '{"x": 1}',
                }

                headers = {
                    "authorization": f"Bearer {user_token}",
                }

                url = "http://localhost:8010/udf_api"
                response = requests.post(url, json=body, headers=headers)
                # print(f"{response.content.decode('ascii')=}{response.status_code=}")
                response.raise_for_status()
                self.api_response_data[full_path] = response.content
                self.getattr(path)
                return 0
            except Exception as e:
                print(f"API call failed: {e}")
                return -errno.EIO
        else:
            return os.open(full_path, flags)
    
    def read(self, path, size, offset):
        full_path = os.path.join(self.mount, path.lstrip('/'))
        if full_path in self.api_response_data:
            data = self.api_response_data[full_path]
            data_length = len(data)
            
            # Ensure the offset is within bounds
            if offset >= data_length:
                return b''
            
            # slice_end = min(offset + size, data_length)
            # print(f"{data.decode('ascii')=}")
            # return data[offset:slice_end]
        
            slice_end = min(offset + size, data_length)
            data_slice = data[offset:slice_end]
            
            # Debug output
            print(f"Returning data slice from {offset} to {slice_end}: {data_slice}")

            # Return the requested data slice
            return data_slice
        else:
            # If the data is not found in memory, return an error or empty data
            return b''

def main():

    usage = """
Userspace nullfs-alike: mirror the filesystem tree from some point on.

""" + Fuse.fusage

    server = Passthrough(version="%prog " + fuse.__version__,
                 usage=usage,
                 dash_s_do='setsingle')

    server.parser.add_option(mountopt="root", metavar="PATH", default='/',
                             help="mirror filesystem from under PATH [default: %default]")
    server.parse(values=server, errex=1)
    server.mount = server.fuse_args.assemble()[1]

    try:
        if server.fuse_args.mount_expected():
            os.chdir(server.root)
    except OSError:
        print("can't enter root of underlying filesystem", file=sys.stderr)
        sys.exit(1)

    server.main()

if __name__ == '__main__':
    # main(sys.argv[2], sys.argv[1])
    main()