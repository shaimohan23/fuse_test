from __future__ import with_statement

import os
import sys
import errno
import requests
import urllib.parse

import subprocess

from fuse import FUSE, FuseOSError, Operations

class Passthrough(Operations):
    def __init__(self, root):
        self.root = root
        self.api_response_data = {}

    # Helpers
    # =======

    def _full_path(self, partial):
        partial = partial.lstrip("/")
        path = os.path.join(self.root, partial)
        return path

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        return os.symlink(name, self._full_path(target))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        full_path = self._full_path(path)
        path_split = full_path.split("/")

        if len(path_split) >= 3 and (path_split[-1].endswith(".h5") or path_split[-1].endswith(".hdf5")):
            udf_fields = path_split[-3:]
            udf, bucket_name, file_name = udf_fields

            print("Making API call...")
            try:
                # Change working directory to root or another safe location
                # original_cwd = os.getcwd()
                # os.chdir("/")

                api_response_bytes = udf_api_call(bucket_name, file_name, udf)
                self.api_response_data[full_path] = api_response_bytes
                print(f"API call successful, response length: {len(api_response_bytes)} bytes")
            except Exception as e:
                print(f"API call failed: {e}")
                raise FuseOSError(errno.EIO)
            # finally:
                # Restore the original working directory
                # os.chdir(original_cwd)

            return os.open("/dev/null", flags)
        else:
            return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        full_path = self._full_path(path)
        if full_path in self.api_response_data:
            # Get the API response bytes
            data = self.api_response_data[full_path]
            data_length = len(data)
            print(f"API response data length: {data_length}")
            print(f"Requested range: {offset} to {offset + length}")

            if offset >= data_length:
                print("Offset beyond data length, returning empty response.")
                return b''
            
            # Calculate the slice end to ensure we don't read out of bounds
            slice_end = min(offset + length, data_length)
            print(f"Returning data slice from {offset} to {slice_end}")

            return data[offset:slice_end]
            # return data[offset:offset + length]
        else:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


def udf_api_call(bucket_name, file_name, udf):
    # Backup environment variables
    # original_env = os.environ.copy()

    # Clear environment variables that may cause file access
    # os.environ.pop('HTTP_PROXY', None)
    # os.environ.pop('HTTPS_PROXY', None)
    # os.environ.pop('NO_PROXY', None)
    # os.environ['HOME'] = '/tmp'
    # os.environ['REQUESTS_CA_BUNDLE'] = ''

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
        user_token = json_data["AdminResponse"]["token"]

        body = {
            "bucketName": bucket_name,
            "objectName": file_name,
            "udf": udf,
            "input": '{"x": 1}',
        }

        headers = {
            "authorization": f"Bearer {user_token}",
        }

        url = "http://127.0.0.1:8010/udf_api"
        response = requests.post(url, json=body, headers=headers)
        response.raise_for_status()
    finally:
        # Restore original environment variables
        # os.environ.clear()
        # os.environ.update(original_env)
        # Write the API response bytes to stdout
        sys.stdout.buffer.write(response.content)
        return response.content

def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])