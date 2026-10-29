"""Local kernel process identities; unreadable is distinct from confirmed exit."""
import ctypes
import errno
import os
from pathlib import Path
import sys


class ProcessUnknown(Exception):
    pass


def identity(pid):
    """Return stable birth identity, None for exit/zombie, or raise uncertainty."""
    if type(pid) is not int or not 1 < pid <= 2**31-1:
        raise ProcessUnknown("owner PID must fit a positive kernel pid_t greater than one")
    try:
        if sys.platform == "linux":
            root = Path('/proc') / str(pid)
            stat = (root / 'stat').read_text()
            fields = stat[stat.rindex(')') + 2:].split()
            if fields[0] in ('Z', 'X'):
                return None
            if root.stat().st_uid != os.getuid():
                raise ProcessUnknown("owner belongs to another user")
            boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
            return f"linux:{boot}:{fields[19]}"
        if sys.platform == "darwin":
            class BSDInfo(ctypes.Structure):
                _fields_ = [(name, ctypes.c_uint32) for name in
                            ('flags', 'status', 'xstatus', 'pid', 'ppid', 'uid', 'gid',
                             'ruid', 'rgid', 'svuid', 'svgid', 'reserved')]
                _fields_ += [('comm', ctypes.c_char * 16), ('name', ctypes.c_char * 32)]
                _fields_ += [(name, ctypes.c_uint32) for name in
                             ('nfiles', 'pgid', 'jobc', 'tdev', 'tpgid', 'nice')]
                _fields_ += [('start_sec', ctypes.c_uint64), ('start_usec', ctypes.c_uint64)]
            lib = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
            lib.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
                                        ctypes.c_void_p, ctypes.c_int]
            lib.proc_pidinfo.restype = ctypes.c_int
            info = BSDInfo()
            size = lib.proc_pidinfo(pid, 3, 0, ctypes.byref(info), ctypes.sizeof(info))
            if size != ctypes.sizeof(info):
                if size == 0 and ctypes.get_errno() == errno.ESRCH:
                    return None
                raise ProcessUnknown("kernel process information unavailable")
            if info.status == 5:  # SZOMB
                return None
            if info.pid != pid or info.uid != os.getuid() or not info.start_sec:
                raise ProcessUnknown("kernel process identity invalid")
            return f"darwin:{info.start_sec}:{info.start_usec}"
        raise ProcessUnknown("unsupported process identity platform")
    except (FileNotFoundError, ProcessLookupError):
        # Only /proc/<pid> disappearance proves exit; a missing proc mount does not.
        if sys.platform == 'linux' and Path('/proc/self/stat').exists():
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return None
            except PermissionError:
                pass
        raise ProcessUnknown("process information disappeared")
    except (OSError, ValueError, IndexError) as exc:
        raise ProcessUnknown(type(exc).__name__) from exc


def state(owner):
    try:
        current = identity(owner['pid'])
        return 'exited' if current is None or current != owner['identity'] else 'alive'
    except ProcessUnknown:
        return 'unknown'
