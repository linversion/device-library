"""Narrow lifecycle/recovery adapters. No ordinary borrower action gateway."""
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path


class ObservationError(RuntimeError):
    pass


class VerificationError(ObservationError):
    """Commands ended, but an observed postcondition did not pass verification."""


class AndroidAdapter:
    """Fail-closed discovery. Commands always address an explicit target.

    ADB's emulator inventory alone cannot prove a stopped/offline emulator process
    does not exist. A complete host process observation is also required. Unknown
    process formats therefore block starts, rather than claiming spare capacity.
    """
    def __init__(self, adb="adb", android="android", runner=None):
        self.adb, self.android = adb, android
        self.runner = runner or self._run

    @staticmethod
    def _run(args, timeout=90):
        try:
            p = subprocess.run(args, capture_output=True, text=True, timeout=timeout,
                               check=False)
        except (subprocess.SubprocessError, OSError) as exc:
            raise ObservationError("command result unknown: "+Path(args[0]).name) from exc
        if p.returncode:
            raise ObservationError("command failed: " + Path(args[0]).name)
        return p.stdout

    def adb_target(self, serial, *args, timeout=90):
        if not serial or serial.startswith("-"):
            raise ObservationError("invalid explicit serial")
        command = [self.adb, "-s", serial, *args]
        if self.runner == self._run:
            return self._run(command, timeout=timeout)
        return self.runner(command)

    def observe(self, devices):
        raw = self.runner([self.adb, "devices", "-l"])
        if not raw.startswith("List of devices attached"):
            raise ObservationError("unrecognized adb inventory")
        transports, emulators = {}, []
        for line in raw.splitlines()[1:]:
            if not line.strip():
                continue
            parts = line.split()
            if len(parts) < 2:
                raise ObservationError("incomplete transport inventory")
            serial, status = parts[:2]
            transport = next((p.split(":", 1)[1] for p in parts[2:]
                              if p.startswith("transport_id:")), None)
            entry = {"serial": serial, "transport": transport, "online": status == "device"}
            if serial.startswith("emulator-"):
                if status != "device":
                    raise ObservationError("offline emulator identity unknown")
                names = self.adb_target(serial, "emu", "avd", "name").splitlines()
                if not names or not names[0].strip() or names[0].strip() == "OK":
                    raise ObservationError("emulator AVD identity empty")
                avd = names[0].strip()
                entry["avd"] = avd
                emulators.append(entry)
            transports[serial] = entry
        # Observe emulator processes, including instances invisible to adb.
        # Match the executable, not arbitrary arguments (agent prompts can
        # contain the word "emulator"). Fetch arguments only for real candidates.
        ps = self.runner(["ps", "-axo", "pid=,comm="])
        process_avds = []
        process_details = {}
        for line in ps.splitlines():
            if not line.strip():
                continue
            fields = line.strip().split(maxsplit=1)
            if len(fields) != 2 or not fields[0].isdigit():
                raise ObservationError("host process inventory malformed")
            pid, executable = fields
            if re.fullmatch(r"qemu-system-[\w-]+|emulator", Path(executable).name):
                details = self.runner(["ps", "eww", "-p", pid, "-o", "command="])
                match = re.search(r"(?:-avd\s+|@)([\w. -]+?)(?=\s+(?:-|[A-Za-z_]\w*=)|$)", details)
                if not match:
                    raise ObservationError("emulator process identity uncertain")
                name = match.group(1).strip()
                process_avds.append(name)
                process_details[name] = details
        visible = [e["avd"] for e in emulators]
        if sorted(process_avds) != sorted(visible):
            raise ObservationError("process and transport inventories disagree")
        result = {}
        for d in devices:
            spec = d["spec"]
            if d["kind"] == "physical":
                aliases = spec.get("serials", [])
                candidates = [transports[s] for s in aliases if s in transports]
                online = [e for e in candidates if e["online"]]
                entry = dict(online[0]) if online else {"online": False}
                if online:
                    hardware = self.adb_target(entry["serial"], "shell", "getprop", "ro.serialno").strip()
                    model = self.adb_target(entry["serial"], "shell", "getprop", "ro.product.model").strip()
                    if hardware != spec["hardware_serial"] or model != spec["model"]:
                        raise ObservationError("physical identity mismatch")
                    entry["identity"] = hardware
            else:
                # Resolve the launch descriptor to storage on every observation.
                # Launch and directory basenames need not match; the path and
                # directory inode + config hash must still match registration.
                self.validate_registration("emulator", spec)
                hits = [e for e in emulators if e["avd"] == spec["avd_name"]]
                if hits:
                    details = process_details.get(spec["avd_name"], "")
                    root = str(Path(spec["avd_path"]).resolve().parent)
                    sdk = str(Path(spec["sdk_root"]).resolve())
                    # Name is insufficient: verify actual process SDK executable and
                    # inherited AVD root. Unsupported ps formats fail closed.
                    if not re.search(r"(?:^|\s)"+re.escape(sdk)+r"/emulator/(?:emulator|qemu/[^ ]+)", details) or not re.search(r"(?:^|\s)ANDROID_AVD_HOME="+re.escape(root)+r"(?:\s|$)", details):
                        raise ObservationError("running AVD root cannot be proven")
                if len(hits) > 1:
                    raise ObservationError("duplicate writable AVD instances")
                entry = dict(hits[0]) if hits else {"online": False}
                entry["identity"] = spec["avd_identity"]
            result[d["id"]] = entry
        # Name alone cannot disambiguate same-name AVDs across SDK roots.
        names = [d["spec"]["avd_name"] for d in devices if d["kind"] == "emulator"]
        if len(names) != len(set(names)):
            raise ObservationError("ambiguous AVD name across roots")
        return {"complete": True, "devices": result,
                "instances": [{"key": e["serial"], **e} for e in emulators]}

    def validate_registration(self, kind, spec):
        if kind != "emulator":
            return
        try:
            path = Path(spec["avd_path"]).resolve(strict=True)
            descriptor = path.parent / (spec["avd_name"] + ".ini")
            values = [line.split("=", 1)[1].strip()
                      for line in descriptor.read_text().splitlines()
                      if line.split("=", 1)[0].strip() == "path" and "=" in line]
            if len(values) != 1 or not Path(values[0]).is_absolute() or Path(values[0]).resolve(strict=True) != path:
                raise ObservationError("AVD launch descriptor does not point to registered storage")
        except OSError as exc:
            raise ObservationError("AVD launch descriptor missing or unreadable") from exc
        if avd_identity(path) != spec["avd_identity"]:
            raise ObservationError("AVD storage identity changed")

    def start(self, device):
        spec = device["spec"]
        self.validate_registration("emulator", spec)
        self.runner(["/usr/bin/env", "ANDROID_AVD_HOME="+str(Path(spec["avd_path"]).resolve().parent),
                     "ANDROID_HOME="+str(Path(spec["sdk_root"]).resolve()),
                     "ANDROID_SDK_ROOT="+str(Path(spec["sdk_root"]).resolve()),
                     self.android, "emulator", "start", spec["avd_name"]])

    def stop(self, device, observation):
        self.runner([self.android, "emulator", "stop", observation["serial"]])

    def healthy(self, device, observation):
        if not observation.get("online"):
            return False
        serial = observation["serial"]
        booted = self.adb_target(serial, "shell", "getprop", "sys.boot_completed").strip()
        if booted != "1":
            return False
        self.adb_target(serial, "shell", "dumpsys", "window", "windows")
        return True

    def layout(self, serial):
        # First-use instrumentation installation logs share stdout with JSON.
        # Use the CLI's machine-output file, never trim/guess JSON in log text.
        # Preparation needs a current tree, not an idle UI: a running stopwatch
        # makes CLI 1.0.16261425 exit 0 without a file unless --no-idle is set.
        with tempfile.TemporaryDirectory(prefix="device-library-layout-") as directory:
            for attempt in range(3):
                # Boot completion can precede the first obtainable layout. Reobserve
                # only a missing output, with a fresh path so a stale file cannot pass.
                path = Path(directory) / f"layout-{attempt}.json"
                self.runner([self.android, "layout", "--device", serial, "--full", "--no-idle",
                             "--output", str(path)])
                try:
                    raw = path.read_text(encoding="utf-8")
                except FileNotFoundError as exc:
                    if attempt == 2:
                        raise ObservationError("layout output file missing or unreadable") from exc
                    time.sleep(1)
                    continue
                except (OSError, UnicodeError) as exc:
                    raise ObservationError("layout output file missing or unreadable") from exc
                try:
                    return layout_nodes(raw)
                except (ValueError, RecursionError) as exc:
                    raise ObservationError("layout output file contains invalid JSON") from exc

    def prepare(self, device, observation):
        actions = []
        if not observation.get("online"):
            return {"healthy": False, "recoveries": []}
        if device["kind"] == "physical" and device["spec"].get("model") == "Pixel 7" and device["spec"].get("lock_policy") == "no-credential-wake":
            actions.append({"kind": "wake", **self.recover(device, observation, "wake")})
        if device["kind"] == "emulator":
            nodes = self.layout(observation["serial"])
            if any(n.get("resourceId") == "android:id/aerr_close" for n in nodes):
                actions.append({"kind": "close-system-anr", **self.recover(device, observation, "close-system-anr")})
        return {"healthy": self.healthy(device, observation), "recoveries": actions}

    @staticmethod
    def _policy_unlocked(policy):
        # Explicit unlocked evidence; unknown OS formats remain unknown.
        fields = re.findall(r"\b(isStatusBarKeyguard|mShowingLockscreen|mKeyguardShowing|isKeyguardShowing)\s*[=:]\s*(true|false)\b", policy)
        credential = re.search(r"\b(?:mInputRestricted|isKeyguardSecure|mKeyguardSecure)\s*[=:]\s*true\b", policy)
        if fields:
            unlocked = all(v == "false" for _, v in fields)
        else:
            # Android 15 KeyguardServiceDelegate omits the legacy fields.
            # Both explicit signals are required; absent or conflicting
            # values never count as unlocked.
            modern = re.findall(r"\b(inputRestricted|dreaming)\s*[=:]\s*(true|false)\b", policy)
            unlocked = ({name for name, _ in modern} == {"inputRestricted", "dreaming"}
                        and all(v == "false" for _, v in modern))
        return unlocked and not credential

    def _verify_unlocked(self, serial):
        # Wake/dismiss can finish while keyguard is still leaving doze. Bound
        # both observations and elapsed time; each real adb read shares the budget.
        deadline = time.monotonic() + 2
        for attempt in range(21):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            policy = self.adb_target(serial, "shell", "dumpsys", "window", "policy", timeout=remaining)
            if self._policy_unlocked(policy):
                return
            if attempt < 20:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(.1, remaining))
        raise VerificationError("unlock not verified")

    def recover(self, device, observation, kind):
        serial = observation["serial"]
        if kind == "wake":
            if device["kind"] != "physical" or device["spec"].get("model") != "Pixel 7" or device["spec"].get("lock_policy") != "no-credential-wake":
                raise ObservationError("wake policy not registered")
            self.adb_target(serial, "shell", "input", "keyevent", "KEYCODE_WAKEUP")
            self.adb_target(serial, "shell", "wm", "dismiss-keyguard")
            self._verify_unlocked(serial)
        elif kind == "close-system-anr":
            if device["kind"] != "emulator":
                raise ObservationError("system ANR recovery limited to emulators")
            nodes = self.layout(serial)
            window = self.adb_target(serial, "shell", "dumpsys", "window", "windows")
            if not re.search(r"(?:Application Not Responding|ANR):?\s+(?:com\.android\.systemui|system_server|system|android)(?=[\s}]|$)", window, re.I):
                raise ObservationError("system ANR window not verified")
            system_text = any(n.get("package") == "android" and
                              re.search(r"(?:System UI|system process).*?(?:isn't responding|not responding)", n.get("text", ""), re.I)
                              for n in nodes)
            buttons = [n for n in nodes if n.get("package") == "android" and
                       n.get("resourceId") == "android:id/aerr_close" and n.get("enabled") is True]
            if not system_text or len(buttons) != 1:
                raise ObservationError("system ANR Close identity not verified")
            b = buttons[0].get("bounds")
            if isinstance(b, dict):
                coords = [b.get(k) for k in ("left", "top", "right", "bottom")]
            elif isinstance(b, list) and len(b) == 4:
                coords = b
            else:
                raise ObservationError("Close bounds unknown")
            if not all(isinstance(c, int) and not isinstance(c, bool) and c >= 0 for c in coords):
                raise ObservationError("invalid Close bounds")
            x1, y1, x2, y2 = coords
            if x2 <= x1 or y2 <= y1:
                raise ObservationError("invalid Close bounds")
            self.adb_target(serial, "shell", "input", "tap", str((x1+x2)//2), str((y1+y2)//2))
            after = self.layout(serial)
            if any(n.get("resourceId") == "android:id/aerr_close" for n in after):
                raise ObservationError("ANR Close did not clear dialog")
        else:
            raise ObservationError("unsupported recovery")
        if not self.healthy(device, observation):
            raise ObservationError("recovery health not verified")
        return {"interrupted": kind == "close-system-anr", "baseline": "retained; borrower actions unobserved",
                "verified": "Android system ANR resource/window/package; Close cleared; health checked" if kind == "close-system-anr" else "explicit unlocked keyguard fields; health checked"}


def avd_identity(path):
    import hashlib
    p = Path(path).resolve(strict=True)
    st = p.stat()
    return f"{st.st_dev}:{st.st_ino}:" + hashlib.sha256((p / "config.ini").read_bytes()).hexdigest()


class FakeAdapter:
    """Explicit sandbox fixture; cannot invoke a real device tool."""
    def __init__(self, fixture):
        self.path = Path(fixture)
        self.calls = []

    def read(self):
        return json.loads(self.path.read_text())

    def observe(self, devices):
        f = self.read()
        if not f.get("complete", True):
            raise ObservationError("fixture incomplete")
        return {"complete": True, "devices": {d["id"]: f.get("devices", {}).get(d["id"], {"online": False}) for d in devices}, "instances": f.get("instances", [])}

    def change(self, device, running):
        f = self.read()
        self.calls.append(("start" if running else "stop", device["id"]))
        if f.get("fail_" + ("start" if running else "stop")):
            raise ObservationError("fixture injected lifecycle failure")
        serial = "emulator-" + device["id"][:6]
        f.setdefault("devices", {})[device["id"]] = {"online": running, "serial": serial, "transport": "1", "identity": device["spec"].get("avd_identity", device["id"])}
        f["instances"] = [e for e in f.get("instances", []) if e.get("device_id") != device["id"]]
        if running:
            f["instances"].append({"key": serial, "serial": serial, "device_id": device["id"]})
        self.path.write_text(json.dumps(f))

    def validate_registration(self, kind, spec):
        pass

    def start(self, device):
        self.change(device, True)

    def stop(self, device, observation):
        self.change(device, False)

    def healthy(self, device, observation):
        return observation.get("online", False) and not self.read().get("unhealthy", False)

    def prepare(self, device, observation):
        return {"healthy": self.healthy(device, observation), "recoveries": []}

    def recover(self, device, observation, kind):
        self.calls.append((kind, device["id"]))
        if self.read().get("fail_recover"):
            raise ObservationError("fixture recovery failed")
        return {"interrupted": kind == "close-system-anr", "baseline": "retained"}


def layout_nodes(raw):
    """Flatten Android CLI JSON, retaining only actual element dictionaries.

    JSON schema differences that omit package/resource identity are unsupported
    and will be rejected by recovery rather than clicking a guessed Close.
    """
    value = json.loads(raw)
    if not isinstance(value, (dict, list)):
        raise ObservationError("layout must be JSON object or array")
    result = []
    def visit(node):
        if isinstance(node, dict):
            if "resourceId" in node or "text" in node:
                item = dict(node)
                # CLI versions may call the Android package field packageName.
                if "package" not in item and "packageName" in item:
                    item["package"] = item["packageName"]
                result.append(item)
            for child in node.values():
                if isinstance(child, (dict, list)):
                    visit(child)
        elif isinstance(node, list):
            for child in node:
                visit(child)
    visit(value)
    return result
