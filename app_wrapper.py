# Temporary compatibility launcher for the Scheduler UI patch.
# The full backend is kept in app_backend.py so future git pulls remain simple.
from pathlib import Path

BACKEND = Path(__file__).with_name("app_backend.py")
s = BACKEND.read_text(encoding="utf-8")
slash_n = chr(92) + "n"
start = s.find(slash_n + slash_n + '@app.get("/api/tmdb/search")')
end = s.find(slash_n + '@app.get("/api/media/{mid}")', start + 1) if start >= 0 else -1
if start >= 0 and end > start:
    block = s[start:end].replace(slash_n, "\n")
    s = s[:start] + block + s[end:]

# Execute the repaired backend as if this file were the application module.
code = compile(s, str(BACKEND), "exec")
globals_dict = globals()
globals_dict["__file__"] = str(BACKEND)
exec(code, globals_dict)
