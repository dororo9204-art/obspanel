from pathlib import Path

BACKEND = Path(__file__).with_name("app_backend.py")
s = BACKEND.read_text(encoding="utf-8")

# Repair the scheduler/TMDB block if it was committed with literal \\n
# escape sequences. The malformed block is recognizable by the TMDB search
# endpoint marker. Only perform this normalization when that marker exists.
if r'\\n\\n@app.get("/api/tmdb/search")' in s:
    s = s.replace(r'\\n', '\n')

code = compile(s, str(BACKEND), "exec")
globals_dict = globals()
globals_dict["__file__"] = str(BACKEND)
exec(code, globals_dict)
