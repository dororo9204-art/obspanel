from pathlib import Path

BACKEND = Path(__file__).with_name("app_backend.py")
s = BACKEND.read_text(encoding="utf-8")

# Normalize a mistakenly escaped TMDB/Scheduler block before compiling.
# The committed backend may contain literal characters "\\n" rather than
# actual line breaks; converting them here keeps startup backward-compatible.
if r"\n\n@app.get(\"/api/tmdb/search\")" in s:
    s = s.replace(r"\n", "\n")

code = compile(s, str(BACKEND), "exec")
globals_dict = globals()
globals_dict["__file__"] = str(BACKEND)
exec(code, globals_dict)
