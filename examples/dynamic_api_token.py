import json, os, tempfile
from datetime import datetime

def atomic_write_json(path, data):
    if not os.path.exists(path):
        with open(path, "a"):
            pass

    dir_name = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        # Atomic replace across platforms (renaming a file is atomic)
        os.replace(tmp_path, path)
    finally:
        # If something failed before replace, clean up
        if os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass

def safe_read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def is_api_token_applicable(token):
    # Checks whether the token is within a predefined list of valid tokens
    return token in ["AUTH_TOKEN"]

def refreshToken(username, password, token_name):
    # Implementation-specific logic to retrieve a new token
    return 'SOME_VALUE', 10 # value, exp_time

def get_api_token(token_name):
    if is_api_token_applicable(token_name):
        # if there is no cache yet
        if os.path.exists('token_cache.json'):
            cache = safe_read_json('token_cache.json')
            token_expires_at = datetime.fromtimestamp(cache[f"{token_name}_EXPIRES_AT"])
            if (token_expires_at - datetime.now()).total_seconds() > 0:
                return cache[token_name]
        else:
            cache = {}

        # Refresh cache
        credentials = os.getenv("SECRET_MANAGER_CREDENTIALS")

        token_value, token_expires_in = refreshToken(credentials.username, credentials.password, token_name)

        # token_expires_in should be in seconds
        cache[f"{token_name}_EXPIRES_AT"] = datetime.now().timestamp() + token_expires_in
        cache[token_name] = token_value
        atomic_write_json('token_cache.json', cache)

        return token_value
    else:
        print(f"ERROR: API token {token_name} is not valid.")
