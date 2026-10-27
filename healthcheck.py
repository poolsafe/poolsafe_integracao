import json
import math
import os
from pathlib import Path
import sys
import time


def healthy(path, max_age):
    try:
        body = json.loads(Path(path).read_text(encoding='utf-8'))
        age = time.time()-float(body['updated_at'])
        return math.isfinite(age) and 0 <= age <= max_age and body['status'] == 'ready'
    except (OSError, ValueError, TypeError, KeyError):
        return False


if __name__ == '__main__':
    sys.exit(0 if healthy(Path(os.getenv('DATA_DIR', '/dados')) / 'health.json',
                         float(os.getenv('HEALTH_STALE_S', '90'))) else 1)
