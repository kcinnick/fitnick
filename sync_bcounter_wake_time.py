import json

from fitnick.integrations.bcounter import (
    BCounterAPIError,
    BCounterConfigurationError,
    sync_latest_wake_time,
)


def main():
    try:
        result = sync_latest_wake_time()
    except (BCounterAPIError, BCounterConfigurationError) as exc:
        print(json.dumps({'ok': False, 'error': str(exc)}))
        raise SystemExit(1) from exc
    print(json.dumps(result))


if __name__ == '__main__':
    main()

