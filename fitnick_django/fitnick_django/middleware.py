import base64
import hmac
import os

from django.http import JsonResponse


def _parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def _parse_exempt_paths(value):
    if not value:
        return []
    return [item.strip() for item in value.split(',') if item.strip()]


def _safe_compare(left, right):
    if left is None or right is None:
        return False
    return hmac.compare_digest(str(left), str(right))


def _parse_basic_auth(authorization_header):
    if not authorization_header:
        return None, None
    try:
        scheme, encoded = authorization_header.split(' ', 1)
    except ValueError:
        return None, None
    if scheme.lower() != 'basic':
        return None, None

    try:
        decoded = base64.b64decode(encoded.strip()).decode('utf-8')
    except (ValueError, UnicodeDecodeError):
        return None, None
    if ':' not in decoded:
        return None, None
    username, password = decoded.split(':', 1)
    return username, password


class AccessControlMiddleware:
    """Protects all endpoints with API key and/or HTTP Basic auth."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        enabled = _parse_bool(os.getenv('FITNICK_REQUIRE_AUTH'), default=False)
        if not enabled:
            return self.get_response(request)

        if getattr(getattr(request, 'user', None), 'is_authenticated', False):
            return self.get_response(request)

        path = request.path or ''
        exempt_paths = _parse_exempt_paths(
            os.getenv('FITNICK_AUTH_EXEMPT_PATHS', '/healthz,/login,/logout,/admin/login')
        )
        if any(path.startswith(prefix) for prefix in exempt_paths):
            return self.get_response(request)

        api_key = os.getenv('FITNICK_API_KEY')
        basic_user = os.getenv('FITNICK_BASIC_AUTH_USER')
        basic_pass = os.getenv('FITNICK_BASIC_AUTH_PASS')

        methods_configured = bool(api_key) or bool(basic_user and basic_pass)
        if not methods_configured:
            return JsonResponse(
                {'ok': False, 'error': 'Authentication is enabled but no auth method is configured.'},
                status=503,
            )

        request_api_key = request.headers.get('X-API-Key')
        if api_key and _safe_compare(request_api_key, api_key):
            return self.get_response(request)

        req_user, req_pass = _parse_basic_auth(request.headers.get('Authorization', ''))
        if basic_user and basic_pass and _safe_compare(req_user, basic_user) and _safe_compare(req_pass, basic_pass):
            return self.get_response(request)

        response = JsonResponse({'ok': False, 'error': 'Unauthorized'}, status=401)
        if basic_user and basic_pass:
            response['WWW-Authenticate'] = 'Basic realm="fitnick"'
        return response

