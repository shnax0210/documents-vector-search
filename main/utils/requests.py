import json
import requests


def __mask_fields(data: dict, fields: list[str]) -> None:
    for field_path in fields:
        keys = field_path.split(".")
        obj = data
        for key in keys[:-1]:
            if not isinstance(obj, dict) or key not in obj:
                break
            obj = obj[key]
        else:
            if isinstance(obj, dict) and keys[-1] in obj:
                obj[keys[-1]] = "***"


def raise_for_status_with_details(response: requests.Response, masked_fields: list[str] = ["request.headers.Authorization"]) -> None:
    if response.ok:
        return

    response_body = None
    try:
        response_body = response.json()
    except Exception:
        response_body = response.text or None

    error_details_data = {
        "request": {
            "url": response.url,
            "headers": dict(response.request.headers),
            "body": response.request.body,
        },
        "response": {
            "status": response.status_code,
            "headers": dict(response.headers),
            "body": response_body,
        },
    }

    if masked_fields:
        __mask_fields(error_details_data, masked_fields)

    error_details = json.dumps(error_details_data, indent=2, default=str)

    raise requests.HTTPError(
        f"HTTP request failed:\n{error_details}",
        response=response,
    )
