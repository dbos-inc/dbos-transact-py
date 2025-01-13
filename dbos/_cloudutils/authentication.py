import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import jwt
import requests
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from cryptography.x509 import load_pem_x509_certificate
from rich import print

from .._logger import dbos_logger

# Constants
DBOS_CLOUD_HOST = os.getenv("DBOS_DOMAIN", "cloud.dbos.dev")
PRODUCTION_ENVIRONMENT = DBOS_CLOUD_HOST == "cloud.dbos.dev"
AUTH0_DOMAIN = "login.dbos.dev" if PRODUCTION_ENVIRONMENT else "dbos-inc.us.auth0.com"
DBOS_CLIENT_ID = (
    "6p7Sjxf13cyLMkdwn14MxlH7JdhILled"
    if PRODUCTION_ENVIRONMENT
    else "G38fLmVErczEo9ioCFjVIHea6yd0qMZu"
)
DBOS_CLOUD_IDENTIFIER = "dbos-cloud-api"


@dataclass
class DeviceCodeResponse:
    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str
    expires_in: int
    interval: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DeviceCodeResponse":
        return cls(
            device_code=data["device_code"],
            user_code=data["user_code"],
            verification_uri=data["verification_uri"],
            verification_uri_complete=data["verification_uri_complete"],
            expires_in=data["expires_in"],
            interval=data["interval"],
        )


@dataclass
class TokenResponse:
    access_token: str
    token_type: str
    expires_in: int
    refresh_token: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TokenResponse":
        return cls(
            access_token=data["access_token"],
            token_type=data["token_type"],
            expires_in=data["expires_in"],
            refresh_token=data.get("refresh_token"),
        )


@dataclass
class AuthenticationResponse:
    token: str
    refresh_token: Optional[str] = None


class JWKSClient:
    def __init__(self, jwks_uri: str):
        self.jwks_uri = jwks_uri

    def get_signing_key(self, kid: str) -> RSAPublicKey:
        response = requests.get(self.jwks_uri)
        jwks = response.json()
        for key in jwks["keys"]:
            if key["kid"] == kid:
                cert_text = f"-----BEGIN CERTIFICATE-----\n{key['x5c'][0]}\n-----END CERTIFICATE-----"
                cert = load_pem_x509_certificate(cert_text.encode())
                return cert.public_key()  # type: ignore
        raise Exception(f"Unable to find signing key with kid: {kid}")


def verify_token(token: str) -> None:
    header = jwt.get_unverified_header(token)

    if not header.get("kid"):
        raise ValueError("Invalid token: No 'kid' in header")

    client = JWKSClient(f"https://{AUTH0_DOMAIN}/.well-known/jwks.json")
    signing_key = client.get_signing_key(header["kid"])
    jwt.decode(
        token,
        signing_key,
        algorithms=["RS256"],
        audience=DBOS_CLOUD_IDENTIFIER,
        options={
            "verify_iat": False,
            "clock_tolerance": 60,
        },
    )


def authenticate(get_refresh_token: bool = False) -> Optional[AuthenticationResponse]:
    print(
        "[bold blue]Please authenticate with DBOS Cloud to access a Postgres database[/bold blue]"
    )

    # Get device code
    device_code_data = {
        "client_id": DBOS_CLIENT_ID,
        "scope": "offline_access" if get_refresh_token else "sub",
        "audience": DBOS_CLOUD_IDENTIFIER,
    }

    try:
        response = requests.post(
            f"https://{AUTH0_DOMAIN}/oauth/device/code",
            data=device_code_data,
            headers={"content-type": "application/x-www-form-urlencoded"},
        )
        device_code_response = DeviceCodeResponse.from_dict(response.json())
    except Exception as e:
        dbos_logger.error(f"Failed to log in: {str(e)}")
        return None

    login_url = device_code_response.verification_uri_complete
    print(f"[bold blue]Login URL:[/bold blue] {login_url}")

    # Poll for token
    token_data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "device_code": device_code_response.device_code,
        "client_id": DBOS_CLIENT_ID,
    }

    elapsed_time_sec = 0
    token_response = None

    while elapsed_time_sec < device_code_response.expires_in:
        try:
            time.sleep(device_code_response.interval)
            elapsed_time_sec += device_code_response.interval

            response = requests.post(
                f"https://{AUTH0_DOMAIN}/oauth/token",
                data=token_data,
                headers={"content-type": "application/x-www-form-urlencoded"},
            )
            if response.status_code == 200:
                token_response = TokenResponse.from_dict(response.json())
                break
        except Exception:
            dbos_logger.info("Waiting for login...")

    if not token_response:
        return None

    verify_token(token_response.access_token)
    return AuthenticationResponse(
        token=token_response.access_token, refresh_token=token_response.refresh_token
    )
