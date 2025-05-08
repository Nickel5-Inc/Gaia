import os
import re
import sys
import asyncio
import aiohttp
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from fiber.logging_utils import get_logger
logger = get_logger(__name__)

BASE_URL = "https://urs.earthdata.nasa.gov"
env_path = find_dotenv(usecwd=True)
if not env_path:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    env_path = project_root / ".env"
    if not env_path.exists():
        env_path = ".env"

ENV_FILE_PATH = str(env_path)
JWT_ENV_VAR = "EARTHDATA_API_KEY"
JWT_EXP_ENV_VAR = "EARTHDATA_API_KEY_EXPIRATION"
TOKEN_EXPIRATION_BUFFER = timedelta(days=7)

async def load_credentials():
    """
    Load Earthdata credentials (username/password)
    """
    load_dotenv(dotenv_path=ENV_FILE_PATH, override=True)
    username = os.getenv("EARTHDATA_USERNAME")
    password = os.getenv("EARTHDATA_PASSWORD")

    if not username or not password:
        logger.error(f"EARTHDATA_USERNAME or EARTHDATA_PASSWORD not set in {ENV_FILE_PATH}.")
        raise ValueError("Earthdata credentials not found.")

    return username, password


async def generate_new_token(username, password, session):
    """
    POST /api/users/token - Generates a new user token using provided session.
    Returns a dict with 'access_token', 'token_type', 'expiration_date', etc.
    """
    endpoint = f"{BASE_URL}/api/users/token"
    try:
        auth = aiohttp.BasicAuth(username, password)
        async with session.post(endpoint, auth=auth) as response:
            if response.status != 200:
                error_text = await response.text()
                if response.status == 403:
                    logger.warning("Forbidden (403): You might have reached your token limit.")
                else:
                     logger.error(f"Token generation failed ({response.status}): {error_text}")
                raise aiohttp.ClientResponseError(
                    response.request_info,
                    response.history,
                    status=response.status,
                    message=error_text,
                    headers=response.headers
                )
            return await response.json()
    except aiohttp.ClientResponseError as e:
        raise e
    except Exception as e:
        logger.exception(f"Unexpected error during token generation: {e}")
        raise


async def list_tokens(username, password, session):
    """
    GET /api/users/tokens - Lists existing user tokens using provided session.
    Returns a list of token info dicts.
    """
    endpoint = f"{BASE_URL}/api/users/tokens"
    try:
        auth = aiohttp.BasicAuth(username, password)
        async with session.get(endpoint, auth=auth) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        logger.error(f"Error listing tokens: {e}")
        return []


async def revoke_token(username, password, token_string, session):
    """
    POST /api/users/revoke_token - Revokes an existing user token using provided session.
    """
    endpoint = f"{BASE_URL}/api/users/revoke_token"
    data = {"token": token_string}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        auth = aiohttp.BasicAuth(username, password)
        async with session.post(endpoint, auth=auth, data=data, headers=headers) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"HTTP Error in revoke_token: {response.status}")
                logger.error(f"Response content: {error_text}")
                return {"status": "error", "message": f"HTTP error: {response.status}"}
            return await response.json()
    except Exception as e:
        logger.error(f"Error revoking token: {e}")
        return {"status": "error", "message": str(e)}


async def revoke_all_tokens(username, password, session):
    """
    List all tokens, then revoke each one using provided session.
    """
    tokens = await list_tokens(username, password, session)
    if not tokens:
        logger.info("No tokens found to revoke.")
        return

    revoked_count = 0
    tasks = []
    for t in tokens:
        token_str = t.get("access_token")
        if not token_str:
            logger.warning(f"Cannot revoke token with metadata: {t}. 'access_token' missing.")
            continue
        tasks.append(revoke_token(username, password, token_str, session))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result, token_info in zip(results, tokens):
        token_str = token_info.get("access_token")
        if isinstance(result, Exception):
             logger.error(f"Could not revoke token {token_str[:10]}...: {result}")
        elif result.get("status") == "error":
             logger.error(f"Could not revoke token {token_str[:10]}...: {result.get('message')}")
        else:
             logger.info(f"Revoked token {token_str[:10]}... result: {result}")
             revoked_count += 1
    logger.info(f"Revoked {revoked_count}/{len(tasks)} tokens.")


def update_env_var_sync(var_name, var_value, env_file=ENV_FILE_PATH):
    abs_path = os.path.abspath(env_file)
    logger.debug(f"Updating {var_name} in {env_file} (absolute: {abs_path})")

    try:
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    except Exception as e:
         logger.error(f"Could not create directory for .env file: {e}")
         return False

    try:
        content = ""
        if os.path.exists(env_file):
            if not os.access(env_file, os.W_OK):
                logger.error(f"No write permission to {env_file}")
                return False

            if not os.access(os.path.dirname(abs_path), os.W_OK):
                 logger.error(f"No write permission to directory {os.path.dirname(abs_path)}")
                 return False

            try:
                with open(env_file, 'r') as file:
                    content = file.read()
                logger.debug(f"Read {len(content)} chars from existing .env")

            except Exception as e:
                 logger.error(f"Could not read existing .env file {env_file}: {e}")

        elif not os.access(os.path.dirname(abs_path), os.W_OK):
             logger.error(f"No write permission to directory {os.path.dirname(abs_path)} to create .env")
             return False


        pattern = re.compile(rf"^{re.escape(var_name)}=.*$", re.MULTILINE)
        new_line = f"{var_name}={str(var_value)}"

        if pattern.search(content):
            new_content = pattern.sub(new_line, content)
            logger.debug(f"Replacing existing {var_name}")
        else:
            new_content = content
            if content and not content.endswith('\n'):
                new_content += '\n'
            new_content += new_line + '\n'
            logger.debug(f"Appending new {var_name}")

        try:
            with open(env_file, 'w') as file:
                file.write(new_content)

        except Exception as e:
             logger.error(f"Could not write to .env file {env_file}: {e}")
             return False

        try:
            with open(env_file, 'r') as file:
                updated_content = file.read()

            if new_line in updated_content:
                logger.info(f"Successfully verified update for {var_name} in {env_file}")
                return True
            else:
                logger.error(f"Failed to verify {var_name} update. File written but change not found.")
                logger.debug(f"Expected line: {new_line}")
                logger.debug(f"File content sample: {updated_content[:200]}...")
                return False

        except Exception as e:
             logger.error(f"Could not re-read .env file for verification {env_file}: {e}")
             return False

    except Exception as e:
        logger.exception(f"Unexpected exception while updating .env: {e}")
        return False

async def _update_env_and_os(token_value, expiration_date_str):
    """Helper to update .env and os.environ with token and expiration."""
    loop = asyncio.get_running_loop()

    token_update_success = await loop.run_in_executor(
        None, update_env_var_sync, JWT_ENV_VAR, token_value, ENV_FILE_PATH
    )

    exp_update_success = await loop.run_in_executor(
        None, update_env_var_sync, JWT_EXP_ENV_VAR, expiration_date_str, ENV_FILE_PATH
    )

    if token_update_success and exp_update_success:
        logger.info(f"Successfully updated {JWT_ENV_VAR} and {JWT_EXP_ENV_VAR} in {ENV_FILE_PATH}")
        os.environ[JWT_ENV_VAR] = token_value
        os.environ[JWT_EXP_ENV_VAR] = expiration_date_str
        logger.info(f"Updated os.environ with new token ({token_value[:10]}...) and expiration ({expiration_date_str}).")
        return True
    else:
        logger.error(f"Failed to update .env file (Token success: {token_update_success}, Exp success: {exp_update_success}). Using new token in memory only.")
        os.environ[JWT_ENV_VAR] = token_value
        os.environ[JWT_EXP_ENV_VAR] = expiration_date_str
        return False

async def is_token_valid_via_api(username, password, session):
    """
    Checks token validity *by calling the Earthdata API* (list_tokens).
    Returns tuple: (is_valid_on_api, token_from_env, expiration_date_from_api or None)
    """
    try:
        load_dotenv(dotenv_path=ENV_FILE_PATH, override=True)
        current_token_from_env = os.getenv(JWT_ENV_VAR)

        if not current_token_from_env:
            logger.info("API Check: No current token found in environment.")
            return False, None, None

        tokens_on_api = await list_tokens(username, password, session)
        if not tokens_on_api:
            logger.info("API Check: No tokens listed on Earthdata API.")
            return False, current_token_from_env, None

        token_info = None
        for t in tokens_on_api:
            if t.get('access_token') == current_token_from_env:
                token_info = t
                break

        if not token_info:
            logger.info("API Check: Token from environment not found on Earthdata API.")
            return False, current_token_from_env, None

        exp_date_str = token_info.get('expiration_date')
        if not exp_date_str:
             logger.warning("API Check: Token found on API has no expiration date.")
             return False, current_token_from_env, None

        try:
            exp_date = datetime.strptime(exp_date_str, '%m/%d/%Y')
            exp_date = exp_date.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
            current_date_utc = datetime.now(timezone.utc)
            
            is_valid = current_date_utc < exp_date
            if not is_valid:
                logger.info(f"API Check: Token ({current_token_from_env[:10]}...) has expired ({exp_date_str}).")
            else:
                logger.info(f"API Check: Token ({current_token_from_env[:10]}...) is valid on API until {exp_date_str}.")

            return is_valid, current_token_from_env, exp_date_str
        except ValueError:
             logger.error(f"API Check: Invalid expiration date format from API: {exp_date_str}")
             return False, current_token_from_env, None

    except Exception as e:
        logger.exception(f"API Check: Error checking token validity via API: {e}")
        return False, os.getenv(JWT_ENV_VAR), None

async def revoke_oldest_token(username, password, session):
    """
    Find and revoke the oldest token using provided session.
    Returns True if successful, False otherwise.
    """
    try:
        tokens = await list_tokens(username, password, session)
        if not tokens:
            logger.info("No tokens to evaluate for oldest.")
            return False

        oldest_token_info = None
        oldest_date = None

        for token_info in tokens:
            token_str = token_info.get('access_token')
            exp_date_str = token_info.get('expiration_date')

            if not token_str or not exp_date_str:
                continue

            try:
                exp_date = datetime.strptime(exp_date_str, '%m/%d/%Y')
                exp_date = exp_date.replace(tzinfo=timezone.utc)

                if oldest_date is None or exp_date < oldest_date:
                    oldest_date = exp_date
                    oldest_token_info = token_info
            except ValueError:
                continue

        if not oldest_token_info:
            logger.info("Could not determine the oldest token (maybe date format issues?).")
            return False

        token_to_revoke = oldest_token_info.get('access_token')
        logger.info(f"Found oldest token expiring {oldest_token_info.get('expiration_date')}. Revoking...")

        result = await revoke_token(username, password, token_to_revoke, session)

        if isinstance(result, dict) and result.get("status") == "error":
            logger.error(f"Failed to revoke oldest token: {result.get('message')}")
            return False

        logger.info(f"Successfully revoked oldest token.")
        return True

    except Exception as e:
        logger.exception(f"Error finding/revoking oldest token: {e}")
        return False


async def ensure_valid_earthdata_token():
    """
    Checks token validity
    Updates both .env file and os.environ.
    Returns the valid token string, or None if refresh fails.
    """
    load_dotenv(dotenv_path=ENV_FILE_PATH, override=True)
    current_token = os.getenv(JWT_ENV_VAR)
    expiration_str = os.getenv(JWT_EXP_ENV_VAR)
    now_utc = datetime.now(timezone.utc)
    refresh_needed = False
    reason = ""

    if current_token and expiration_str:
        try:
            exp_date = datetime.strptime(expiration_str, '%m/%d/%Y')
            exp_date = exp_date.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)

            if exp_date > (now_utc + TOKEN_EXPIRATION_BUFFER):
                logger.info(f"Token ({current_token[:10]}...) valid based on stored expiration date ({expiration_str}). No API check needed.")
                if os.environ.get(JWT_ENV_VAR) != current_token or os.environ.get(JWT_EXP_ENV_VAR) != expiration_str:
                    os.environ[JWT_ENV_VAR] = current_token
                    os.environ[JWT_EXP_ENV_VAR] = expiration_str
                    logger.debug("Updated os.environ with token/exp from validated .env")
                return current_token
            else:
                refresh_needed = True
                reason = f"Stored expiration date {expiration_str} is within the {TOKEN_EXPIRATION_BUFFER.days}-day buffer."

        except ValueError:
            refresh_needed = True
            reason = f"Could not parse stored expiration date '{expiration_str}' from .env."
    else:
        refresh_needed = True
        reason = "Token or expiration date missing from environment/.env file."

    logger.info(f"{reason} Performing API check/refresh.")

    try:
        username, password = await load_credentials()
        async with aiohttp.ClientSession() as session:

            if refresh_needed:
                is_valid_on_api, token_from_env, exp_from_api = await is_token_valid_via_api(username, password, session)
                current_token = token_from_env

                if is_valid_on_api and exp_from_api and "Could not parse" not in reason and "missing from environment" not in reason:
                    try:
                        api_exp_date = datetime.strptime(exp_from_api, '%m/%d/%Y')
                        api_exp_date = api_exp_date.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                        if api_exp_date > (now_utc + TOKEN_EXPIRATION_BUFFER):
                             logger.info(f"API confirmed token ({current_token[:10]}...) is valid until {exp_from_api}. Refresh not required yet.")
                             await _update_env_and_os(current_token, exp_from_api)
                             return current_token
                        else:
                             logger.info(f"API confirms token expires soon ({exp_from_api}). Proceeding with refresh.")
                    except ValueError:
                         logger.warning("Could not parse expiration date from API response during validity check.")
                elif not is_valid_on_api:
                    logger.info(f"API check confirms token ({current_token[:10]}... if present) is invalid or expired. Proceeding with refresh.")

            logger.info(f"Attempting token generation.")
            new_token_info = None
            try:
                new_token_info = await generate_new_token(username, password, session)

            except aiohttp.ClientResponseError as e:
                if e.status == 403 and "max_token_limit" in e.message.lower():
                    logger.info("Max token limit reached. Attempting to revoke oldest token...")
                    revoked = await revoke_oldest_token(username, password, session)
                    if revoked:
                        logger.info("Oldest token revoked. Retrying token generation...")
                        try:
                            new_token_info = await generate_new_token(username, password, session)
                        except Exception as retry_e:
                            logger.error(f"Failed to generate token after revocation: {retry_e}")
                            return current_token
                    else:
                        logger.error("Failed to revoke oldest token. Cannot generate new one.")
                        return current_token
                else:
                    logger.error(f"HTTP error during token generation: {e}")
                    return current_token
            except Exception as e:
                 logger.exception(f"Unexpected error during token generation: {e}")
                 return current_token

            if new_token_info:
                new_token = new_token_info.get("access_token")
                new_expiration = new_token_info.get("expiration_date")

                if new_token and new_expiration:
                     logger.info(f"Successfully generated new token ({new_token[:10]}...) expiring {new_expiration}.")
                     await _update_env_and_os(new_token, new_expiration)
                     return new_token
                else:
                     logger.error("Token generation response missing 'access_token' or 'expiration_date'.")
                     return current_token
            else:
                logger.error("Failed to obtain new token info after generation attempts.")
                return current_token

    except ValueError as e:
         logger.error(f"{e}")
         return None
    except Exception as e:
        logger.exception(f"Unexpected error in ensure_valid_earthdata_token: {e}")
        load_dotenv(dotenv_path=ENV_FILE_PATH, override=True)
        return os.getenv(JWT_ENV_VAR)
