#!/usr/bin/env python3
"""
Standalone script to authenticate and check vessel_rupture data in valid datasets.

This script:
1. Authenticates via password
2. Queries all datasets with status 'valid'
3. For each dataset, queries the 'vessel_rupture' variable
4. Checks if the data contains non-NaN values
5. Reports results
"""

import json
import requests
import sys
import getpass
import numpy as np
import urllib3
import logging
import argparse

from pathlib import Path
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin

# Add this to suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("assas_data_hub_crawler")

class AssasAPIClient:
    """Client for interacting with ASSAS Data Hub API."""
    
    def __init__(self, base_url: str, verify_ssl: bool = True):
        """
        Initialize API client.
        
        Args:
            base_url: Base URL of the API (e.g., 'http://localhost:5000')
            verify_ssl: Whether to verify SSL certificates
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.verify = verify_ssl
        self.authenticated = False
        self.auth_token = None
        
    def authenticate(self, username: str, password: str) -> bool:
        """
        Authenticate with the API using username/password.
        
        Args:
            username: Username for authentication
            password: Password for authentication
            
        Returns:
            bool: True if authentication successful
        """
        auth_endpoints = [
            '/test/auth/basic/login',
        ]
        
        auth_data = {
            "username": username,
            "password": password
        }
        
        for endpoint in auth_endpoints:
            try:
                auth_url = urljoin(self.base_url, endpoint)
                logger.info(f"Attempting authentication at: {auth_url}")

                # First try to submit the form
                response = self.session.post(
                    auth_url, data=auth_data, allow_redirects=True
                )
                
                logger.info(f"Response status: {response.status_code}")
                logger.info(f"Response headers: {dict(response.headers)}")
                
                # Check for successful authentication indicators
                success_indicators = self._check_authentication_success(response)
                
                if success_indicators['is_authenticated']:
                    logger.info("Authentication successful!")
                    for indicator in success_indicators['indicators']:
                        logger.info(f"  - {indicator}")
                    self.authenticated = True
                    return True
                else:
                    logger.error("Authentication failed")
                    for reason in success_indicators['failure_reasons']:
                        logger.error(f"  - {reason}")

                    # Show response preview for debugging
                    if response.text:
                        logger.debug(f"  Response preview: {response.text[:300]}...")

                    return False
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                continue

        logger.error("All authentication endpoints failed")
        return False

    def _check_authentication_success(self, response) -> Dict[str, Any]:
        """
        Check if authentication was successful based on multiple indicators.
        
        Args:
            response: HTTP response object
            
        Returns:
            Dict with authentication status and indicators
        """
        indicators = []
        failure_reasons = []
        is_authenticated = False
        
        # 1. Check for session cookies
        session_cookies = \
            [cookie for cookie in self.session.cookies \
                if any(name in cookie.name.lower() \
                       for name in ['session', 'auth', 'login'])]
        
        if session_cookies:
            indicators.append(f"Session cookie found: {session_cookies[0].name}")
            is_authenticated = True
        else:
            failure_reasons.append("No session cookie found")
        
        # 2. Check HTTP status codes
        if response.status_code == 200:
            indicators.append("HTTP 200 OK response")
        elif response.status_code in [302, 303, 301]:
            location = response.headers.get('location', '')
            if location and '/login' not in location.lower():
                indicators.append(f"Redirected to: {location}")
                is_authenticated = True
            else:
                failure_reasons.append(f"Redirected back to login: {location}")
        else:
            failure_reasons.append(f"HTTP {response.status_code}")
        
        # 3. Check response content
        content_type = response.headers.get('content-type', '')
        
        if content_type.startswith('application/json'):
            try:
                data = response.json()
                if data.get('success') or data.get('status') == 'success':
                    indicators.append("JSON success response")
                    is_authenticated = True
                elif 'error' in data or 'message' in data:
                    failure_reasons.append(
                        f"JSON error: \
                            {data.get('error', data.get('message', 'Unknown'))}"
                    )
            except json.JSONDecodeError:
                failure_reasons.append("Invalid JSON response")
        
        elif content_type.startswith('text/html'):
            response_text = response.text.lower()
            
            # Check for error messages (these indicate failure)
            error_phrases = [
                'invalid username or password',
                'authentication failed',
                'login failed',
                'incorrect credentials',
                'please provide both username and password',
                'error',
                'failed'
            ]
            
            found_errors = \
                [phrase for phrase in error_phrases if phrase in response_text]
            
            if found_errors:
                failure_reasons.extend(
                    [f"Error message found: '{error}'" for error in found_errors]
                )
                is_authenticated = False
            else:
                # Check for success indicators in HTML
                success_phrases = [
                    'welcome',
                    'dashboard',
                    'home',
                    'logout',
                    'successfully',
                    'assas data hub',  # Likely the main page
                    'datasets',       # Likely the dashboard
                ]
                
                found_success = \
                    [phrase for phrase in success_phrases if phrase in response_text]
                
                if found_success:
                    indicators.extend(
                        [f"Success indicator: '{phrase}'" for phrase in found_success]
                    )
                    # If we have both session cookie AND success indicators,
                    # we're authenticated
                    if session_cookies:
                        is_authenticated = True
                else:
                    failure_reasons.append("No success indicators found in HTML")
        
        # 4. Check URL path (if we ended up on a different page)
        final_url = response.url.lower()
        if '/login' in final_url:
            failure_reasons.append("Still on login page")
        elif final_url != urljoin(self.base_url, '/test/auth/basic/login').lower():
            indicators.append(f"Navigated to different page: {response.url}")
            if session_cookies:
                is_authenticated = True
        
        # 5. Final decision logic
        # If we have session cookies and no explicit error messages,
        # consider it successful
        if session_cookies and not any(
            'error' in reason.lower() for reason in failure_reasons
        ):
            is_authenticated = True
        
        return {
            'is_authenticated': is_authenticated,
            'indicators': indicators,
            'failure_reasons': failure_reasons,
            'has_session_cookie': bool(session_cookies),
            'response_type': content_type
        }

    def get_datasets(
        self,
        status: str = None,
        limit: int = 1000
    ) -> Optional[List[Dict]]:
        """
        Get datasets from the API.
        
        Args:
            status: Filter by status (e.g., 'valid')
            limit: Maximum number of datasets to retrieve
            
        Returns:
            List of dataset dictionaries or None if error
        """
        try:
            if not self.authenticated:
                logger.error("Not authenticated")
                return None
            
            datasets_url = urljoin(self.base_url, '/test/assas_app/datasets')
            params = {'limit': limit}
            
            if status:
                params['status'] = status

            logger.info(f"Querying datasets: {datasets_url} (status={status})")
            response = self.session.get(datasets_url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    datasets = data.get('data', {}).get('datasets', [])
                    logger.info(f"Retrieved {len(datasets)} datasets")
                    return datasets
                else:
                    logger.error(f"Failed to get datasets: {data.get('message')}")
                    return None
            else:
                logger.error(f"Failed to get datasets: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"✗ Error getting datasets: {e}")
            return None
    
    def get_variable_data(self, dataset_id: str, variable_name: str) -> Optional[Dict]:
        """
        Get variable data from a dataset.
        
        Args:
            dataset_id: UUID of the dataset
            variable_name: Name of the variable to query
            
        Returns:
            Variable data dictionary or None if error
        """
        try:
            if not self.authenticated:
                logger.error("Not authenticated")
                return None

            var_url = urljoin(
                self.base_url, 
                f'/test/assas_app/datasets/{dataset_id}/data/{variable_name}'
            )

            response = self.session.get(var_url, params={'include_stats': 'true'})
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    return data.get('data')
                else:
                    return None
            elif response.status_code == 404:
                return None  # Variable not found
            else:
                logger.error(f"Error getting variable data: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting variable data: {e}")
            return None
    
    def check_variable_has_valid_data(self, data: Any) -> bool:
        """
        Check if variable data contains non-NaN values.
        
        Args:
            data: Variable data (can be list, number, etc.)
            
        Returns:
            bool: True if contains valid (non-NaN) data
        """
        try:
            if data is None:
                return False
            
            # Handle different data types
            if isinstance(data, (list, tuple)):
                # Flatten nested lists
                flat_data = self._flatten_list(data)
                if not flat_data:
                    return False
                
                # Check for any non-NaN values
                for value in flat_data:
                    if value is not None:
                        try:
                            float_val = float(value)
                            if not (np.isnan(float_val) or np.isinf(float_val)):
                                return True
                        except (ValueError, TypeError):
                            # Non-numeric data is considered valid
                            return True
                return False
                
            elif isinstance(data, (int, float)):
                return not (np.isnan(float(data)) or np.isinf(float(data)))
                
            elif isinstance(data, str):
                return len(data.strip()) > 0
                
            else:
                return data is not None
                
        except Exception:
            return False
    
    def _flatten_list(self, nested_list: List) -> List:
        """Recursively flatten nested lists."""
        flat = []
        for item in nested_list:
            if isinstance(item, (list, tuple)):
                flat.extend(self._flatten_list(item))
            else:
                flat.append(item)
        return flat


def setup_logging(loglevel: str):
    """Configure logging for the crawler."""
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        print(f"Invalid log level: {loglevel}. Defaulting to INFO.")
        numeric_level = logging.INFO
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler("assas_data_hub_crawler.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )

def main():
    """Main script execution."""
    parser = argparse.ArgumentParser(
        description="ASSAS Data Hub - Data Crawler"
    )
    parser.add_argument(
        "--loglevel",
        default="INFO",
        help="Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    args = parser.parse_args()

    setup_logging(args.loglevel)
    logger = logging.getLogger("assas_data_hub_crawler")
    logger.info("Starting ASSAS Data Hub - Data Crawler")
    logger.info("=" * 50)
    
    # Configuration
    logger.info("Prompting for API base URL")
    base_url = \
        input("Enter API base URL (default: https://assas.scc.kit.edu): ").strip()
    if not base_url:
        base_url = "https://assas.scc.kit.edu"

    # Authentication
    logger.info("Prompting for username")
    username = input("Username: ").strip()
    if not username:
        logger.error("Username is required")
        sys.exit(1)
    
    password = getpass.getpass("Password: ")
    if not password:
        logger.error("Password is required")
        sys.exit(1)

    var_name = input("ASTEC variable name: ").strip()
    if not var_name:
        logger.error("ASTEC variable name is required")
        sys.exit(1)
    
    # Initialize client
    client = AssasAPIClient(base_url, verify_ssl=False)
    
    # Authenticate
    if not client.authenticate(username, password):
        logger.error("Authentication failed")
        sys.exit(1)
    
    logger.info("Authentication successful")
    
    # Get valid datasets
    logger.info("Querying datasets with status 'Valid'...")
    datasets = client.get_datasets(status='Valid')
    
    if not datasets:
        logger.error("No valid datasets found or error occurred")
        sys.exit(1)
    
    logger.info(f"Found {len(datasets)} valid datasets")

    # Check ASTEC variable data for each dataset
    results = {
        'total_datasets': len(datasets),
        f'has_{var_name}': 0,
        'has_valid_data': 0,
        'has_nan_only': 0,
        'variable_not_found': 0,
        'error_accessing': 0,
        'details': []
    }

    logger.info("Checking ASTEC variable in each dataset...")
    logger.info("-" * 60)
    
    for i, dataset in enumerate(datasets, 1):
        dataset_id = dataset.get('uuid')
        dataset_name = dataset.get('name', 'Unknown')
        
        logger.info(f"[{i}/{len(datasets)}] Dataset: {dataset_name} (ID: {dataset_id})")
        
        if not dataset_id:
            logger.warning("No dataset ID found")
            results['error_accessing'] += 1
            continue
        
        var_data = client.get_variable_data(dataset_id, var_name)

        dataset_result = {
            'id': dataset_id,
            'name': dataset_name,
            'status': 'unknown'
        }
        
        if var_data is None:
            logger.warning(f"{var_name} variable not found")
            dataset_result['status'] = 'variable_not_found'
            results['variable_not_found'] += 1
        else:
            results[f'has_{var_name}'] += 1

            # Check the actual data
            data_array = var_data.get('data')
            statistics = var_data.get('statistics', {})
            
            # Use statistics if available for quick check
            if statistics:
                valid_count = statistics.get('valid_value_count', 0)
                total_count = statistics.get('size', 0)
                
                if valid_count > 0:
                    logger.info(f"Has valid data ({valid_count}/{total_count} values)")
                    dataset_result['status'] = 'has_valid_data'
                    dataset_result['valid_count'] = valid_count
                    dataset_result['total_count'] = total_count
                    results['has_valid_data'] += 1
                else:
                    logger.warning(f"All values are NaN/missing ({total_count} total)")
                    dataset_result['status'] = 'all_nan'
                    dataset_result['total_count'] = total_count
                    results['has_nan_only'] += 1
            else:
                # Fallback to manual data check
                has_valid = client.check_variable_has_valid_data(data_array)
                
                if has_valid:
                    logger.info("Has valid data")
                    dataset_result['status'] = 'has_valid_data'
                    results['has_valid_data'] += 1
                else:
                    logger.warning("All values are NaN/missing")
                    dataset_result['status'] = 'all_nan'
                    results['has_nan_only'] += 1
        
        results['details'].append(dataset_result)

    logger.info("=" * 60)
    logger.info("SUMMARY REPORT")
    logger.info("=" * 60)
    logger.info(f"Total datasets checked: {results['total_datasets']}")
    logger.info(f"Datasets with {var_name} variable: {results[f'has_{var_name}']}")
    logger.info(f"Datasets with valid {var_name} data: {results['has_valid_data']}")
    logger.info(f"Datasets with only NaN/missing data: {results['has_nan_only']}")
    logger.info(f"Datasets without {var_name} variable: {results['variable_not_found']}")
    logger.info(f"Datasets with access errors: {results['error_accessing']}")
    
    # Detailed results
    if results['has_valid_data'] > 0:
        logger.info(f"\nDatasets with VALID {var_name} data:")
        for detail in results['details']:
            if detail['status'] == 'has_valid_data':
                valid_info = ""
                if 'valid_count' in detail:
                    valid_info = (
                        f" ({detail['valid_count']}/{detail['total_count']} "
                        f"valid values)"
                    )
                logger.info(f"{detail['name']} (ID: {detail['id']}){valid_info}")

    if results['has_nan_only'] > 0:
        logger.info(f"\nDatasets with ONLY NaN/missing {var_name} data:")
        for detail in results['details']:
            if detail['status'] == 'all_nan':
                logger.info(f"{detail['name']} (ID: {detail['id']})")
    
    # Save detailed results to file
    try:
        filepath = Path(__file__).parent.resolve() / f"{var_name}_check_results.json"
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"\nDetailed results saved to: {filepath}")
    except Exception as e:
        logger.warning(f"\nWarning: Could not save results to file: {e}")
    
    logger.info("\nCheck completed.")

if __name__ == "__main__":
    main()