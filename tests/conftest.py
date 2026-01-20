import os
import pytest

# Sets OS vars for entire set of tests
TEST_ENV_VARS = {
    "ENVIRONMENT": "test_environment",
    "AWS_REGION": "test_aws_region",
    "REDSHIFT_DB_NAME": "test_redshift_db",
    "REDSHIFT_DB_HOST": "test_redshift_host",
    "REDSHIFT_DB_USER": "test_redshift_user",
    "REDSHIFT_DB_PASSWORD": "test_redshift_password",
}


@pytest.fixture(scope="session", autouse=True)
def tests_setup_and_teardown():
    # Will be executed before the first test
    os.environ.update(TEST_ENV_VARS)

    yield

    # Will execute after the final test
    for os_config in TEST_ENV_VARS.keys():
        if os_config in os.environ:
            del os.environ[os_config]
