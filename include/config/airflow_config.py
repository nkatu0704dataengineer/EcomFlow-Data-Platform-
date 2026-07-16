"""
Shared Airflow configuration for EcomFlow DAGs.
"""
from datetime import timedelta
PROJECT_NAME = "EcomFlow"
#owner & timezone
OWNER = "ecomflow"
TIMEZONE = "Asia/Ho_Chi_Minh"

#retry policy
RETRIES = 3
RETRY_DELAY_MINUTES = 5

#email notification
EMAIL = ["Nkatu0704@gmail.com"]
EMAIL_ON_FAILURE = True
EMAIL_ON_RETRY = False

def get_default_args():
    return {
        "owner": OWNER,
        "retries": RETRIES,
        "retry_delay": timedelta(minutes=RETRY_DELAY_MINUTES),
        "email": EMAIL,
        "email_on_failure": EMAIL_ON_FAILURE,
        "email_on_retry": EMAIL_ON_RETRY
    }