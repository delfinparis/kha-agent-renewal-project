"""
KHA Agent Renewal — Configuration
"""
from datetime import date

# Monday.com board
MONDAY_BOARD_ID = 359616654
MONDAY_TERMINATED_GROUP_ID = "new_group32247"

# DFPR (data.illinois.gov)
DFPR_DATASET_ID = "pzzh-kp68"
DFPR_BUSINESS_DBA = "Kale Realty"

# License renewal cutoff: agents with expiration AFTER this date have renewed
RENEWAL_CUTOFF = date(2026, 4, 30)

# Email
EMAIL_RECIPIENTS = ["dj@kalerealty.com", "rea@kalerealty.com"]
ENTITY_NAME = "KHA"
