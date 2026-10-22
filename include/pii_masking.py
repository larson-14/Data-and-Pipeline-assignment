"""Mask direct customer attributes in public quarantine evidence."""


def mask_customer_evidence(frame):
    masked=frame.copy()
    for column in ('full_name','email','phone','city','area','pincode','signup_date','loyalty_tier','is_active'):
        if column in masked:
            masked[column]='[REDACTED]'
    return masked
