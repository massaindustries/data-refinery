# Human Review Report

## Summary
- Total records: 7
- Records with issues: 2
- Issues found: 2
- Recommendation: **REVIEW_REQUIRED**

## Issues Requiring Attention

### issue_1
- **Type**: low_confidence
- **Severity**: high
- **Field**: `email`
- **Record**: customer / SHA256_RSSMRA89C12H501X
- **Confidence**: 0.92
- **Reason**: Email format appears invalid
- **Evidence**: Page 1
  ```
customer_name: Mario Rossi, fiscal_code: RSSMRA89C12H501X, residence_address: Via Roma 5, 00100 Roma (RM)
```
- **Suggestion**: Verify correct email address with customer
- **Decision Required**: YES

### issue_2
- **Type**: inconsistency
- **Severity**: high
- **Field**: `data`
- **Record**: transaction / SHA256_2024-01-13|1200.00|payment|PLZ-RCA-77821
- **Confidence**: N/A
- **Reason**: Inconsistent transaction dates for the same policy
- **Evidence**: Page 3
  ```
date: 2024-01-13, amount: 1200.00, description: Premio polizza RCA – Gennaio, type: payment
```
- **Suggestion**: Verify transaction dates and resolve inconsistency
- **Decision Required**: YES

## Auto-Fix Suggestions

- **issue_3**: `telefono`
  - Original: `+39 333 1234567`
  - Suggested: `+393331234567`
  - Confidence: 0.85
