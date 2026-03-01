# CRISP-DM Documentation

## Business Understanding
Hospitals require real-time monitoring systems to detect abnormal vitals and reduce patient risk.

## Data Understanding
Vitals data includes:
- Heart rate
- Oxygen saturation (SpO2)
- Blood pressure
- Temperature
- Timestamp

## Data Preparation
JSON schema enforcement and timestamp parsing.

## Modeling
Rule-based risk scoring:
- HIGH: SpO2 < 90 or HR > 130 or Temp >= 39°C
- MEDIUM: Moderate abnormal ranges
- LOW: Normal vitals

## Evaluation
Analyze distribution of risk levels.

## Deployment
Local Spark streaming prototype.
