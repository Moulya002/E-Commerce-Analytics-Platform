# Dashboard Screenshots

Place screenshots here after running the platform locally.

## How to Capture

1. Start the platform: `./scripts/setup.sh`
2. Seed metrics (optional): `python scripts/seed_metrics.py`
3. Open http://localhost:8501
4. Capture screenshots:

| File | Description |
|------|-------------|
| `dashboard_kpis.png` | KPI cards row |
| `dashboard_revenue.png` | Revenue & orders chart |
| `dashboard_products.png` | Top products bar chart |
| `dashboard_geo.png` | Geographic pie/bar charts |
| `spark_ui.png` | Spark Master UI with running jobs |
| `airflow_dags.png` | Airflow DAG graph view |
| `kafka_topics.png` | Kafka topic list |

## Example Commands

```bash
# macOS screenshot
screencapture -x docs/screenshots/dashboard_kpis.png
```

Add screenshots to your resume/portfolio README after capture.
