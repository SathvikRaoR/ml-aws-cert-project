# ML AWS Certified Associate Project

This project prepares for the AWS Certified Machine Learning Associate (MLA-C01) exam by building a real-world Personal Finance ML application. It integrates with Zerodha MCP, real-time news feeds, and leverages AWS ML services (SageMaker, Comprehend, Rekognition, etc.) with strong security best practices.

## Stack
- Python, Flask/FastAPI
- AWS S3, SageMaker, Comprehend, Rekognition
- Boto3, Pandas, Jupyter
- Integrated with 3rd-party finance/news APIs

## Repository Structure
- /app - core web application code
- /scripts - automation and data scripts
- /data - sample and dev datasets
- /docs - documentation, logs, diagrams, architecture
- /notebooks - Jupyter notebooks for EDA, experiments
- /logs - local logs (do not commit secrets)
- /tests - (planned) unit/integration tests

## Security & Cost Practices
- All cloud resources use least privilege IAM, encryption
- Cost tracked; project stays in AWS Free Tier