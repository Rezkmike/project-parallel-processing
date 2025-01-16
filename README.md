# Parallel Processing Project

## Project Overview
This project sets up a distributed computing environment using Apache Airflow, Apache Spark, and NFS for parallel data processing.

## Architecture
- **Airflow**: Workflow orchestration
- **Spark**: Distributed data processing
- **NFS**: Shared file storage
- **PostgreSQL**: Metadata and task tracking
- **Redis**: Celery broker for task queuing

## Prerequisites
- Docker
- Docker Compose
- Minimum RAM: 8GB
- Minimum CPU: 4 cores

## Project Structure
```
project-parallel-processing/
│
├── dags/                # Airflow DAG definitions
│   └── dag-spark-submit.py
│
├── spark-apps/          # Spark application scripts
│   └── spark-app.py
│
├── nfs/                 # NFS shared directory
│
├── docker-compose.yaml  # Docker Compose configuration
└── airflow.Dockerfile   # Custom Airflow image build
```

## Setup Instructions
1. Clone the repository
2. Ensure Docker and Docker Compose are installed
3. Run the following commands:
```bash
docker-compose build
docker-compose up -d
```

## Accessing Services
- Airflow Webserver: http://localhost:8080
- Spark Master UI: http://localhost:8081

## Troubleshooting
- Check container logs: `docker-compose logs <service_name>`
- Restart services: `docker-compose restart`

## Contributing
Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.

## License
This project is licensed under the MIT License - see the LICENSE.md file for details.