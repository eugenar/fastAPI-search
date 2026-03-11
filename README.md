# FastAPI for search

This README describes how to install and run Docker so that you can spin up the necessary services (e.g., PostgreSQL, Elasticsearch, and others) for this project.

## 1. Install Docker

Docker is a containerization platform that allows you to run software and services in self-contained environments.

- **Mac & Windows**: You’ll need [Docker Desktop](https://www.docker.com/get-started).  
  - Follow the installation prompts (it will install both Docker Engine and Docker Compose).
  - Once installed, launch Docker Desktop and wait until it indicates that Docker is running.

- **Linux**:  
  1. Refer to the [Docker Engine installation instructions for Linux](https://docs.docker.com/engine/install/).
  2. (Optional) If you want to avoid using `sudo` every time, follow [Post-installation steps](https://docs.docker.com/engine/install/linux-postinstall/) to add your user to the `docker` group.

## 2. Verify Docker Installation

After installing, open a terminal or command prompt and run:
```bash
docker --version
```
You should see something like:
```
Docker version 20.10.X, build ...
```
_(Version numbers may vary.)_

You can also test with:
```bash
docker ps
```
If Docker is running, this command should succeed (you might see no running containers or an empty list).

## 3. Bring Up Services with Docker Compose

In this repository, there should be a file named `docker-compose.yml` which defines the services you need (e.g., PostgreSQL, Elasticsearch, etc.).

1. **Navigate** to the project folder containing `docker-compose.yml`:
   ```bash
   cd bevel-backend-takehome-food
   ```
2. **Start** the containers:
   ```bash
   docker-compose up
   ```
   or, if you have a newer Docker CLI:
   ```bash
   docker compose up
   ```
   - This command reads `docker-compose.yml` and pulls the required images if they’re not already on your machine.
   - It then starts each container in the configuration.

### 3.1 Running in the Background

If you don’t want to watch the logs in your terminal, add the `-d` flag to run in detached mode:
```bash
docker-compose up -d
```
_(The containers will continue running in the background.)_

### 3.2 Shutting Down

Press `Ctrl + C` (in foreground mode) or run:
```bash
docker-compose down
```
to stop and remove the containers.

## 4. Troubleshooting

- **“Cannot connect to the Docker daemon”** or **FileNotFoundError**: Ensure Docker Desktop (Mac/Windows) or the Docker service (Linux) is running.
- **“No such file or directory”**: Make sure you’re running `docker-compose` from the same directory where `docker-compose.yml` is located.
- **Permissions** (Linux): If you need to use `sudo docker-compose ...` each time, consider adding your user to the `docker` group (see [Linux post-install](https://docs.docker.com/engine/install/linux-postinstall/)).

## 5. Further Reading

- [Official Docker Documentation](https://docs.docker.com/)
- [Docker Compose Overview](https://docs.docker.com/compose/)

You should now be ready to run Docker services for this take-home assignment. If you have any questions or run into issues, let us know!

# Backend Setup

Two boilerplate implementations are provided for the backend take-home assessment. Choose the language you're most comfortable with:

## Option 1: TypeScript/Node.js (app-ts)
An Express.js server implementation with TypeScript. See `/app-ts/README.md` for setup instructions.

## Option 2: Python (app-py)
A FastAPI server implementation with Python. See `/app-py/README.md` for setup instructions.

Both implementations provide identical functionality:
- PostgreSQL database connection
- Elasticsearch client integration
- Health check endpoints
- Example search endpoint

---

## TypeScript Setup (app-ts)

## Install global dependencies

```
npm install -g yarn
```

## Install local dependencies

```
yarn install
```

## Run the server

```
yarn dev
```

---

## Python Setup (app-py)

### Create virtual environment

```
cd app-py
python -m venv venv
```

### Activate virtual environment

**On macOS/Linux:**
```
source venv/bin/activate
```

**On Windows:**
```
venv\Scripts\activate
```

### Install dependencies

```
pip install -r requirements.txt
```

### Run the server

```
uvicorn src.main:app --reload --port 3000
```

---

# Run health checks

After starting the server, you should be able to check `localhost:3000/health` to see if the server is running and the db connection is working.

You can also check `localhost:3000/health` to see if the connection to database and Elasticsearch is working, and the required tables, triggers, and indices exist.

Access Swagger UI to test `localhost:3000/docs`

# Troubleshooting

### Cannot find database user
If you see an error similar to
```
{"error":"role \"myuser\" does not exist"} 
``` 
when running the db health check, then it could be because the postgres container is colliding with an existing process that is using the same port. The db port is set to `54328` in the docker-compose.yml file. You can change the port in the file to something else, but make sure to update the port in your application configuration (db.ts for TypeScript or db.py for Python) as well.

When restarting the postgres container for debugging, you may need to force delete the volume as well, which is where the data is stored. You can do this by running:

```
docker compose down --volumes
docker compose up --force-recreate
```
You can also check running volumes with:
```
docker volume ls
```
and terminate any hanging volumes that match the name of the volume in the docker-compose.yml file.

# Mapping nutrient -> nutrient category

Based on https://fdc.nal.usda.gov/Foundation_Foods_Documentation

Energy (Atwater Specific Factors) -> calories (more accurate than Energy (Atwater General Factors))
Adjusted Protein -> protein (indicated by the % Daily Value on labels)
Total fat (NLEA) -> fat
Carbohydrate, by summation -> carbs (more accurate then Carbohydrate, by difference)
